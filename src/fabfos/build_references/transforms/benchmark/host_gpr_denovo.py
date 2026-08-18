"""B2 -- the GPR each host's own annotation lanes infer.

Thin by design. The tables themselves are produced upstream by the SHIPPED 4-lane
mapper running on each host's proteome, exactly as it runs on a fosmid ORF set -- this
step attaches host attribution and re-emits them under the reference type.

That the benchmark's de-novo evidence comes out of the shipped pipeline rather than
from a frozen intermediate is the point of wiring it this way: it makes the benchmark a
test of the method rather than of a file someone once produced.

ONE JOB OVER THE HOST SET, matching B1. The mapper fans out over the proteomes -- one
`annotation::gpr_table` per host -- and this collects them, which is why it groups on
the genomes folder and walks the mapper's outputs as a batch. The alternative, one job
per mapper output, cannot name its host: the mapper is host-agnostic by design and the
attribution has to come from the proteome the table was built from.

THE LANE SET IS THE TABLE'S CONTRACT, not a property of the run. A table here carries
exactly the channels `lib::fabfos_evidence.LANE_SETS["chosen_4"]` declares, and this step
refuses by name when the mapper's output does not. A lane short of that set is not a
smaller claim about the proteome: `nomination_contributions` divides each ORF's belief by
its own distinct-channel count, so a missing lane silently rescales every weight the table
feeds. There is no degraded mode here.
"""
from metasmith.python_api import *
import os
from pathlib import Path

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
# The host set and, through it, each host's proteome. Groups here so the product is
# keyed by host the same way B1's is.
genomes = model.AddRequirement(lib.GetType("fabfos_data::genomes"))
# The chosen-4 lane set IS `annotation::gpr_table` -- `gpr_4lane` produces the type
# directly rather than a subtype, so there is exactly one producer and no tiebreak to
# express here. Pinned to `genomes` so the tables collected are the ones built FROM
# this host set; without the pin the planner may satisfy it from any ORF set it can
# reach, and the host attribution below would be attached to the wrong table.
gpr     = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={genomes})
# The declared lane set is read from the library that declares it, never restated here.
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
out     = model.AddProduct(lib.GetType("ref::gpr_table_denovo"))

LANE_SET = "chosen_4"
# The host de-novo layer's blocks. `cohort` is a study's, not a host's.
EXTENSIONS = ("attribution", "feature", "universe")

# The schema and the same blocks the GEM table carries, so the two lines of evidence
# line up column for column. See benchmark/host_gpr_gem.py.

DRIVER = r'''
import json, os, sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname("{ev_lib}"))
import fabfos_evidence as fe

GENOMES = Path("{genomes}")
OUT     = Path("{out}")
LANE_SET = "{lane_set}"
EXTENSIONS = tuple({extensions})

# staged mapper table -> the host it describes, BY THE ORF IDS IN IT.
#
# The file name cannot carry this. `host_proteomes` copies each proteome to the path the
# engine assigns -- `{{batch}}-{{i}}-{{branch}}.{{hash}}-{{key}}.faa`, which the generated
# nextflow process collects by glob -- so the accession does not survive into the staged
# stem, and the mapper's `source` column is that stem. Attributing on it read the
# accession for exactly as long as nobody ran this step.
#
# The ids do carry it. Every record in a host's proteome is unique to that assembly, so
# the ORFs a table describes name their proteome whatever the file is called. That is the
# same rule check_epi300_identity.py settled on for a different join: go by content.
ACCESSION_FOR_HOST = {{}}
HOST_FOR_ORF = {{}}
for d in sorted(p for p in GENOMES.glob("*") if p.is_dir()):
    faa = sorted((d / "genome").glob("*.faa"))
    if len(faa) != 1:
        raise SystemExit(f"[denovo_gpr] expected one proteome under {{d}}/genome, "
                         f"found {{[p.name for p in faa]}}")
    ACCESSION_FOR_HOST[d.name] = faa[0].stem
    for line in faa[0].open():
        if line.startswith(">"):
            orf = line[1:].split()[0]
            # A record id shared by two hosts would make the attribution ambiguous, and
            # silence about it would attach one host's table to another. RefSeq protein
            # ids are per-assembly here because the id carries the contig accession.
            if HOST_FOR_ORF.setdefault(orf, d.name) != d.name:
                raise SystemExit(f"[denovo_gpr] ORF id {{orf}} appears in two host "
                                 f"proteomes -- attribution by id is not possible")

tables = [Path(p) for p in {gpr_paths}]
print(f"[denovo_gpr] {{len(tables)}} mapper table(s) for {{len(ACCESSION_FOR_HOST)}} hosts",
      flush=True)

summary = []
seen = set()
for path in tables:
    g = pd.read_parquet(path)
    srcs = sorted(g["source"].unique())
    if len(srcs) != 1:
        raise SystemExit(f"[denovo_gpr] {{path.name}} carries {{len(srcs)}} ORF sets "
                         f"{{srcs}} -- one table must describe one proteome")
    attributed = {{HOST_FOR_ORF.get(o) for o in g["orf"].unique()}}
    known = sorted(h for h in attributed if h)
    if len(known) != 1 or None in attributed:
        raise SystemExit(
            f"[denovo_gpr] mapper table {{path.name}} (source {{srcs[0]!r}}) has ORFs "
            f"from {{known or 'no'}} host proteome(s)"
            + (", and some belong to none of them" if None in attributed else "")
            + f". The host set is declared in acquire/genomes.py; a table whose ORFs "
              f"are not one host's has no host to attribute it to.")
    host = known[0]
    if host in seen:
        raise SystemExit(f"[denovo_gpr] two mapper tables both name {{host}}")
    seen.add(host)

    lanes = sorted(g["channel"].unique())
    print(f"[denovo_gpr] {{host}}: {{len(g):,}} mapper rows, {{g['orf'].nunique():,}} ORFs, "
          f"{{g['mnxr'].nunique():,}} MNXR, lanes {{lanes}}", flush=True)

    expected = sorted(fe.LANE_SETS[LANE_SET])
    if lanes != expected:
        raise SystemExit(
            f"[denovo_gpr] {{host}}: lane set is {{lanes}}, not {{expected}}. Missing "
            f"{{sorted(set(expected) - set(lanes))}}; unexpected "
            f"{{sorted(set(lanes) - set(expected))}}. A lane that contributed no rows is a "
            f"broken join or an unstaged reference, and a table short of the declared set "
            f"rescales every belief weight downstream.")

    # The mapper's own columns ARE the core -- channel keeps the frozen spelling, and
    # `lane_set` is what says these rows are de-novo evidence rather than a curated
    # assertion. This step adds attribution and nothing else.
    df = g.copy()
    df["build_id"] = "denovo_" + host
    df["host"] = host
    # The unit is the proteome, not a model: this table's claim is "this host's own
    # annotation lanes infer these reactions", and naming a GEM here would imply a
    # curated model was consulted, which is the whole thing the de-novo line is not.
    df["unit_id"] = df["source"]
    df["feature_kind"] = "orf"
    df["feature_name"] = df["intermediate_name"]
    # There is no boolean rule: a de-novo call is per ORF, and inventing "orf" as a
    # one-gene rule would make the two tables look like the same kind of claim.
    df["gpr_rule"] = None
    # Not computable here without the bake, and NOT defaulted to True: a row wrongly
    # marked in-universe claims an edge can exist for a reaction that has no atom
    # pairs. Null means "not asserted", which a consumer can see.
    df["in_atom_universe"] = pd.Series([None] * len(df), dtype="object")
    df = df[fe.schema_for(EXTENSIONS)]
    df = df.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort",
                        na_position="last").reset_index(drop=True)
    fe.validate_gpr(df, LANE_SET, None, df["source"].iat[0], EXTENSIONS)
    d = OUT / "hosts" / host
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "gpr_denovo.parquet", index=False, compression="zstd")
    print(f"[denovo_gpr] wrote {{len(df):,}} rows for {{host}}", flush=True)
    summary.append(dict(host=host, orf_set=srcs[0], lanes=lanes, n_lanes=len(lanes),
                        rows=len(df), orfs=int(df["orf"].nunique()),
                        mnxr=int(df["mnxr"].nunique())))

missing = sorted(set(ACCESSION_FOR_HOST) - seen)
if missing:
    raise SystemExit(f"[denovo_gpr] no mapper table for {{missing}} -- the de-novo half "
                     f"of the host benchmark is not comparable across a missing host")

# The lane set, by name, per host -- a record of something checked rather than merely
# observed. Every host passed the same gate above, so cross-host comparison is
# like-for-like by construction and needs no warning here.
(OUT / "BUILD.json").write_text(json.dumps(
    dict(lane_set=LANE_SET, extensions={extensions}, hosts=summary), indent=2))
print(f"[denovo_gpr] {{len(summary)}} hosts -> {{OUT}}/hosts/<host>/gpr_denovo.parquet",
      flush=True)
'''


def staged_siblings(one_local: Path, one_container: str) -> list[str]:
    """Every table staged for this requirement, not just the one the lineage names.

    `AsBatch()` iterates the task's lineage list, and the runtime hands a collector ONE
    entry however many files it staged: the group's indexes are merged into a single
    union index before the task is built, so the three mapper outputs arrive as three
    files under one lineage. Iterating it yields one path and silently drops the rest --
    which reads as "the mapper only produced one table" when all three are sitting in
    the work directory.

    So enumerate the FILES. Metasmith stages each product as `<hash>-<product_key>.<ext>`
    and the key is the same for every instance of one requirement, so the siblings
    sharing this file's key are exactly the other tables for it. Each is a symlink whose
    target is already the container-visible path, which is what the driver needs.

    Attribution does not depend on any of this: the collector reads each table's own
    `source` column. Lineage only has to deliver the files.
    """
    key = one_local.name.rsplit("-", 1)[-1]
    # Glob THIS TASK's directory, which is cwd. `.local` resolves through the staged
    # symlink to the file in the PRODUCING task's work directory, and that directory
    # holds exactly one table -- its own. Globbing there finds one file and looks like
    # a correct answer, which is how this fix failed the first time.
    found = sorted(Path.cwd().glob(f"*-{key}"))
    if not found:
        found = sorted(one_local.parent.glob(f"*-{key}"))
    if not found:
        return [one_container]
    return [os.readlink(p) if p.is_symlink() else str(p) for p in found]


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    # Every mapper table staged into this task -- see staged_siblings on why the
    # lineage cannot be asked for them.
    igpr = context.Input(gpr)
    gpr_paths = staged_siblings(Path(igpr.local), str(igpr.container))
    driver = DRIVER.format(
        genomes=context.Input(genomes).container,
        gpr_paths=repr(gpr_paths), out=iout.container,
        ev_lib=context.Input(ev_lib).container, lane_set=LANE_SET,
        extensions=repr(list(EXTENSIONS)),
    )
    context.LocalShell("cat > _host_gpr_denovo.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _host_gpr_denovo.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _host_gpr_denovo.py")

    made = sorted((iout.local / "hosts").glob("*/gpr_denovo.parquet")) \
        if (iout.local / "hosts").exists() else []
    Log.Info(f"denovo_gpr: {len(made)} host tables")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=bool(made) and (iout.local / "BUILD.json").exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=genomes,
    # WITHOUT THIS, "walks the mapper's outputs as a batch" above is false. batch_size
    # defaults to 1, so the generated `o.group(genomes, ..., 1)` collates the mapper's
    # fan-out one table at a time and `AsBatch()` yields a single lineage -- one host's
    # table, silently, while the other two sit finished in the results tree. Grouping on
    # genomes decides there is ONE job; it does not decide how much that job receives.
    #
    # Generous rather than exact, the way gtdbtk and checkm set theirs: nextflow's
    # collate() emits a short final group, so a size above the host count costs nothing
    # and a host set that outgrows it fails loudly on the assertion below rather than
    # quietly dropping hosts.
    batch_size=25,
    # NO labels=["local"] -- see host_proteomes.py. This runs on a compute node
    # alongside the lanes it collects.
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(minutes=30)),
)
