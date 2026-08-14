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

WHICH LANES RAN IS PART OF THE TABLE'S IDENTITY. The fourth lane needs
`ref::reference_label_pool`, which has no producer in this tree; if it was absent the
tables ship three lanes, and that is recorded in BUILD.json by name rather than
averaged away. Three lanes is a smaller claim, not a smaller table.
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
out     = model.AddProduct(lib.GetType("ref::gpr_table_denovo"))

CHANNEL_PREFIX = "denovo"

# The same frozen 14-column schema the GEM table uses, so the two lines of evidence line
# up column for column. See benchmark/host_gpr_gem.py.
GPR_COLS = (
    "build_id", "host", "unit_id", "feature_id", "feature_kind", "feature_name",
    "mnxr", "channel", "evidence_id", "evidence_name", "raw_score",
    "projection_via", "in_atom_universe", "gpr_rule",
)

DRIVER = r'''
import json
from pathlib import Path
import numpy as np
import pandas as pd

GENOMES = Path("{genomes}")
OUT     = Path("{out}")
GPR_COLS = {gpr_cols}
PREFIX = "{prefix}"

# staged mapper table -> the host it describes. The mapper names its `source` after the
# ORF file's stem, which for a host proteome is that host's sequence accession -- which
# is exactly how the acquisition names the file. That is the join, and it is why the
# mapper's `source` column was made to name the ORF SET rather than the assay.
ACCESSION_FOR_HOST = {{}}
for d in sorted(p for p in GENOMES.glob("*") if p.is_dir()):
    faa = sorted((d / "genome").glob("*.faa"))
    if len(faa) != 1:
        raise SystemExit(f"[denovo_gpr] expected one proteome under {{d}}/genome, "
                         f"found {{[p.name for p in faa]}}")
    ACCESSION_FOR_HOST[d.name] = faa[0].stem
HOST_FOR_ACCESSION = {{v: k for k, v in ACCESSION_FOR_HOST.items()}}
if len(HOST_FOR_ACCESSION) != len(ACCESSION_FOR_HOST):
    raise SystemExit(f"[denovo_gpr] two hosts share a proteome accession: "
                     f"{{ACCESSION_FOR_HOST}} -- the source column cannot name the host")

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
    host = HOST_FOR_ACCESSION.get(srcs[0])
    if host is None:
        raise SystemExit(
            f"[denovo_gpr] mapper table {{path.name}} names ORF set {{srcs[0]!r}}, which "
            f"is none of the host proteomes {{sorted(HOST_FOR_ACCESSION)}}. The host set "
            f"is declared in acquire/genomes.py; a table from some other ORF set has no "
            f"host to attribute it to.")
    if host in seen:
        raise SystemExit(f"[denovo_gpr] two mapper tables both name {{host}}")
    seen.add(host)

    lanes = sorted(g["channel"].unique())
    print(f"[denovo_gpr] {{host}}: {{len(g):,}} mapper rows, {{g['orf'].nunique():,}} ORFs, "
          f"{{g['mnxr'].nunique():,}} MNXR, lanes {{lanes}}", flush=True)

    df = pd.DataFrame({{
        "build_id": "denovo_" + host,
        "host": host,
        # The unit is the proteome, not a model: this table's claim is "this host's own
        # annotation lanes infer these reactions", and naming a GEM here would imply a
        # curated model was consulted, which is the whole thing the de-novo line is not.
        "unit_id": "proteome",
        "feature_id": g["orf"],
        "feature_kind": "orf",
        "feature_name": g["intermediate_name"],
        "mnxr": g["mnxr"],
        # The lane stays in the channel, prefixed, so a row's provenance survives the
        # merge with the GEM table (whose single channel is `gem_gpr`).
        "channel": PREFIX + "_" + g["channel"].astype(str),
        "evidence_id": g["intermediate_id"],
        "evidence_name": g["intermediate_name"],
        # Carried through from the lane, unlike the GEM table's uniform 1.0: here the
        # score IS evidence strength, and it is what the condition GPR's belief split
        # reads.
        "raw_score": g["raw_score"].astype(np.float32),
        "projection_via": g["projection_via"],
        # Not computable here without the bake, and NOT defaulted to True: a row wrongly
        # marked in-universe claims an edge can exist for a reaction that has no atom
        # pairs. Null means "not asserted", which a consumer can see.
        "in_atom_universe": pd.Series([None] * len(g), dtype="object"),
        # There is no boolean rule: a de-novo call is per ORF, and inventing "orf" as a
        # one-gene rule would make the two tables look like the same kind of claim.
        "gpr_rule": None,
    }})[list(GPR_COLS)]

    df = df.sort_values(["feature_kind", "feature_id", "mnxr", "channel"],
                        kind="mergesort", na_position="last").reset_index(drop=True)
    d = OUT / "hosts" / host
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "gpr_denovo.parquet", index=False, compression="zstd")
    print(f"[denovo_gpr] wrote {{len(df):,}} rows for {{host}}", flush=True)
    summary.append(dict(host=host, orf_set=srcs[0], lanes=lanes, n_lanes=len(lanes),
                        rows=len(df), orfs=int(df["feature_id"].nunique()),
                        mnxr=int(df["mnxr"].nunique())))

missing = sorted(set(ACCESSION_FOR_HOST) - seen)
if missing:
    raise SystemExit(f"[denovo_gpr] no mapper table for {{missing}} -- the de-novo half "
                     f"of the host benchmark is not comparable across a missing host")

# WHICH LANES RAN, by name, per host. The lane set is part of each table's build
# identity; a three-lane table and a four-lane one are different claims about the same
# proteome, and a consumer that cannot tell them apart will average them.
(OUT / "BUILD.json").write_text(json.dumps(
    dict(channel_prefix=PREFIX, hosts=summary), indent=2))
lane_sets = {{tuple(s["lanes"]) for s in summary}}
if len(lane_sets) != 1:
    print(f"[denovo_gpr] WARNING: hosts do not share a lane set: "
          f"{{{{s['host']: s['lanes'] for s in summary}}}} -- cross-host comparison is "
          f"not like-for-like", flush=True)
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
        gpr_paths=repr(gpr_paths), prefix=CHANNEL_PREFIX,
        gpr_cols=repr(GPR_COLS), out=iout.container,
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
