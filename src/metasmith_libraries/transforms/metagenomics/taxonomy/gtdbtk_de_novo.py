# A decorated tree, which classify_wf does not produce and iPHoP cannot do without.
#
# `iphop add_to_db --gtdb_dir` reads exactly two things, both by glob with the
# marker count wildcarded: `gtdbtk.{ar,bac}[0-9]*.decorated.tree` and the matching
# `.tree-taxonomy`. Those are de_novo_wf outputs. `gtdbtk.py` runs classify_wf,
# whose summary TSV is a different artifact entirely -- which is why the type
# `taxonomy::gtdbtk_raw` has sat in taxonomy.yml with no producer.
#
# It is a separate transform rather than a second product on gtdbtk.py because it
# is a different invocation of the tool, not a second output of the same one, and
# because gtdbtk.py is reached by three targets in every metagenomics template.
#
# Two runs, not one. de_novo_wf takes --bacteria XOR --archaea and one outgroup
# each, and iPHoP wants both trees under one directory, so this writes both into
# the same out_dir. An empty domain is survivable and expected: a survey with no
# archaeal MAG produces no ar53 tree, and iPHoP's glob finds the bacterial one and
# carries on -- its only hard stop is neither tree existing.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
ref   = model.AddRequirement(lib.GetType("ref::gtdb"))
asm   = model.AddRequirement(lib.GetType("sequences::putative_genome"))
raw   = model.AddProduct(lib.GetType("taxonomy::gtdbtk_raw"))

# These are r232 names and the paper's are not. GTDB renamed its phyla to the
# -ota suffix, so the published `p__Cyanobacteria` and `p__Altarchaeota` do not
# exist in the release staged here and de_novo_wf exits on an unknown outgroup.
# Checked against taxonomy/gtdb_taxonomy.tsv in the staged tree, which carries
# `p__Cyanobacteriota` and `p__Altiarchaeota`. Re-check on a release change.
OUTGROUPS = {
    "bacteria": "p__Cyanobacteriota",
    "archaea": "p__Altiarchaeota",
}


def protocol(context: ExecutionContext):
    iref = context.Input(ref)
    out_raw = Path("gtdbtk_de_novo")

    genome_dir = Path("./genomes")
    genome_dir.mkdir(exist_ok=True)
    n = 0
    for item in context.AsBatch():
        src = item.Input(asm).local
        (genome_dir/src.name).write_bytes(src.read_bytes())
        n += 1
    if n == 0:
        return ExecutionResult(manifest=[], success=False)

    threads = context.params.get("cpus", 8)
    ext = "fna"

    # `|| true` per domain, deliberately. de_novo_wf exits non-zero when the input
    # set holds none of the domain it was asked for, and that is a fact about the
    # sample rather than a failure of the step. Success is decided below, on
    # whether a tree exists, not on either exit code.
    domain_cmds = "\n".join(
        f"""
            gtdbtk de_novo_wf --genome_dir {genome_dir} --{domain} \\
                --outgroup_taxon {outgroup} \\
                --extension {ext} --cpus {threads} \\
                --force --out_dir {out_raw} || true
        """
        for domain, outgroup in OUTGROUPS.items()
    )
    _cmd = f"""
        export GTDBTK_DATA_PATH=/ref
        mkdir -p {out_raw}
        {domain_cmds}
    """
    context.ExecWithEnv().ifContainerDo(
        binds=[(iref.external, "/ref")],
        env=image,
        cmd=_cmd,
    ).ifVirtualEnvDo(env=image, cmd=_cmd)

    trees = sorted(out_raw.glob("gtdbtk.*.decorated.tree")) + \
            sorted(out_raw.glob("infer/gtdbtk.*.decorated.tree"))
    if not trees:
        Log.Error(f"de_novo_wf produced no decorated tree under [{out_raw}]")
        return ExecutionResult(manifest=[], success=False)
    Log.Info(f"decorated trees: {[t.name for t in trees]}")

    o = context.Output(raw)
    out_raw.rename(o.local)

    return ExecutionResult(
        manifest=[{raw: o.local}],
        success=o.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    batch_size=10_000,
    resources=Resources(
        cpus=32,
        memory=Size.GB(240),
        duration=Duration(hours=48),
    ),
)
