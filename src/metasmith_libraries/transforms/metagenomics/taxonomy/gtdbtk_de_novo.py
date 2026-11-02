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
#
# Budget for that honestly, because the two runs are not two halves of the work.
# Observed on fir against the r232 reference with three genomes: `align` processes
# BOTH domains regardless of which one was asked for, so the second invocation
# repeats it, and only then does the domain flag select a tree to infer. The
# bacterial inference is the wall: FastTree over 189,805 taxa, which is the whole
# reference set plus the user's genomes. Identify and align together took three
# minutes. On fir, both domains together took 16 h 43 m over 30 CAMI marine bins and
# 16 h 03 m and 16 h 55 m over 100, so the wall barely moves with the bin count, and
# peaked at 173.5 GB of 240 GB on about 8 of the 32 cores.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
ref   = model.AddRequirement(lib.GetType("ref::gtdb"))
# The survey's quality MAGs, not any putative genome: add_to_db joins the tree's leaves to
# the bins it adds by file name, so a tree over another bin set adds nothing, and as a
# putative genome the planner fed it das_tool's bins, which no other target wanted.
#
# One tree per study, gathered by group_by rather than folded out of a batch. The cache
# keys each batch member on its own, so a batch folded into one tree records the first
# member only, and the next run re-ran de_novo_wf on the other 99 bins beside the cached
# tree over all 100.
study = model.AddRequirement(lib.GetType("viromics::contig_study"))
asm   = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={study})
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
    for handle in context.InputGroup(asm):
        src = handle.local
        (genome_dir/src.name).write_bytes(src.read_bytes())
        n += 1
    if n == 0:
        return ExecutionResult(manifest=[], success=False)

    threads = context.params.get("cpus", 8)
    ext = "fna"

    # A non-zero exit per domain is tolerated, deliberately, except an OOM kill.
    # de_novo_wf exits non-zero when the input set holds none of the domain it was
    # asked for, and that is a fact about the sample rather than a failure of the
    # step. Success is decided below, on whether a tree exists.
    domain_cmds = "\n".join(
        f"""
            gtdbtk de_novo_wf --genome_dir {genome_dir} --{domain} \\
                --outgroup_taxon {outgroup} \\
                --extension {ext} --cpus {threads} \\
                --force --out_dir {out_raw} \\
                || {{ c=$?; case $c in 125|137) exit $c ;; esac; }}
        """
        for domain, outgroup in OUTGROUPS.items()
    )
    _cmd = f"""
        mkdir -p {out_raw}
        {domain_cmds}
    """
    context.ExecWithEnv(
        env=image,
        exports={"GTDBTK_DATA_PATH": iref.container},
        cmd=_cmd,
    )

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
    group_by=study,
    resources=Resources(
        cpus=32,
        memory=Size.GB(240),
        duration=Duration(hours=48),
    ),
)
