# Methods P43's island filter starts with geNomad's own gene table, restricted to vOTU
# representatives >=100 kb -- annotating the whole representative set for a filter that only
# ever touches its longest tail would cost a full geNomad run for nothing. `annotate`, not
# `end-to-end`: the filter needs gene annotations, not virus/plasmid classification.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_genomad = model.AddRequirement(lib.GetType("env::genomad.env"))
img_seqkit  = model.AddRequirement(lib.GetType("env::seqkit.env"))
ref         = model.AddRequirement(lib.GetType("ref::genomad"))
reps        = model.AddRequirement(lib.GetType("e3::votu_representatives"))

out = model.AddProduct(lib.GetType("e3::genomad_island_genes"))

ISLAND_LENGTH_BP = 100_000


def protocol(context: ExecutionContext):
    ireps = context.Input(reps)
    idb = context.Input(ref)
    o = context.Output(out)
    threads = context.params.get("cpus", 16)

    context.ExecWithEnv(env=img_seqkit, cmd=f"""
        seqkit seq -m {ISLAND_LENGTH_BP} {ireps.container} > large_reps.fna
    """)
    n_large = sum(1 for line in open("large_reps.fna") if line.startswith(">"))
    Log.Info(f"{n_large} vOTU representatives >= {ISLAND_LENGTH_BP} bp")

    genes = Path("genomad_output/large_reps_annotate/large_reps_genes.tsv")
    if n_large > 0:
        context.ExecWithEnv(env=img_genomad, cmd=f"""
            /usr/local/bin/_entrypoint.sh genomad annotate large_reps.fna genomad_output {idb.container} -t {threads}
        """)
        assert genes.exists(), f"genomad annotate wrote no {genes}"

    # No vOTU cleared the 100 kb gate: the island filter has nothing to score either, so an
    # empty table is correct, not a failed run.
    o.local.write_bytes(genes.read_bytes() if genes.exists() else b"")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reps,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=6)),
)
