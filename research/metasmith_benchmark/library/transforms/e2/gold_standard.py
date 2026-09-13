import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
sam     = model.AddRequirement(lib.GetType("e2::minimap2.env"))
polars  = model.AddRequirement(lib.GetType("e2::polars.env"))
# The vote score_reference_amber.py runs over E1's contigs, so both arms get the same gold standard.
vote    = model.AddRequirement(lib.GetType("e2::cami_gold_standard.py"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
truth   = model.AddRequirement(lib.GetType("e2::read_truth"), parents={meta})
asm     = model.AddRequirement(lib.GetType("e2::assembly"), parents={meta})
bam     = model.AddRequirement(lib.GetType("e2::binning_bam"), parents={asm})
out     = model.AddProduct(lib.GetType("e2::contig_gold_standard"))


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    itruth = context.Input(truth)
    iasm = context.Input(asm)
    ibam = context.Input(bam)
    ivote = context.Input(vote)
    iout = context.Output(out)
    sample = json.loads(Path(imeta.local).read_text())["sample"]
    cpus = context.params.get("cpus") or 1

    context.ExecWithEnv(env=sam, cmd=f"""
        samtools view -@ {cpus} -F 0x904 {ibam.container} | cut -f1,3 > read_contig.tsv
        samtools faidx {iasm.container} --fai-idx contigs.fai
        cut -f1,2 contigs.fai > contig_lengths.tsv
    """)
    context.ExecWithEnv(env=polars, cmd=f"""
        python {ivote.container} read_contig.tsv {itruth.container} contig_lengths.tsv {sample} {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=4, memory=Size.GB(16), duration=Duration(hours=2)),
)
