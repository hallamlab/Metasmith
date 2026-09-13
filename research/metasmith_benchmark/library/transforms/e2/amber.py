import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::amber.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
asm     = model.AddRequirement(lib.GetType("e2::assembly"), parents={meta})
table   = model.AddRequirement(lib.GetType("e2::das_tool_contig_to_bin"), parents={asm})
gold    = model.AddRequirement(lib.GetType("e2::contig_gold_standard"), parents={asm})
results = model.AddProduct(lib.GetType("e2::amber_results"))
per_bin = model.AddProduct(lib.GetType("e2::amber_bin_metrics"))

LABEL = "DASTool"


def protocol(context: ExecutionContext):
    itable = context.Input(table)
    igold = context.Input(gold)
    oresults = context.Output(results)
    oper_bin = context.Output(per_bin)

    sample_id = next(
        line.strip().split(":", 1)[1]
        for line in Path(igold.local).read_text().splitlines()
        if line.startswith("@SampleID:")
    )
    # MEGAHIT names contigs k141_<N> in every assembly, so a gold standard from another sample
    # shares nearly every contig name with this table. Only @SampleID tells them apart.
    sample = json.loads(Path(context.Input(meta).local).read_text())["sample"]
    if sample_id != sample:
        Log.Error(f"gold standard @SampleID [{sample_id}] is not this group's sample [{sample}]")
        return ExecutionResult(manifest=[], success=False)
    with open("prediction.tsv", "w") as f:
        f.write(f"@Version:0.9.1\n@SampleID:{sample_id}\n\n@@SEQUENCEID\tBINID\n")
        f.write(Path(itable.local).read_text())

    context.ExecWithEnv(env=image, cmd=f"""
        amber.py -g {igold.container} -l {LABEL} -o amber_out --skip_gs prediction.tsv
        cp amber_out/results.tsv {oresults.container}
        cp amber_out/genome/{LABEL}/metrics_per_bin.tsv {oper_bin.container}
    """)

    return ExecutionResult(
        manifest=[{results: oresults.local, per_bin: oper_bin.local}],
        success=oresults.local.exists() and oper_bin.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)
