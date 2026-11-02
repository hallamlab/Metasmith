import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::megahit.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::trimmed_short_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::megahit_assembly"))


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    ireads = context.Input(reads)
    iout = context.Output(out)
    sample = json.loads(Path(imeta.local).read_text())["sample"]
    cpus = context.params.get("cpus") or 1
    # nf-core/mag passes the task's whole memory as bytes.
    mem_bytes = int((context.params.get("memory") or 16) * 1024**3)

    context.ExecWithEnv(env=image, cmd=f"""
        megahit -m {mem_bytes} -t {cpus} --12 {ireads.container} --out-prefix MEGAHIT-{sample}
        cp megahit_out/MEGAHIT-{sample}.contigs.fa {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=12)),
)
