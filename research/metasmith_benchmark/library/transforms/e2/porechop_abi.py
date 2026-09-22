import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::porechop_abi.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::long_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::adapter_trimmed_long_reads"))


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    ireads = context.Input(reads)
    iout = context.Output(out)
    platform = json.loads(Path(imeta.local).read_text())["platform"]
    # nf-core/mag adapter-trims Nanopore reads only and passes PacBio reads straight through.
    assert platform in {"OXFORD_NANOPORE", "OXFORD_NANOPORE_HQ"}, f"Porechop ABI runs on Nanopore reads, not [{platform}]"
    cpus = context.params.get("cpus") or 1

    context.ExecWithEnv(env=image, cmd=f"""
        porechop_abi --input {ireads.container} --threads {cpus} --output trimmed.fastq.gz
        mv trimmed.fastq.gz {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=12)),
)
