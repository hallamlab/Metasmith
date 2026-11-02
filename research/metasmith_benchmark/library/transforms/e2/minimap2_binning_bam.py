import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::minimap2.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::filtered_long_reads"), parents={meta})
asm     = model.AddRequirement(lib.GetType("e2::flye_assembly"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::binning_bam"))

PRESET = {
    "OXFORD_NANOPORE": "-x map-ont",
    "OXFORD_NANOPORE_HQ": "-x lr:hq",
    "PACBIO_CLR": "-x map-pb",
    "PACBIO_HIFI": "-x map-hifi",
}


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    ireads = context.Input(reads)
    iasm = context.Input(asm)
    iout = context.Output(out)
    preset = PRESET[json.loads(Path(imeta.local).read_text())["platform"]]
    cpus = context.params.get("cpus") or 2

    # MINIMAP2_ASSEMBLY_INDEX builds the index with no preset, then MINIMAP2_ASSEMBLY_ALIGN applies one.
    context.ExecWithEnv(env=image, cmd=f"""
        minimap2 -t {cpus} -d assembly.mmi {iasm.container}
        minimap2 {preset} -t {cpus} assembly.mmi {ireads.container} -a \
            | samtools sort -@ {cpus - 1} -o {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=12)),
)
