import json
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_mm2 = model.AddRequirement(lib.GetType("env::minimap2.env"))
img_sam = model.AddRequirement(lib.GetType("env::samtools.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::reads"), parents={meta})
rstats  = model.AddRequirement(lib.GetType("sequences::read_qc_stats"), parents={meta})
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={meta})
bam_out = model.AddProduct(lib.GetType("alignment::bam"), parents={asm})

def protocol(context: ExecutionContext):
    irmeta = context.Input(meta)
    ireads = context.Input(reads)
    irstats = context.Input(rstats)
    iasm = context.Input(asm)
    ibam = context.Output(bam_out)

    with open(irmeta.local) as j:
        read_meta = json.load(j)
    length_class = read_meta["length_class"]
    assert length_class in {"short", "long"}, f"unknown length_class: [{length_class}]"
    is_long = length_class == "long"

    with open(irstats.local) as j:
        read_stats = json.load(j)
    q = read_stats["mean_quality"]

    if is_long:
        if q >= 30:
            preset = "-x asm5"
        elif q >= 20:
            preset = "-x asm10"
        else:
            preset = "-x asm20"
    else:
        preset = "-x sr"

    cpus = context.params.get("cpus")
    cpus_mm2 = "" if cpus is None else f"-t {cpus}"
    cpus_sam = "" if cpus is None else f"-@ {cpus}"

    temp_sam = "temp.sam"
    bam_file = "aligned.bam"
    context.ExecWithEnv().ifContainerDo(
        env=img_mm2,
        cmd=f"""
            minimap2 {preset} -a -2 {cpus_mm2} \
                {iasm.container} {ireads.container} > {temp_sam}
        """
    )

    context.ExecWithEnv().ifContainerDo(
        env=img_sam,
        cmd=f"""
            samtools view {cpus_sam} -b {temp_sam} \
                | samtools sort {cpus_sam} -o {bam_file} -O bam
            samtools index {cpus_sam} -c {bam_file}
            rm -f {temp_sam}
        """
    )

    context.LocalShell(f"cp {bam_file} {ibam.local}")

    return ExecutionResult(
        manifest=[{
            bam_out: ibam.local,
        }],
        success=ibam.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)
