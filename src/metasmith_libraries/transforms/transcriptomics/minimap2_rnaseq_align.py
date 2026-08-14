"""Align RNA-seq reads to an organellar reference genome using minimap2 (prokaryotic, no splice awareness)."""

from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
ref      = model.AddRequirement(lib.GetType("transcriptomics::organellar_reference"))
pair     = model.AddRequirement(lib.GetType("sequences::read_pair"))
r1       = model.AddRequirement(lib.GetType("sequences::zipped_forward_short_reads"), parents={pair})
r2       = model.AddRequirement(lib.GetType("sequences::zipped_reverse_short_reads"), parents={pair})
mm2_img  = model.AddRequirement(lib.GetType("env::minimap2.env"))
sam_img  = model.AddRequirement(lib.GetType("env::samtools.env"))
out      = model.AddProduct(lib.GetType("transcriptomics::organellar_bam"))

def protocol(context: ExecutionContext):
    iref = context.Input(ref)
    ipair = context.Input(pair)
    ir1  = context.Input(r1)
    ir2  = context.Input(r2)
    iout = context.Output(out)
    threads = context.params.get("cpus")
    threads = 4 if threads is None else threads

    # Read sample name from the read_pair file
    with open(ipair.local) as f:
        sample_name = f.read().strip()

    # minimap2: short-read mode (-x sr), no splice awareness = prokaryotic
    # Add read group with sample name so downstream tools can identify the sample
    context.ExecWithEnv().ifContainerDo(
        env=mm2_img,
        cmd=f"""\
            minimap2 -a -x sr -t {threads} \
                -R '@RG\\tID:{sample_name}\\tSM:{sample_name}' \
                {iref.container} {ir1.container} {ir2.container} \
                > aligned.sam
        """,
    )

    # samtools: sort and index
    context.ExecWithEnv().ifContainerDo(
        env=sam_img,
        cmd=f"""\
            samtools sort -@ {threads} -o {iout.container} aligned.sam && \
            samtools index {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=pair,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
