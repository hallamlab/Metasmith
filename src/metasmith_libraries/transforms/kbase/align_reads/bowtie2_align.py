from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::bowtie2.env"))
# bowtie2 writes SAM and nothing in its image can sort one. Two containers cannot
# share a pipe, so the sort is a second dispatch rather than the second half of
# this one, and the SAM lands in the task directory in between.
sam_env = model.AddRequirement(lib.GetType("env::samtools.env"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={exp})
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={exp})
out     = model.AddProduct(lib.GetType("transcriptomics::bowtie2_bam"))

def protocol(context: ExecutionContext):
    iasm=context.Input(asm)
    ireads=context.Input(reads)
    iout=context.Output(out)

    threads = context.params.get('cpus')
    threads = 4 if threads is None else threads

    # A bacterial transcript is colinear with the genome, so a spliced aligner invents
    # introns to raise its own score. That is why this exists beside `star_align.py`
    # rather than reusing it.
    _cmd = f"""\
            bowtie2-build --threads {threads} {iasm.container} ref
            bowtie2 -p {threads} -x ref -U {ireads.container} -S aligned.sam
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    _cmd = f"""\
            samtools sort -@ {threads} -o {iout.container} aligned.sam
            samtools index {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=sam_env, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
