from metasmith.python_api import *
import gzip

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

# Nothing in this transform's lineage records parity, so it is read from the file: this
# library's paired clean_short_reads are interleaved, and two mates share a read name up
# to a trailing /1 or /2.
def _is_interleaved(path) -> bool:
    with open(path, "rb") as raw:
        gzipped = raw.read(2) == b"\x1f\x8b"
    with (gzip.open(path, "rt") if gzipped else open(path)) as f:
        heads = [l for _, l in zip(range(8), f)][0::4]
    names = {h[1:].split()[0].removesuffix("/1").removesuffix("/2") for h in heads}
    return len(heads) == 2 and len(names) == 1

def protocol(context: ExecutionContext):
    iasm=context.Input(asm)
    ireads=context.Input(reads)
    iout=context.Output(out)

    threads = context.params.get('cpus')
    threads = 4 if threads is None else threads
    layout = "--interleaved" if _is_interleaved(ireads.local) else "-U"

    # A bacterial transcript is colinear with the genome, so a spliced aligner invents
    # introns to raise its own score. That is why this exists beside `star_align.py`
    # rather than reusing it.
    _cmd = f"""\
            bowtie2-build --threads {threads} {iasm.container} ref
            bowtie2 -p {threads} -x ref {layout} {ireads.container} -S aligned.sam
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    _cmd = f"""\
            samtools sort -@ {threads} -o {iout.container} aligned.sam
            samtools index {iout.container}
        """
    context.ExecWithEnv(env=sam_env, cmd=_cmd)

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
