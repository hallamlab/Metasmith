from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"))
r1      = model.AddRequirement(lib.GetType("sequences::zipped_forward_short_reads"), parents={pair})
r2      = model.AddRequirement(lib.GetType("sequences::zipped_reverse_short_reads"), parents={pair})
idx     = model.AddRequirement(lib.GetType("transcriptomics::star_index"))
image   = model.AddRequirement(lib.GetType("env::star.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::star_bam"))

def protocol(context: ExecutionContext):
    ir1=context.Input(r1)
    ir2=context.Input(r2)
    iidx=context.Input(idx)
    iout=context.Output(out)
    threads = context.params.get('cpus')
    threads = 8 if threads is None else threads
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            STAR --genomeDir {iidx.container} \
                --readFilesIn {ir1.container} {ir2.container} \
                --readFilesCommand zcat \
                --outSAMtype BAM SortedByCoordinate \
                --outSAMstrandField intronMotif \
                --limitBAMsortRAM 10000000000 \
                --runThreadN {threads} \
                --outFileNamePrefix star_out/
        """,
    )
    context.LocalShell(f"mv star_out/Aligned.sortedByCoord.out.bam {iout.local}")
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=pair,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    )
)
