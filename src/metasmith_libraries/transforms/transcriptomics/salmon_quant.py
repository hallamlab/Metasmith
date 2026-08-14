from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"))
r1      = model.AddRequirement(lib.GetType("sequences::zipped_forward_short_reads"), parents={pair})
r2      = model.AddRequirement(lib.GetType("sequences::zipped_reverse_short_reads"), parents={pair})
idx     = model.AddRequirement(lib.GetType("transcriptomics::salmon_index"))
image   = model.AddRequirement(lib.GetType("env::salmon.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::salmon_quant"))

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
            salmon quant \
                -i {iidx.container} \
                -l A \
                -1 {ir1.container} \
                -2 {ir2.container} \
                --validateMappings \
                -o salmon_out \
                -p {threads}
        """,
    )
    context.LocalShell(f"cp salmon_out/quant.sf {iout.local}")
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
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    )
)
