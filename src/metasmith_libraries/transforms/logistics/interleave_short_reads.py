from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"))
r1      = model.AddRequirement(lib.GetType("sequences::forward_short_reads"), parents={pair})
r2      = model.AddRequirement(lib.GetType("sequences::reverse_short_reads"), parents={pair})
image   = model.AddRequirement(lib.GetType("env::bbtools.env"))
out     = model.AddProduct(lib.GetType("sequences::short_reads"))

def protocol(context: ExecutionContext):
    ir1=context.Input(r1)
    ir2=context.Input(r2)
    iout=context.Output(out)
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    # out=stdout.fq
    # ^ this actually tells reformat.sh to output to stdout
    # the suffix indicates format and compression
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f'''
        reformat.sh \
            in1="{ir1.container}" \
            in2="{ir2.container}" \
            out=stdout.fq \
        | pigz {threads} > {iout.container}
        '''
    )
    return ExecutionResult(
        manifest=[{
            out: iout.local
        }],
        success=iout.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=r1,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=3),
    )
)
