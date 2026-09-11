from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::seqkit.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
minlen  = model.AddRequirement(lib.GetType("sequences::min_contig_length"))
out     = model.AddProduct(lib.GetType("sequences::filtered_assembly"))

def protocol(context: ExecutionContext):
    iasm=context.Input(asm)
    iminlen=context.Input(minlen)
    iout=context.Output(out)

    with open(iminlen.local) as f:
        min_len = int(f.read().strip())

    _cmd = f"""\
            seqkit seq --min-len {min_len} {iasm.container} > {iout.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(minutes=30),
    ),
)
