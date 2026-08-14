from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
image   = model.AddRequirement(lib.GetType("env::salmon.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::salmon_index"))

def protocol(context: ExecutionContext):
    iasm=context.Input(asm)
    iout=context.Output(out)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"salmon index -t {iasm.container} -i salmon_idx -k 31",
    )
    context.LocalShell(f"mv salmon_idx {iout.local}")
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
    group_by=asm,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    )
)
