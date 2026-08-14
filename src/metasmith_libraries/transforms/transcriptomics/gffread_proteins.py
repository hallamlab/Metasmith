from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
exp   = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
gtf   = model.AddRequirement(lib.GetType("transcriptomics::braker3_gff"), parents={exp})
asm   = model.AddRequirement(lib.GetType("sequences::assembly"), parents={exp})
image = model.AddRequirement(lib.GetType("env::gffread.env"))
out   = model.AddProduct(lib.GetType("sequences::orfs"))

def protocol(context: ExecutionContext):
    igtf = context.Input(gtf)
    iasm = context.Input(asm)
    iout = context.Output(out)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"gffread {igtf.container} -g {iasm.container} -y {iout.container}",
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    )
)
