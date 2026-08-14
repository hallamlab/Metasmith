from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::pprodigal.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
cds         = model.AddProduct(lib.GetType("sequences::orfs"))
gff         = model.AddProduct(lib.GetType("sequences::gff"))

def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    icds = context.Output(cds)
    igff = context.Output(gff)

    cpus_string = ""
    cpus = context.params.get("cpus")
    if cpus is not None:
        cpus_string = f"-T {cpus}"

    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""\
            pprodigal \
                {cpus_string} \
                -C 100 \
                -p meta \
                -i {iasm.container} \
                -a {icds.container} \
                -f gff \
                -o {igff.container}
            """,
    )
    
    return ExecutionResult(
        manifest=[{
            cds: icds.local,
            gff: igff.local,
        }],
        success=icds.local.exists() and igff.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    )
)
