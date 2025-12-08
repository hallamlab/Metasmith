from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
dep     = model.AddRequirement(lib.GetType("mock::start"))
outa     = model.AddProduct(lib.GetType("mock::a"))
model.NewProductGroup()
outb     = model.AddProduct(lib.GetType("mock::b"))

def protocol(context: ExecutionContext):
    dep_path = context.Input(dep)
    iouta = context.Output(outa)
    ioutb = context.Output(outb)
    context.external_shell.Exec(f"touch {iouta.external} && touch {ioutb.external}")
    return ExecutionResult(
        manifest=[
            {
                outa: iouta.local,
            },
            {
                outb: ioutb.local,
            },
        ],
        success=iouta.local.exists() or ioutb.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
)
