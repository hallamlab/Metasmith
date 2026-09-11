from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::polars.env"))
matrix  = model.AddRequirement(lib.GetType("pangenome::ppanggolin_matrix"))
kofam   = model.AddRequirement(lib.GetType("annotation::kofamscan_descriptions"))
script  = model.AddRequirement(lib.GetType("lib::ppanggolin_summary.py"))
out     = model.AddProduct(lib.GetType("pangenome::ppanggolin_summary"))

def protocol(context: ExecutionContext):
    imatrix=context.Input(matrix)
    ikofam=context.Input(kofam)
    iscript=context.Input(script)
    iout=context.Output(out)

    # A readout on an existing product -- no new tool. The accessory fraction is
    # where a lifestyle difference between strains shows.
    _cmd = f"""\
            python {iscript.container} {imatrix.container} {ikofam.container} {iout.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=matrix,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
