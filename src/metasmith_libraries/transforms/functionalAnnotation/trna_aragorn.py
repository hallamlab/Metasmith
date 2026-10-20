from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
aragorn = model.AddRequirement(lib.GetType("env::aragorn.env"))
python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
rna_lib = model.AddRequirement(lib.GetType("lib::rna_scan"))
out     = model.AddProduct(lib.GetType("annotation::trna_gff"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    irna = context.Input(rna_lib)
    iout = context.Output(out)

    context.ExecWithEnv(env=aragorn, cmd=f"aragorn -t -l -gcbact -w -o aragorn.txt {iasm.container}")
    context.ExecWithEnv(env=python, cmd=f"""
        PYTHONPATH={irna.container} python3 {irna.container}/trna.py aragorn \
            --raw aragorn.txt --out {iout.container}
    """)

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
        duration=Duration(hours=6),
    ),
)
