from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
scripts = model.AddRequirement(lib.GetType("lib::isolate_rnaseq"))
raw     = model.AddRequirement(lib.GetType("isolate_rnaseq::raw_count_table"))
tpm     = model.AddProduct(lib.GetType("isolate_rnaseq::tpm_table"))


def protocol(context: ExecutionContext):
    itpm = context.Output(tpm)
    context.ExecWithEnv(env=image, cmd=f"""\
        python {context.Input(scripts).container}/tpm.py {context.Input(raw).container} {itpm.container}
    """)
    return ExecutionResult(manifest=[{tpm: itpm.local}], success=itpm.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=raw,
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30)),
)
