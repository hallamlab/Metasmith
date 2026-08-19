from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
exp    = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
counts = model.AddRequirement(lib.GetType("transcriptomics::gene_count_table"), parents={exp})
image  = model.AddRequirement(lib.GetType("env::pydeseq2.env"))
helpers = model.AddRequirement(lib.GetType("lib::transcriptomics"))
out    = model.AddProduct(lib.GetType("transcriptomics::deseq2_results"))

def protocol(context: ExecutionContext):
    icounts = context.Input(counts)
    ihelp = context.Input(helpers)
    iout    = context.Output(out)
    cpus    = context.params.get("cpus")
    cpus    = 4 if cpus is None else cpus

    _cmd = f"python {ihelp.container}/deseq2.py {icounts.container} {iout.container} {cpus}"
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
