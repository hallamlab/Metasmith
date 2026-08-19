from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
metanetx   = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
kegg       = model.AddRequirement(lib.GetType("fabfos_data::kegg"))
rhea       = model.AddRequirement(lib.GetType("fabfos_data::rhea"))
ev_lib     = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
bl         = model.AddRequirement(lib.GetType("buildlib::mnxr_lookup.py"))
bridge     = model.AddProduct(lib.GetType("ref::mnxr_lookup"))


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ikeg = context.Input(kegg)
    irhe = context.Input(rhea)
    iev  = context.Input(ev_lib)
    ibl  = context.Input(bl)
    iout = context.Output(bridge)

    cmd = f"""
            python3 {ibl.container} \
            --ev-lib {iev.container} \
            --metanetx {imnx.container} \
            --kegg {ikeg.container} \
            --rhea {irhe.container} \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{bridge: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(32), duration=Duration(hours=2)),
)
