from metasmith.python_api import *

lib        = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model      = Transform()

exp        = model.AddRequirement(lib.GetType("fabfos::experiment"))
gpr        = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={exp})
conditions = model.AddRequirement(lib.GetType("ecspr::conditions"), parents={exp})
pairs      = model.AddRequirement(lib.GetType("ecspr::atom_pairs"))
direction  = model.AddRequirement(lib.GetType("ecspr::direction_ratios"))
img_ecspr  = model.AddRequirement(lib.GetType("env::ecspr.env"))
out        = model.AddProduct(lib.GetType("ecspr::results"))

PROBE = "ground"
ELEMENT = "C"
LEAK = "1e-6"


def protocol(context: ExecutionContext):
    igpr  = context.Input(gpr)
    icond = context.Input(conditions)
    ipair = context.Input(pairs)
    idir  = context.Input(direction)
    iout  = context.Output(out)

    cmd = f"""
        ecspr --where
        ecspr {PROBE} \
            --gpr {igpr.container} \
            --conditions {icond.container} \
            --atom-pairs {ipair.container} \
            --direction {idir.container} \
            --element {ELEMENT} --leak {LEAK} \
            --shard-dir ecspr_shards \
            --log ecspr.log \
            --out {iout.container}
    """
    context.ExecWithEnv(env=img_ecspr, cmd=cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=16,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    )
)
