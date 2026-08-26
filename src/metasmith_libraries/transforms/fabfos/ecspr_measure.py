from metasmith.python_api import *

lib        = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model      = Transform()

exp        = model.AddRequirement(lib.GetType("fabfos::experiment"))
gpr        = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={exp})
conditions = model.AddRequirement(lib.GetType("ecspr::conditions"), parents={exp})
tests      = model.AddRequirement(lib.GetType("ecspr::ecspr_tests"), parents={exp})
pairs      = model.AddRequirement(lib.GetType("ecspr::atom_pairs"))
direction  = model.AddRequirement(lib.GetType("ecspr::direction_ratios"))
img_ecspr  = model.AddRequirement(lib.GetType("env::ecspr.env"))
out        = model.AddProduct(lib.GetType("ecspr::results"))

PROBE = "ground"
ELEMENT = "C"
LEAK = "1e-6"


def protocol(context: ExecutionContext):
    # EVERY GPR TABLE THE GROUP CARRIES, not the first one. `--gpr` takes nargs="+" and
    # a conditions table names its tables by BASENAME, so a run staging a host table
    # beside a clone table has to hand the probe both -- `Input` returns
    # `input_group[0]`, which would silently measure the host alone and report it as
    # the whole experiment.
    igpr  = context.InputGroup(gpr)
    icond = context.Input(conditions)
    itest = context.Input(tests)
    ipair = context.Input(pairs)
    idir  = context.Input(direction)
    iout  = context.Output(out)

    gpr_args = " ".join(str(g.container) for g in igpr)
    cmd = f"""
        ecspr --where
        ecspr {PROBE} \
            --gpr {gpr_args} \
            --conditions {icond.container} \
            --tests {itest.container} \
            --atom-pairs {ipair.container} \
            --direction {idir.container} \
            --element {ELEMENT} --leak {LEAK} \
            --shard-dir ecspr_shards \
            --log ecspr.log \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=img_ecspr, cmd=cmd) \
        .ifVirtualEnvDo(env=img_ecspr, cmd=cmd)

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
