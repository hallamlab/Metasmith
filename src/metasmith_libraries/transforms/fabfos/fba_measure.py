from metasmith.python_api import *

lib        = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model      = Transform()

exp        = model.AddRequirement(lib.GetType("fabfos::experiment"))
gpr        = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={exp})
conditions = model.AddRequirement(lib.GetType("ecspr::conditions"), parents={exp})
tests      = model.AddRequirement(lib.GetType("ecspr::gem_tests"), parents={exp})
frame      = model.AddRequirement(lib.GetType("ecspr::frame"), parents={exp})
media      = model.AddRequirement(lib.GetType("ecspr::media"), parents={exp})
mnx        = model.AddRequirement(lib.GetType("ref::metanetx_release"))
img_ecspr  = model.AddRequirement(lib.GetType("env::ecspr.env"))
out        = model.AddProduct(lib.GetType("ecspr::fba_results"))


def protocol(context: ExecutionContext):
    # THE SAME CONDITIONS TABLE THE CONDUCTANCE ARM READS, and every GPR table the group
    # carries, for the same reason `ecspr_measure` takes a group: a condition names its
    # tables by basename, and staging one of two resolves the condition to half of itself.
    igpr  = context.InputGroup(gpr)
    icond = context.Input(conditions)
    itest = context.Input(tests)
    imnx  = context.Input(mnx)
    imed  = context.Input(media)
    iout  = context.Output(out)

    # A `gem_tests` row names its frame by stem, so every staged frame is offered and the
    # table picks. One experiment measured against two frames is then one job.
    iframe = context.InputGroup(frame)

    gpr_args = " ".join(str(g.container) for g in igpr)
    frame_args = " ".join(str(f.container) for f in iframe)
    cmd = f"""
        ecspr --where
        ecspr fba \
            --gpr {gpr_args} \
            --conditions {icond.container} \
            --tests {itest.container} \
            --frame {frame_args} \
            --media {imed.container} \
            --mnx {imnx.container} \
            --mnx-cache ./mnx_cache \
            --census fba_census.json \
            --log fba.log \
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
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=24),
    )
)
