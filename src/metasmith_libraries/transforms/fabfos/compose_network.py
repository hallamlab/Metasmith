from metasmith.python_api import *

lib          = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model        = Transform()

exp          = model.AddRequirement(lib.GetType("fabfos::experiment"))
gpr          = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={exp})
conditions   = model.AddRequirement(lib.GetType("ecspr::conditions"), parents={exp})
compositions = model.AddRequirement(lib.GetType("ecspr::compositions"), parents={exp})
pairs        = model.AddRequirement(lib.GetType("ecspr::atom_pairs"))
direction    = model.AddRequirement(lib.GetType("ecspr::direction_ratios"))
names        = model.AddRequirement(lib.GetType("ecspr::metabolite_names"))
blacklist    = model.AddRequirement(lib.GetType("ecspr::carrier_blacklist"))
img_ecspr    = model.AddRequirement(lib.GetType("env::ecspr.env"))
out          = model.AddProduct(lib.GetType("ecspr::composed_network"))

G0 = "1.0"
ELEMENTS = "C N P S"


def protocol(context: ExecutionContext):
    # THE REFERENCE PAIR TABLE COMES IN, A COMPOSED ONE GOES OUT, and they are different
    # types on purpose. The reference is a static function of the MNXR/MNXM id space and
    # is shared by every run; a composed table carries member-prefixed ids and bridge rows
    # that mean nothing to any other network. Producing `ecspr::atom_pairs` here would
    # make one type mean both, and a measurement could then be handed the wrong one with
    # nothing to say so.
    igpr  = context.InputGroup(gpr)
    icond = context.Input(conditions)
    icomp = context.Input(compositions)
    ipair = context.Input(pairs)
    idir  = context.Input(direction)
    inam  = context.Input(names)
    ibl   = context.Input(blacklist)
    iout  = context.Output(out)

    gpr_args = " ".join(str(g.container) for g in igpr)
    cmd = f"""
        ecspr --where
        ecspr compose \
            --gpr {gpr_args} \
            --conditions {icond.container} \
            --compositions {icomp.container} \
            --atom-pairs {ipair.container} \
            --direction {idir.container} \
            --names {inam.container} \
            --blacklist {ibl.container} \
            --g0 {G0} --elements {ELEMENTS} \
            --log compose.log \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=img_ecspr, cmd=cmd) \
        .ifVirtualEnvDo(env=img_ecspr, cmd=cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and any(iout.local.iterdir()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=4,
        memory=Size.GB(96),
        duration=Duration(hours=12),
    )
)
