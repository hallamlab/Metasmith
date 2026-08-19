from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::equilibrator.env"))

annot      = model.AddRequirement(lib.GetType("interm::direction_annotation"))
vocab      = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))

bakelib    = model.AddRequirement(lib.GetType("buildlib::ecspr"))

ratios     = model.AddProduct(lib.GetType("ref::direction_ratios"))


def protocol(context: ExecutionContext):
    iann = context.Input(annot)
    ivoc = context.Input(vocab)
    ilib = context.Input(bakelib)
    iout = context.Output(ratios)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"
    cmd = f"""
        set -e
        {py} -m ecspr.bake.metabolism direction \
            --direction {iann.container} \
            --vocab {ivoc.container} \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{ratios: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)
