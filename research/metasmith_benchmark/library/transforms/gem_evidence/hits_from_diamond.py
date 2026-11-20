from metasmith.python_api import *
import shutil

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
hits  = model.AddRequirement(lib.GetType("bench::bigg_diamond_hits"))
out   = model.AddProduct(lib.GetType("bench::bigg_hits_diamond"))


def protocol(context: ExecutionContext):
    ihits = context.Input(hits)
    iout  = context.Output(out)
    shutil.copyfile(ihits.local, iout.local)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=hits,
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=10)),
)
