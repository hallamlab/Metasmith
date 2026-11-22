from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

package = model.AddRequirement(lib.GetType("licensed::deeptmhmm_package"))
recipe  = model.AddRequirement(lib.GetType("lib::licensed_deeptmhmm"))
image   = model.AddProduct(lib.GetType("env::deeptmhmm.env"))


def protocol(context: ExecutionContext):
    iimage = context.Output(image)
    context.BuildImage(recipe=context.Input(recipe), out=iimage, files={"package": context.Input(package)})
    return ExecutionResult(
        manifest=[{image: iimage.local}],
        success=iimage.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=package,
    # The build pulls a public base image and, for some tools, pip wheels.
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
