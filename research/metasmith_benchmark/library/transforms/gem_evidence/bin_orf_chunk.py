from metasmith.python_api import *
import shutil

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
orfs  = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
chunk = model.AddProduct(lib.GetType("sequences::orf_chunk"))


# A bin's ORFs as the one chunk the standard kofamscan transform reads. A metaGEM bin holds a
# few thousand proteins, about one chunk's worth, so it is not split.
def protocol(context: ExecutionContext):
    iorfs  = context.Input(orfs)
    ichunk = context.Output(chunk)
    shutil.copyfile(iorfs.local, ichunk.local)
    return ExecutionResult(
        manifest=[{chunk: ichunk.local}],
        success=ichunk.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=10)),
)
