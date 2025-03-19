from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
def protocol(context: ExecutionContext):
    Log.Info("this is pprodigal!")
    container_key = lib.GetType("metagenomics::oci_image_pprodigal")
    container = context._inputs[container_key]
    Log.Info(f"container: [{container.local}] exists [{container.local.exists()}]")
    contigs = context._inputs[lib.GetType("metagenomics::contigs")]
    orfs = context._outputs[lib.GetType("metagenomics::orfs_faa")]
    context.external_shell.Exec(
        cmd = """
        echo "hello from outside"
        pwd -P
        """,
        timeout=None,
    )
    context.ExecWithContainer(
        image = container_key,
        cmd = f"""
        echo "hello from container"
        ls /
        head -n 1 {contigs.container} >{orfs.container}
        """,
    )
    Log.Info(f"expects [{orfs.local}]")
    return ExecutionResult(success=orfs.local.exists())

model = Transform()
dep = model.AddRequirement(node=lib.GetType("metagenomics::oci_image_pprodigal"))
dep = model.AddRequirement(node=lib.GetType("metagenomics::contigs"))

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        model.AddProduct(node=lib.GetType("metagenomics::orfs_faa")): "orfs.faa",
    },
)
