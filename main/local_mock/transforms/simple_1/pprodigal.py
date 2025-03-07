from pathlib import Path
from metasmith.pythonapi import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
def protocol(context: ExecutionContext):
    Log.Info("this is pprodigal!")
    container = context.inputs[lib.GetType("metagenomics::oci_image_pprodigal")]
    Log.Info(f"container: [{container}] exists [{container.exists()}]")
    context.shell.Exec(f"touch orfs.faa")
    return ExecutionResult(success=True)

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
