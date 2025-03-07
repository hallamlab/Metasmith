from pathlib import Path
from metasmith.pythonapi import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
def protocol(context: ExecutionContext):
    Log.Info("this is diamond!")
    container = context.inputs[lib.GetType("metagenomics::oci_image_diamond")]
    Log.Info(f"container: [{container}] exists [{container.exists()}]")
    context.shell.Exec(f"touch annotations.csv")
    return ExecutionResult(success=True)

model = Transform()
dep = model.AddRequirement(node=lib.GetType("metagenomics::oci_image_diamond"))
dep = model.AddRequirement(node=lib.GetType("metagenomics::orfs_faa"))
dep = model.AddRequirement(node=lib.GetType("metagenomics::protein_reference_diamond"))

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        model.AddProduct(node=lib.GetType("metagenomics::orf_annotations")): "annotations.csv",
    },
)
