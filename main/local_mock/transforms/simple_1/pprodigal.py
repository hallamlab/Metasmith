from pathlib import Path
from metasmith.pythonapi import *

# todo: url for more consistency
lib = DataTypeLibrary.Load("/home/tony/workspace/tools/Metasmith/main/local_mock/prototypes/metagenomics.dev3.yml")

def protocol(context: ExecutionContext):
    Log.Info("this is pprodigal!")
    for x in context.output:
        context.Shell(f"touch {x.source.address}")
    return ExecutionResult(success=True)

model = Transform()
dep = model.AddRequirement(node=lib["oci_image_pprodigal"])
dep = model.AddRequirement(node=lib["contigs"])

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        model.AddProduct(lib["orfs_faa"]): Path("orfs.faa"),
    },
)
