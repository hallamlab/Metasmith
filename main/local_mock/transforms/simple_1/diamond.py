from pathlib import Path
from metasmith import DataInstance, DataTypeLibrary, TransformInstance, ExecutionContext, ExecutionResult, Log, Container

def protocol(context: ExecutionContext):
    Log.Info("this is diamond!")
    for k, x in context.outputs.items():
        context.Shell(f"touch {x.source}")
    return ExecutionResult(success=True)

HERE = Path(__file__).parent
lib = DataTypeLibrary.Load((HERE/"../../prototypes/metagenomics.yml").resolve())

TransformInstance.Register(
    protocol=protocol,
    input_signature={
        lib["oci_image_diamond"],
        lib["orfs_faa"],
        lib["protein_reference_diamond"],
    },
    output_signature={
        DataInstance(Path("annotations.csv"), lib["orf_annotations"]),
    },
)
