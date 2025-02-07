from pathlib import Path
from metasmith import DataInstance, DataTypeLibrary, TransformInstance, ExecutionContext, ExecutionResult, Log, Container

def protocol(context: ExecutionContext):
    Log.Info("this is pprodigal!")
    for k, x in context.outputs.items():
        context.Shell(f"touch {x.source}")
    return ExecutionResult(success=True)

HERE = Path(__file__).parent
lib = DataTypeLibrary.Load((HERE/"../../prototypes/metagenomics.yml").resolve())

TransformInstance.Register(
    protocol=protocol,
    input_signature={
        lib["oci_image_pprodigal"],
        lib["contigs"],
    },
    output_signature={
        DataInstance(Path("orfs.faa"), lib["orfs_faa"]),
    },
)
