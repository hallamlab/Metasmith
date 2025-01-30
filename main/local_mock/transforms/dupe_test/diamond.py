from pathlib import Path
from metasmith import DataInstance, DataTypeLibrary, TransformInstance, ExecutionContext, ExecutionResult

def protocol(context: ExecutionContext):
    print("this is diamond2!")
    return ExecutionResult()

HERE = Path(__file__).parent
lib = DataTypeLibrary.Load((HERE/"../../prototypes/metagenomics.yml").resolve())

TransformInstance.Register(
    container="docker://bschiffthaler/diamond:2.0.14",
    protocol=protocol,
    input_signature={
        lib["orfs_faa"],
        lib["diamond_protein_reference"],
    },
    output_signature={
        DataInstance(Path("annotations.csv"), lib["orf_annotations"]),
    },
)
