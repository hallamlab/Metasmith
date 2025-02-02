from pathlib import Path
from metasmith import DataInstance, DataTypeLibrary, TransformInstance, ExecutionContext, ExecutionResult

def protocol(context: ExecutionContext):
    print("this is pprodigal!")
    return ExecutionResult()

HERE = Path(__file__).parent
lib = DataTypeLibrary.Load((HERE/"../../prototypes/metagenomics.yml").resolve())

TransformInstance.Register(
    container="docker://quay.io/hallamlab/external_pprodigal:1.0.1",
    protocol=protocol,
    input_signature={
        lib["contigs"],
    },
    output_signature={
        DataInstance(Path("orfs.gbk"), lib["orfs_gbk"]),
        DataInstance(Path("orfs.faa"), lib["orfs_faa"]),
    },
)
