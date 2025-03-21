from pathlib import Path
import shutil

from .logging import Log
from .models.libraries import DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary
HERE = Path(__file__).parent
EXAMPLES_DIR = HERE/"example_resources"

def GenomicsAnnotation():
    dtypes = DataTypeLibrary.Load(EXAMPLES_DIR/"types/minimal_genomics.yml")
    xgdb_path = EXAMPLES_DIR/"data/fosmid.xgdb"
    xgdb = DataInstanceLibrary.Load(xgdb_path)
    refdb_path = EXAMPLES_DIR/"data/references.xgdb"
    refdb = DataInstanceLibrary.Load(refdb_path)
    trans_path = EXAMPLES_DIR/"transforms/gene_annotation"
    transforms = TransformInstanceLibrary.Load(trans_path)
    return dtypes, xgdb, refdb, transforms

def DataTypeLibraries(name: str):
    path = EXAMPLES_DIR/f"types/{name}.yml"
    assert path.exists(), f"example [{name}] does not exist"
    return DataTypeLibrary.Load(path)

def DataInstanceLibraries(name: str):
    path = EXAMPLES_DIR/f"data/{name}"
    assert path.exists(), f"example [{name}] does not exist"
    return DataInstanceLibrary.Load(path)

def TransformInstanceLibraries(name: str):
    path = EXAMPLES_DIR/f"transforms/{name}"
    assert path.exists(), f"example [{name}] does not exist"
    return TransformInstanceLibrary.Load(path)
