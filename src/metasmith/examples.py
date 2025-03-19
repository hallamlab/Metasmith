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
    trans_path = EXAMPLES_DIR/"transforms/gene_annotation.xgdb"
    transforms = TransformInstanceLibrary.Load(trans_path)
    return dtypes, xgdb, refdb, transforms
