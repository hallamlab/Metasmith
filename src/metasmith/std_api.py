import glob
from pathlib import Path

from .models.libraries import DataInstanceLibrary, DataTypeLibrary, TransformInstanceLibrary

def Std() -> tuple[DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary]:
    base_dir = Path(__file__).parent
    containers = DataInstanceLibrary.Load(base_dir / "std/containers")
    transforms: TransformInstanceLibrary = TransformInstanceLibrary.Load(base_dir / "std/transforms")

    assert len(transforms.types) == 2 # transforms, std
    dtypes: DataTypeLibrary = DataTypeLibrary()

    for k, e in transforms.types["std"].types.items():
        dtypes[k] = e
    for k, e in containers.types["std"].types.items():
        assert k in dtypes
        assert dtypes[k] == e

    return dtypes, containers, transforms
