from ast import Tuple
from pathlib import Path

from .models.libraries import DataInstanceLibrary, DataTypeLibrary, TransformInstanceLibrary
from .std.container import StdContainers
from .std.data_types import StdTypes


def Std() -> tuple[DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary]:
    dtypes: DataTypeLibrary = StdTypes()
    containers: DataInstanceLibrary = StdContainers()

    base_path = Path(__file__).parent
    transform_path = base_path / "std/transforms"
    transforms: TransformInstanceLibrary = TransformInstanceLibrary.Load(transform_path)

    return dtypes, containers, transforms
