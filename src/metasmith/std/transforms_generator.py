from pathlib import Path

from ..std.data_types import std_types
from ..models.libraries import DataTypeLibrary, TransformInstanceLibrary

base_dir = Path(__file__).parent
transforms_path = base_dir / "transforms"

transforms = TransformInstanceLibrary(transforms_path)
dtypes = std_types()

# Always copy type library in case it has changed
_ = transforms.AddTypeLibrary("std", dtypes)
transforms.Save()
