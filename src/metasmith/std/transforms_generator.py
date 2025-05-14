from pathlib import Path

from ..std.data_types import StdTypes
from ..models.libraries import DataTypeLibrary, TransformInstanceLibrary

base_dir = Path(__file__).parent
transforms_path = base_dir / "transforms"

transforms = TransformInstanceLibrary(transforms_path)
dtypes = StdTypes()

# Always copy type library in case it has changed
_ = transforms.AddTypeLibrary("std", dtypes)
transforms.Save()
