from pathlib import Path

from ..std.data_types import StdTypes
from ..models.libraries import DataTypeLibrary, TransformInstanceLibrary

base_dir = Path(__file__).parent
types_path = base_dir / "transforms/_metadata/types"
dtypes = StdTypes()

# Copy type library
dtypes.Save(types_path / "std.yml")
