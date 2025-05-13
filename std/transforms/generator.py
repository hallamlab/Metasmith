from pathlib import Path
from metasmith.models.libraries import DataTypeLibrary, TransformInstanceLibrary

base_dir = Path(__file__).parent
transforms_path = base_dir / "std_transforms"
dtypes_path = base_dir / "../data_types/std_types.yml"

transforms = TransformInstanceLibrary(transforms_path)
dtypes = DataTypeLibrary.Load(dtypes_path)

_ = transforms.AddTypeLibrary("std", dtypes)
transforms.Save()
