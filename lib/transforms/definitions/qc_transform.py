from pathlib import Path
from metasmith.models.libraries import DataTypeLibrary, TransformInstanceLibrary

base_dir = Path(__file__).parent
transforms_path = base_dir / "../qc"
dtypes_path = base_dir / "../../data_types/qc.yml"

transforms = TransformInstanceLibrary(transforms_path)
dtypes = DataTypeLibrary.Load(dtypes_path)

_ = transforms.AddTypeLibrary("qc", dtypes)
_ = transforms.AddStub("fastqc")
_ = transforms.AddStub("longqc")
transforms.Save()
