from pathlib import Path
from metasmith.python_api import DataInstanceLibrary, DataTypeLibrary

base_dir = Path(__file__).parent
xgdb_path = base_dir / "std_containers.xgdb"
xgdb = DataInstanceLibrary(xgdb_path)

dtypes_path = base_dir / "../data_types/std_types.yml"
dtypes = DataTypeLibrary.Load(dtypes_path)
xgdb.AddTypeLibrary("std", dtypes)

fastqc_path = base_dir / "containers/fastqc"
longqc_path = base_dir / "containers/longqc"
xgdb.Add(
    items = [
        (fastqc_path, "fastqc.oci.uri", "std::oci_image_fastqc"),
        (longqc_path, "longqc.oci.uri", "std::oci_image_longqc")
    ],
)

xgdb.Save()
