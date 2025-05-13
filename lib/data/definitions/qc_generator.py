from pathlib import Path
from metasmith.python_api import DataInstanceLibrary, DataTypeLibrary

base_dir = Path(__file__).parent
xgdb_path = base_dir / "../qc.xgdb"
xgdb = DataInstanceLibrary(xgdb_path)

dtypes_path = base_dir / "../../data_types/qc.yml"
dtypes = DataTypeLibrary.Load(dtypes_path)
xgdb.AddTypeLibrary("qc", dtypes)

fastqc_path = base_dir / "containers/fastqc"
longqc_path = base_dir / "containers/longqc"
xgdb.Add(
    items = [
        (fastqc_path, "fastqc.oci.uri", "qc::oci_image_fastqc"),
        (longqc_path, "longqc.oci.uri", "qc::oci_image_longqc")
    ],
)

xgdb.Save()
