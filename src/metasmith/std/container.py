from pathlib import Path

from ..std.data_types import std_types
from ..models.libraries import DataInstanceLibrary, DataTypeLibrary

def std_images() -> DataInstanceLibrary:
    xgdb = DataInstanceLibrary("containers.xgdb")

    dtypes = std_types()
    xgdb.AddTypeLibrary(namespace="std", lib=dtypes)

    base_dir = Path(__file__).parent
    fastqc_path = base_dir / "containers/fastqc"
    longqc_path = base_dir / "containers/longqc"

    xgdb.Add(
        items = [
            (fastqc_path, "fastqc.oci.uri", "std::oci_image_fastqc"),
            (longqc_path, "longqc.oci.uri", "std::oci_image_longqc")
        ],
    )

    return xgdb
