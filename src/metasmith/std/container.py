from pathlib import Path
from typing import Tuple

from ..models.libraries import DataInstanceLibrary

def StdContainers() -> DataInstanceLibrary:
    xgdb = DataInstanceLibrary("std_containers.xgdb")

    base_dir = Path(__file__).parent

    def _container_item(name: str) -> Tuple[Path, str, str]:
        return (base_dir / f"containers/{name}", f"{name}.oci.uri", f"std::oci_image_{name}")

    xgdb.Add(
        items = [
            _container_item("fastqc"),
            _container_item("longqc"),
            _container_item("fasterq_dump"),
            _container_item("filtlong"),
            _container_item("flye"),
            _container_item("trimmomatic"),
            _container_item("megahit"),
            _container_item("bwa"),
            _container_item("samtools"),
            _container_item("pilon"),
        ],
    )

    return xgdb
