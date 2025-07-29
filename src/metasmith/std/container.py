from pathlib import Path

from ..models.libraries import DataInstanceLibrary

def StdContainers() -> DataInstanceLibrary:
    base_dir = Path(__file__).parent
    xgdb = DataInstanceLibrary(base_dir / "std_containers.xgdb")
    xgdb.Save()


    def _container_item(name: str) -> tuple[Path, str, str]:
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
            _container_item("miniasm"),
            _container_item("minimap2"),
            _container_item("samtools"),
            _container_item("pilon"),
            _container_item("bakta"),
            _container_item("diamond"),
            _container_item("kofamscan"),
            _container_item("prodigal"),
            _container_item("ubuntu"),
        ],
    )

    return xgdb
