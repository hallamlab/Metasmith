from pathlib import Path

from ..models.solver import Endpoint
from ..models.libraries import DataTypeLibrary

def StdTypes() -> DataTypeLibrary:
    short_reads = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file",
            "data": "Short sequence"
        }
    })
    long_reads = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file",
            "data": "Long sequence"
        }
    })
    read_stats = Endpoint.Unpack({
        "properties": {
            "format": "Directory",
            "data": "Read statistics"
        }
    })

    fasterq_accession = Endpoint.Unpack({
        "properties": {
            "format": "Plaintext file",
            "data": "Accession number"
        }
    })

    fastqc_image = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["fastqc"]
        }
    })
    longqc_image = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["longqc"]
        }
    })
    fasterq_dump_image = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["fasterq_dump"]
        }
    })

    std_data_types = DataTypeLibrary()

    std_data_types["short_reads"] = short_reads
    std_data_types["long_reads"] = long_reads
    std_data_types["read_stats"] = read_stats

    std_data_types["fasterq_accession"] = fasterq_accession

    std_data_types["oci_image_fastqc"] = fastqc_image
    std_data_types["oci_image_longqc"] = longqc_image
    std_data_types["oci_image_fasterq_dump"] = fasterq_dump_image

    return std_data_types
