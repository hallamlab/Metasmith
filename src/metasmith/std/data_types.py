from pathlib import Path

from ..models.solver import Endpoint
from ..models.libraries import DataTypeLibrary

def StdTypes() -> DataTypeLibrary:
    short_reads = Endpoint.Unpack({
    "properties": {
        "format": "Sequence file",
        "data": "DNA sequence"
    }})
    long_reads = Endpoint.Unpack({
    "properties": {
        "format": "Sequence file",
        "data": "DNA sequence"
    }})
    read_stats = Endpoint.Unpack({
    "properties": {
        "format": "Read statistics",
        "data": "HTML"
    }})
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

    std_data_types = DataTypeLibrary()
    std_data_types["short_reads"] = short_reads
    std_data_types["long_reads"] = short_reads
    std_data_types["read_stats"] = read_stats
    std_data_types["oci_image_fastqc"] = fastqc_image
    std_data_types["oci_image_longqc"] = longqc_image

    return std_data_types
