from metasmith.models.solver import Endpoint
from metasmith.models.libraries import DataTypeLibrary
from pathlib import Path

# Define the data types
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

# Organize into a DataTypeLibrary
qc_data_types = DataTypeLibrary()
qc_data_types["short_reads"] = short_reads
qc_data_types["long_reads"] = short_reads
qc_data_types["read_stats"] = read_stats
qc_data_types["oci_image_fastqc"] = fastqc_image
qc_data_types["oci_image_longqc"] = longqc_image

# Save the library to a YAML file within a directory
base_dir = Path(__file__).parent
out_path = base_dir / "../qc.yml"
qc_data_types.Save(out_path)

print(f"Saved QC data types library to: {out_path}")
