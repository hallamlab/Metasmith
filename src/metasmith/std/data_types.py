from ..models.solver import Endpoint
from ..models.libraries import DataTypeLibrary

def StdTypes() -> DataTypeLibrary:
    std_data_types = DataTypeLibrary()

    std_data_types["short_reads"] = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file",
            "data": "Short sequence"
        }
    })
    std_data_types["long_reads"] = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file",
            "data": "Long sequence"
        }
    })
    std_data_types["long_reads_miniasm"] = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file"
        }
    })
    std_data_types["long_reads_gfa"] = Endpoint.Unpack({
        "properties": {
            "format": "GFA file",
            "data": "Miniasm assembly graph"
        }
    })
    std_data_types["long_reads_self_mappings"] = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file",
            "data": "All-vs-all read self-mappings via minimap2"
        }
    })

    std_data_types["read_stats"] = Endpoint.Unpack({
        "properties": {
            "format": "Directory",
            "data": "Read statistics"
        }
    })

    std_data_types["short_reads_accession"] = Endpoint.Unpack({
        "properties": {
            "format": "Plaintext file",
            "data": "Accession number associated with short reads"
        }
    })
    std_data_types["long_reads_accession"] = Endpoint.Unpack({
        "properties": {
            "format": "Plaintext file",
            "data": "Accession number associated with long reads"
        }
    })

    std_data_types["long_reads_filtered"] = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file",
            "data": "Long reads filtered by filtlong"
        }
    })
    std_data_types["short_reads_trimmed"] = Endpoint.Unpack({
        "properties": {
            "format": "Sequence file",
            "data": "Short reads trimmed by Trimmomatic"
        }
    })
    std_data_types["long_reads_assembly"] = Endpoint.Unpack({
        "properties": {
            "format": "Directory",
            "data": "Sequence assembly",
            "from": "Long reads"
        }
    })
    std_data_types["short_reads_assembly"] = Endpoint.Unpack({
        "properties": {
            "format": "Directory",
            "data": "Sequence assembly",
            "from": "Short reads"
        }
    })
    std_data_types["hybrid_assembly"] = Endpoint.Unpack({
        "properties": {
            "format": "Directory",
            "data": "Sequence assembly",
            "from": "Long reads, improved by short reads"
        }
    })
    # TODO!
    # Update other assembly types to split the actual assembly file and "rest"
    std_data_types["assembly"] = Endpoint.Unpack({
        "properties": {
            "data": "Sequence assembly"
        }
    })
    std_data_types["sequence_alignment_map"] = Endpoint.Unpack({
        "properties": {
            "format": "SAM",
            "data": "Sequence Alignment/Map file listing read placements"
        }
    })
    std_data_types["binary_alignment_map"] = Endpoint.Unpack({
        "properties": {
            "format": "BAM",
            "data": "Binary, compressed SAM file"
        }
    })

    std_data_types["coding_sequences"] = Endpoint.Unpack({
        "properties": {
            "format": "FAA",
            "data": "Amino acid sequences of all CDS"
        }
    })
    std_data_types["gene_features"] = Endpoint.Unpack({
        "properties": {
            "format": "GFF3",
            "data": "CDS coordinates"
        }
    })
    std_data_types["bakta_database"] = Endpoint.Unpack({
        "properties": {
            "data": "BAKTA database",
            "format": "Directory",
        }
    })
    std_data_types["bakta_database_light"] = Endpoint.Unpack({
        "properties": {
            "data": "BAKTA database",
            "format": "Directory",
            "size": "light"
        }
    })
    std_data_types["bakta_database_full"] = Endpoint.Unpack({
        "properties": {
            "data": "BAKTA database",
            "format": "Directory",
            "size": "full"
        }
    })
    std_data_types["bakta_annotations"] = Endpoint.Unpack({
        "properties": {
            "format": "Directory",
            "data": "BAKTA-annotated genome"
        }
    })
    std_data_types["kofamscan_profile"] = Endpoint.Unpack({
        "properties": {
            "format": "Directory | .hmm",
            "data": "Kofamscan profile database"
        }
    })
    std_data_types["kofamscan_ko_list"] = Endpoint.Unpack({
        "properties": {
            "format": "TSV",
            "data": "KO list"
        }
    })
    std_data_types["kofamscan_annotations"] = Endpoint.Unpack({
        "properties": {
            "format": "TSV",
            "data": "Kofamscan-annotated genome"
        }
    })
    std_data_types["cazy_ref"] = Endpoint.Unpack({
        "properties": {
            "format": "FASTA",
            "data": "CAZY reference database"
        }
    })
    std_data_types["cazy_annotations"] = Endpoint.Unpack({
        "properties": {
            "format": "TSV",
            "data": "CAZY alignment annotations"
        }
    })
    std_data_types["busco_ref"] = Endpoint.Unpack({
        "properties": {
            "format": "FASTA",
            "data": "BUSCO reference database"
        }
    })
    std_data_types["busco_annotations"] = Endpoint.Unpack({
        "properties": {
            "format": "TSV",
            "data": "BUSCO alignment annotations"
        }
    })
    std_data_types["busco_map"] = Endpoint.Unpack({
        "properties": {
            "format": "info",
            "data": "BUSCO mappings of ID to species"
        }
    })
    std_data_types["functional_annotations"] = Endpoint.Unpack({
        "properties": {
            "format": "Directory",
            "data": "Compiled functional annotations from all sources"
        }
    })


    std_data_types["oci_image_fastqc"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["fastqc"]
        }
    })
    std_data_types["oci_image_longqc"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["longqc"]
        }
    })
    std_data_types["oci_image_fasterq_dump"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["fasterq_dump"]
        }
    })
    std_data_types["oci_image_filtlong"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["filtlong"]
        }
    })
    std_data_types["oci_image_flye"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["flye"]
        }
    })
    std_data_types["oci_image_pilon"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["pilon"]
        }
    })
    std_data_types["oci_image_trimmomatic"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["trimmomatic"]
        }
    })
    std_data_types["oci_image_megahit"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["megahit"]
        }
    })
    std_data_types["oci_image_minimap2"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["minimap2"]
        }
    })
    std_data_types["oci_image_samtools"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["samtools"]
        }
    })
    std_data_types["oci_image_prodigal"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["prodigal"]
        }
    })
    std_data_types["oci_image_bakta"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["bakta"]
        }
    })
    std_data_types["oci_image_kofamscan"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["kofamscan"]
        }
    })
    std_data_types["oci_image_diamond"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["diamond"]
        }
    })
    std_data_types["oci_image_miniasm"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["miniasm"]
        }
    })
    std_data_types["oci_image_gfatools"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["gfatools"]
        }
    })
    std_data_types["oci_image_polypolish"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["polypolish"]
        }
    })
    std_data_types["oci_image_ubuntu"] = Endpoint.Unpack({
        "properties": {
            "format": "Software container",
            "data": "OCI",
            "provides": ["GNU coreutils"]
        }
    })

    return std_data_types
