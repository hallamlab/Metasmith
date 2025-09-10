params.home = '/scratch/phyberos/metasmith'
params.workspace = "${params.home}/runs/p5X9uuT8"
params.bootstrap = '''
CONTAINER=/msm_home
DIRECT=/scratch/phyberos/metasmith
function bootstrap {
    if [ -e $CONTAINER ]; then
        $CONTAINER/lib/msm_bootstrap $@
    elif [ -e $DIRECT ]; then
        $DIRECT/lib/msm_bootstrap $@
    else
        echo "critical error: could not find metasmith bootstrap script"
    fi
}
'''

process s0001_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0001", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0002_fasterq_long__5U3XB88a {
    publishDir "$params.output/0002", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0003_fasterq_long__5U3XB88a {
    publishDir "$params.output/0003", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0004_fasterq_long__5U3XB88a {
    publishDir "$params.output/0004", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0005_fasterq_long__5U3XB88a {
    publishDir "$params.output/0005", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0006_fasterq_long__5U3XB88a {
    publishDir "$params.output/0006", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0007_fasterq_long__5U3XB88a {
    publishDir "$params.output/0007", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0008_fasterq_long__5U3XB88a {
    publishDir "$params.output/0008", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0009_fasterq_long__5U3XB88a {
    publishDir "$params.output/0009", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0010_fasterq_long__5U3XB88a {
    publishDir "$params.output/0010", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0011_fasterq_long__5U3XB88a {
    publishDir "$params.output/0011", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0012_fasterq_long__5U3XB88a {
    publishDir "$params.output/0012", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0013_fasterq_long__5U3XB88a {
    publishDir "$params.output/0013", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0014_fasterq_long__5U3XB88a {
    publishDir "$params.output/0014", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0015_fasterq_long__5U3XB88a {
    publishDir "$params.output/0015", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0016_fasterq_long__5U3XB88a {
    publishDir "$params.output/0016", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0017_fasterq_long__5U3XB88a {
    publishDir "$params.output/0017", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0018_fasterq_long__5U3XB88a {
    publishDir "$params.output/0018", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0019_fasterq_long__5U3XB88a {
    publishDir "$params.output/0019", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0020_fasterq_long__5U3XB88a {
    publishDir "$params.output/0020", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0021_fasterq_long__5U3XB88a {
    publishDir "$params.output/0021", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0022_fasterq_long__5U3XB88a {
    publishDir "$params.output/0022", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0023_fasterq_long__5U3XB88a {
    publishDir "$params.output/0023", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0024_fasterq_long__5U3XB88a {
    publishDir "$params.output/0024", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0025_fasterq_long__5U3XB88a {
    publishDir "$params.output/0025", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0026_fasterq_long__5U3XB88a {
    publishDir "$params.output/0026", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0027_fasterq_long__5U3XB88a {
    publishDir "$params.output/0027", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0028_fasterq_long__5U3XB88a {
    publishDir "$params.output/0028", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0029_fasterq_long__5U3XB88a {
    publishDir "$params.output/0029", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0030_fasterq_long__5U3XB88a {
    publishDir "$params.output/0030", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0031_fasterq_long__5U3XB88a {
    publishDir "$params.output/0031", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0032_fasterq_long__5U3XB88a {
    publishDir "$params.output/0032", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0033_fasterq_long__5U3XB88a {
    publishDir "$params.output/0033", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0034_fasterq_long__5U3XB88a {
    publishDir "$params.output/0034", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0035_fasterq_long__5U3XB88a {
    publishDir "$params.output/0035", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0036_fasterq_long__5U3XB88a {
    publishDir "$params.output/0036", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0037_fasterq_long__5U3XB88a {
    publishDir "$params.output/0037", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0038_fasterq_long__5U3XB88a {
    publishDir "$params.output/0038", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0039_fasterq_long__5U3XB88a {
    publishDir "$params.output/0039", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0040_fasterq_long__5U3XB88a {
    publishDir "$params.output/0040", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0041_fasterq_long__5U3XB88a {
    publishDir "$params.output/0041", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0042_fasterq_long__5U3XB88a {
    publishDir "$params.output/0042", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0043_fasterq_long__5U3XB88a {
    publishDir "$params.output/0043", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0044_fasterq_long__5U3XB88a {
    publishDir "$params.output/0044", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0045_fasterq_long__5U3XB88a {
    publishDir "$params.output/0045", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0046_fasterq_long__5U3XB88a {
    publishDir "$params.output/0046", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0047_fasterq_long__5U3XB88a {
    publishDir "$params.output/0047", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0048_fasterq_long__5U3XB88a {
    publishDir "$params.output/0048", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0049_fasterq_long__5U3XB88a {
    publishDir "$params.output/0049", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0050_fasterq_long__5U3XB88a {
    publishDir "$params.output/0050", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0051_fasterq_long__5U3XB88a {
    publishDir "$params.output/0051", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0052_fasterq_long__5U3XB88a {
    publishDir "$params.output/0052", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0053_fasterq_long__5U3XB88a {
    publishDir "$params.output/0053", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0054_fasterq_long__5U3XB88a {
    publishDir "$params.output/0054", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0055_fasterq_long__5U3XB88a {
    publishDir "$params.output/0055", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0056_fasterq_long__5U3XB88a {
    publishDir "$params.output/0056", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0057_fasterq_long__5U3XB88a {
    publishDir "$params.output/0057", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0058_fasterq_long__5U3XB88a {
    publishDir "$params.output/0058", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0059_fasterq_long__5U3XB88a {
    publishDir "$params.output/0059", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0060_fasterq_long__5U3XB88a {
    publishDir "$params.output/0060", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0061_fasterq_long__5U3XB88a {
    publishDir "$params.output/0061", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0062_fasterq_long__5U3XB88a {
    publishDir "$params.output/0062", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0063_fasterq_long__5U3XB88a {
    publishDir "$params.output/0063", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0064_fasterq_long__5U3XB88a {
    publishDir "$params.output/0064", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0065_fasterq_long__5U3XB88a {
    publishDir "$params.output/0065", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0066_fasterq_long__5U3XB88a {
    publishDir "$params.output/0066", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0067_fasterq_long__5U3XB88a {
    publishDir "$params.output/0067", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0068_fasterq_long__5U3XB88a {
    publishDir "$params.output/0068", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0069_fasterq_long__5U3XB88a {
    publishDir "$params.output/0069", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0070_fasterq_long__5U3XB88a {
    publishDir "$params.output/0070", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0071_fasterq_long__5U3XB88a {
    publishDir "$params.output/0071", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0072_fasterq_long__5U3XB88a {
    publishDir "$params.output/0072", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0073_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0073", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0074_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0074", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0075_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0075", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0076_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0076", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0077_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0077", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0078_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0078", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0079_fasterq_long__5U3XB88a {
    publishDir "$params.output/0079", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0080_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0080", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0081_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0081", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0082_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0082", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0083_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0083", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0084_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0084", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0085_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0085", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0086_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0086", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0087_fasterq_long__5U3XB88a {
    publishDir "$params.output/0087", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0088_fasterq_long__5U3XB88a {
    publishDir "$params.output/0088", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0089_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0089", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0090_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0090", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0091_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0091", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0092_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0092", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0093_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0093", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0094_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0094", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0095_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0095", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0096_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0096", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0097_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0097", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0098_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0098", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0099_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0099", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0100_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0100", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0101_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0101", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0102_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0102", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0103_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0103", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0104_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0104", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0105_fasterq_long__5U3XB88a {
    publishDir "$params.output/0105", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0106_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0106", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0107_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0107", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0108_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0108", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0109_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0109", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0110_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0110", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0111_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0111", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0112_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0112", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0113_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0113", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0114_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0114", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0115_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0115", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0116_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0116", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0117_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0117", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0118_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0118", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0119_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0119", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0120_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0120", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0121_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0121", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0122_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0122", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0123_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0123", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0124_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0124", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0125_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0125", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0126_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0126", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0127_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0127", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0128_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0128", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0129_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0129", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0130_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0130", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0131_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0131", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0132_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0132", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0133_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0133", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0134_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0134", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0135_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0135", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0136_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0136", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0137_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0137", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0138_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0138", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0139_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0139", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0140_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0140", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0141_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0141", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0142_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0142", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0143_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0143", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0144_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0144", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0145_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0145", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0146_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0146", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0147_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0147", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0148_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0148", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0149_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0149", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0150_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0150", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0151_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0151", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0152_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0152", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0153_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0153", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0154_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0154", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0155_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0155", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0156_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0156", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0157_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0157", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0158_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0158", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0159_fasterq_long__5U3XB88a {
    publishDir "$params.output/0159", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0160_fasterq_long__5U3XB88a {
    publishDir "$params.output/0160", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0161_fasterq_long__5U3XB88a {
    publishDir "$params.output/0161", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0162_fasterq_long__5U3XB88a {
    publishDir "$params.output/0162", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0163_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0163", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0164_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0164", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0165_fasterq_long__5U3XB88a {
    publishDir "$params.output/0165", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0166_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0166", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0167_fasterq_long__5U3XB88a {
    publishDir "$params.output/0167", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0168_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0168", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0169_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0169", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0170_fasterq_long__5U3XB88a {
    publishDir "$params.output/0170", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0171_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0171", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0172_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0172", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0173_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0173", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0174_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0174", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0175_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0175", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0176_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0176", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0177_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0177", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0178_fasterq_long__5U3XB88a {
    publishDir "$params.output/0178", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0179_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0179", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0180_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0180", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0181_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0181", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0182_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0182", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0183_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0183", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0184_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0184", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0185_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0185", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0186_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0186", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0187_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0187", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0188_fasterq_long__5U3XB88a {
    publishDir "$params.output/0188", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0189_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0189", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0190_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0190", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0191_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0191", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0192_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0192", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0193_fasterq_long__5U3XB88a {
    publishDir "$params.output/0193", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0194_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0194", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0195_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0195", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0196_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0196", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0197_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0197", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0198_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0198", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0199_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0199", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0200_fasterq_long__5U3XB88a {
    publishDir "$params.output/0200", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0201_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0201", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0202_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0202", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0203_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0203", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0204_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0204", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0205_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0205", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0206_fasterq_long__5U3XB88a {
    publishDir "$params.output/0206", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0207_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0207", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0208_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0208", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0209_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0209", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0210_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0210", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0211_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0211", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0212_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0212", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0213_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0213", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0214_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0214", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0215_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0215", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0216_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0216", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0217_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0217", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0218_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0218", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0219_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0219", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0220_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0220", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0221_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0221", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0222_fasterq_long__5U3XB88a {
    publishDir "$params.output/0222", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0223_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0223", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0224_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0224", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0225_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0225", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0226_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0226", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0227_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0227", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0228_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0228", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0229_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0229", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0230_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0230", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0231_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0231", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0232_fasterq_long__5U3XB88a {
    publishDir "$params.output/0232", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0233_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0233", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0234_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0234", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0235_fasterq_long__5U3XB88a {
    publishDir "$params.output/0235", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0236_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0236", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0237_fasterq_long__5U3XB88a {
    publishDir "$params.output/0237", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0238_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0238", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0239_fasterq_long__5U3XB88a {
    publishDir "$params.output/0239", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0240_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0240", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0241_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0241", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0242_fasterq_long__5U3XB88a {
    publishDir "$params.output/0242", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0243_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0243", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0244_fasterq_long__5U3XB88a {
    publishDir "$params.output/0244", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0245_fasterq_long__5U3XB88a {
    publishDir "$params.output/0245", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0246_fasterq_long__5U3XB88a {
    publishDir "$params.output/0246", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0247_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0247", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0248_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0248", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0249_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0249", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0250_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0250", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0251_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0251", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0252_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0252", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0253_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0253", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0254_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0254", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0255_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0255", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0256_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0256", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0257_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0257", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0258_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0258", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0259_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0259", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0260_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0260", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0261_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0261", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0262_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0262", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0263_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0263", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0264_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0264", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0265_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0265", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0266_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0266", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0267_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0267", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0268_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0268", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0269_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0269", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0270_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0270", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0271_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0271", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0272_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0272", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0273_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0273", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0274_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0274", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0275_fasterq_long__5U3XB88a {
    publishDir "$params.output/0275", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0276_fasterq_long__5U3XB88a {
    publishDir "$params.output/0276", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0277_fasterq_long__5U3XB88a {
    publishDir "$params.output/0277", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0278_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0278", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0279_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0279", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0280_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0280", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0281_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0281", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0282_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0282", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0283_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0283", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0284_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0284", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0285_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0285", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0286_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0286", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0287_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0287", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0288_fasterq_long__5U3XB88a {
    publishDir "$params.output/0288", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0289_fasterq_long__5U3XB88a {
    publishDir "$params.output/0289", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0290_fasterq_long__5U3XB88a {
    publishDir "$params.output/0290", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0291_fasterq_long__5U3XB88a {
    publishDir "$params.output/0291", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0292_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0292", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0293_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0293", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0294_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0294", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0295_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0295", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0296_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0296", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0297_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0297", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0298_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0298", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0299_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0299", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0300_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0300", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0301_fasterq_long__5U3XB88a {
    publishDir "$params.output/0301", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0302_fasterq_long__5U3XB88a {
    publishDir "$params.output/0302", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0303_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0303", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0304_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0304", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0305_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0305", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0306_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0306", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0307_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0307", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0308_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0308", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0309_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0309", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0310_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0310", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0311_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0311", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0312_fasterq_long__5U3XB88a {
    publishDir "$params.output/0312", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0313_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0313", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0314_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0314", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0315_fasterq_long__5U3XB88a {
    publishDir "$params.output/0315", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0316_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0316", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0317_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0317", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0318_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0318", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0319_fasterq_long__5U3XB88a {
    publishDir "$params.output/0319", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0320_fasterq_long__5U3XB88a {
    publishDir "$params.output/0320", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0321_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0321", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0322_fasterq_long__5U3XB88a {
    publishDir "$params.output/0322", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0323_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0323", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0324_fasterq_long__5U3XB88a {
    publishDir "$params.output/0324", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0325_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0325", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0326_fasterq_long__5U3XB88a {
    publishDir "$params.output/0326", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0327_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0327", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0328_fasterq_long__5U3XB88a {
    publishDir "$params.output/0328", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0329_fasterq_long__5U3XB88a {
    publishDir "$params.output/0329", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0330_fasterq_long__5U3XB88a {
    publishDir "$params.output/0330", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0331_fasterq_long__5U3XB88a {
    publishDir "$params.output/0331", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0332_fasterq_long__5U3XB88a {
    publishDir "$params.output/0332", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0333_fasterq_long__5U3XB88a {
    publishDir "$params.output/0333", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0334_fasterq_long__5U3XB88a {
    publishDir "$params.output/0334", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0335_fasterq_long__5U3XB88a {
    publishDir "$params.output/0335", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0336_fasterq_long__5U3XB88a {
    publishDir "$params.output/0336", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0337_fasterq_long__5U3XB88a {
    publishDir "$params.output/0337", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0338_fasterq_long__5U3XB88a {
    publishDir "$params.output/0338", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0339_fasterq_long__5U3XB88a {
    publishDir "$params.output/0339", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0340_fasterq_long__5U3XB88a {
    publishDir "$params.output/0340", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0341_fasterq_long__5U3XB88a {
    publishDir "$params.output/0341", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0342_fasterq_long__5U3XB88a {
    publishDir "$params.output/0342", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0343_fasterq_long__5U3XB88a {
    publishDir "$params.output/0343", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0344_fasterq_long__5U3XB88a {
    publishDir "$params.output/0344", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0345_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0345", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0346_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0346", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0347_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0347", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0348_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0348", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0349_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0349", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0350_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0350", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0351_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0351", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0352_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0352", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0353_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0353", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0354_fasterq_long__5U3XB88a {
    publishDir "$params.output/0354", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0355_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0355", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0356_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0356", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0357_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0357", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0358_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0358", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0359_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0359", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0360_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0360", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0361_fasterq_long__5U3XB88a {
    publishDir "$params.output/0361", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0362_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0362", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0363_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0363", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0364_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0364", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0365_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0365", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0366_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0366", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0367_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0367", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0368_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0368", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0369_fasterq_long__5U3XB88a {
    publishDir "$params.output/0369", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0370_fasterq_long__5U3XB88a {
    publishDir "$params.output/0370", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0371_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0371", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0372_fasterq_long__5U3XB88a {
    publishDir "$params.output/0372", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0373_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0373", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0374_fasterq_long__5U3XB88a {
    publishDir "$params.output/0374", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0375_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0375", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0376_fasterq_long__5U3XB88a {
    publishDir "$params.output/0376", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0377_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0377", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0378_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0378", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0379_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0379", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0380_fasterq_long__5U3XB88a {
    publishDir "$params.output/0380", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0381_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0381", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0382_fasterq_long__5U3XB88a {
    publishDir "$params.output/0382", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0383_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0383", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0384_fasterq_long__5U3XB88a {
    publishDir "$params.output/0384", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0385_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0385", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0386_fasterq_long__5U3XB88a {
    publishDir "$params.output/0386", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0387_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0387", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0388_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0388", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0389_fasterq_long__5U3XB88a {
    publishDir "$params.output/0389", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0390_fasterq_long__5U3XB88a {
    publishDir "$params.output/0390", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0391_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0391", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0392_fasterq_long__5U3XB88a {
    publishDir "$params.output/0392", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0393_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0393", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0394_fasterq_long__5U3XB88a {
    publishDir "$params.output/0394", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0395_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0395", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0396_fasterq_long__5U3XB88a {
    publishDir "$params.output/0396", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0397_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0397", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0398_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0398", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0399_fasterq_long__5U3XB88a {
    publishDir "$params.output/0399", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0400_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0400", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0401_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0401", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0402_fasterq_long__5U3XB88a {
    publishDir "$params.output/0402", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0403_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0403", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0404_fasterq_long__5U3XB88a {
    publishDir "$params.output/0404", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0405_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0405", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0406_fasterq_long__5U3XB88a {
    publishDir "$params.output/0406", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0407_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0407", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0408_fasterq_long__5U3XB88a {
    publishDir "$params.output/0408", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0409_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0409", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0410_fasterq_long__5U3XB88a {
    publishDir "$params.output/0410", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0411_fasterq_long__5U3XB88a {
    publishDir "$params.output/0411", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0412_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0412", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0413_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0413", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0414_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0414", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0415_fasterq_long__5U3XB88a {
    publishDir "$params.output/0415", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0416_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0416", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0417_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0417", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0418_fasterq_long__5U3XB88a {
    publishDir "$params.output/0418", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0419_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0419", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0420_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0420", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0421_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0421", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0422_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0422", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0423_fasterq_long__5U3XB88a {
    publishDir "$params.output/0423", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0424_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0424", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0425_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0425", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0426_fasterq_long__5U3XB88a {
    publishDir "$params.output/0426", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0427_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0427", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0428_fasterq_long__5U3XB88a {
    publishDir "$params.output/0428", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0429_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0429", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0430_fasterq_long__5U3XB88a {
    publishDir "$params.output/0430", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0431_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0431", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0432_fasterq_long__5U3XB88a {
    publishDir "$params.output/0432", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0433_fasterq_long__5U3XB88a {
    publishDir "$params.output/0433", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0434_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0434", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0435_fasterq_long__5U3XB88a {
    publishDir "$params.output/0435", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0436_fasterq_long__5U3XB88a {
    publishDir "$params.output/0436", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0437_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0437", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0438_fasterq_long__5U3XB88a {
    publishDir "$params.output/0438", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0439_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0439", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0440_fasterq_long__5U3XB88a {
    publishDir "$params.output/0440", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0441_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0441", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0442_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0442", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0443_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0443", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0444_fasterq_long__5U3XB88a {
    publishDir "$params.output/0444", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0445_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0445", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0446_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0446", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0447_fasterq_long__5U3XB88a {
    publishDir "$params.output/0447", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0448_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0448", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0449_fasterq_long__5U3XB88a {
    publishDir "$params.output/0449", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0450_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0450", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0451_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0451", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0452_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0452", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0453_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0453", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0454_fasterq_long__5U3XB88a {
    publishDir "$params.output/0454", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0455_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0455", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0456_fasterq_long__5U3XB88a {
    publishDir "$params.output/0456", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0457_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0457", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0458_fasterq_long__5U3XB88a {
    publishDir "$params.output/0458", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0459_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0459", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0460_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0460", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0461_fasterq_long__5U3XB88a {
    publishDir "$params.output/0461", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0462_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0462", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0463_fasterq_long__5U3XB88a {
    publishDir "$params.output/0463", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0464_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0464", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0465_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0465", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0466_fasterq_long__5U3XB88a {
    publishDir "$params.output/0466", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0467_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0467", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0468_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0468", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0469_fasterq_long__5U3XB88a {
    publishDir "$params.output/0469", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0470_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0470", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0471_fasterq_long__5U3XB88a {
    publishDir "$params.output/0471", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0472_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0472", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0473_fasterq_long__5U3XB88a {
    publishDir "$params.output/0473", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0474_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0474", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0475_fasterq_long__5U3XB88a {
    publishDir "$params.output/0475", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0476_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0476", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0477_fasterq_long__5U3XB88a {
    publishDir "$params.output/0477", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0478_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0478", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0479_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0479", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0480_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0480", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0481_fasterq_long__5U3XB88a {
    publishDir "$params.output/0481", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0482_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0482", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0483_fasterq_long__5U3XB88a {
    publishDir "$params.output/0483", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0484_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0484", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0485_fasterq_long__5U3XB88a {
    publishDir "$params.output/0485", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0486_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0486", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0487_fasterq_long__5U3XB88a {
    publishDir "$params.output/0487", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0488_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0488", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0489_fasterq_long__5U3XB88a {
    publishDir "$params.output/0489", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0490_fasterq_long__5U3XB88a {
    publishDir "$params.output/0490", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0491_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0491", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0492_fasterq_long__5U3XB88a {
    publishDir "$params.output/0492", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0493_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0493", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0494_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0494", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0495_fasterq_long__5U3XB88a {
    publishDir "$params.output/0495", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0496_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0496", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0497_fasterq_long__5U3XB88a {
    publishDir "$params.output/0497", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0498_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0498", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0499_fasterq_long__5U3XB88a {
    publishDir "$params.output/0499", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0500_fasterq_long__5U3XB88a {
    publishDir "$params.output/0500", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0501_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0501", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0502_fasterq_long__5U3XB88a {
    publishDir "$params.output/0502", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0503_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0503", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0504_fasterq_long__5U3XB88a {
    publishDir "$params.output/0504", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0505_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0505", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0506_fasterq_long__5U3XB88a {
    publishDir "$params.output/0506", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0507_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0507", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0508_fasterq_long__5U3XB88a {
    publishDir "$params.output/0508", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0509_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0509", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0510_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0510", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0511_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0511", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0512_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0512", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0513_fasterq_long__5U3XB88a {
    publishDir "$params.output/0513", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0514_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0514", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0515_fasterq_long__5U3XB88a {
    publishDir "$params.output/0515", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0516_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0516", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0517_fasterq_long__5U3XB88a {
    publishDir "$params.output/0517", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0518_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0518", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0519_fasterq_long__5U3XB88a {
    publishDir "$params.output/0519", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0520_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0520", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0521_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0521", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0522_fasterq_long__5U3XB88a {
    publishDir "$params.output/0522", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0523_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0523", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0524_fasterq_long__5U3XB88a {
    publishDir "$params.output/0524", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0525_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0525", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0526_fasterq_long__5U3XB88a {
    publishDir "$params.output/0526", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0527_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0527", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0528_fasterq_long__5U3XB88a {
    publishDir "$params.output/0528", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0529_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0529", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0530_fasterq_long__5U3XB88a {
    publishDir "$params.output/0530", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0531_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0531", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0532_fasterq_long__5U3XB88a {
    publishDir "$params.output/0532", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0533_fasterq_long__5U3XB88a {
    publishDir "$params.output/0533", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0534_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0534", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0535_fasterq_long__5U3XB88a {
    publishDir "$params.output/0535", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0536_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0536", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0537_fasterq_long__5U3XB88a {
    publishDir "$params.output/0537", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0538_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0538", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0539_fasterq_long__5U3XB88a {
    publishDir "$params.output/0539", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0540_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0540", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0541_fasterq_long__5U3XB88a {
    publishDir "$params.output/0541", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0542_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0542", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0543_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0543", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0544_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0544", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0545_fasterq_long__5U3XB88a {
    publishDir "$params.output/0545", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0546_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0546", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0547_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0547", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0548_fasterq_long__5U3XB88a {
    publishDir "$params.output/0548", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0549_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0549", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0550_fasterq_long__5U3XB88a {
    publishDir "$params.output/0550", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0551_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0551", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0552_fasterq_long__5U3XB88a {
    publishDir "$params.output/0552", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0553_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0553", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0554_fasterq_long__5U3XB88a {
    publishDir "$params.output/0554", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0555_fasterq_long__5U3XB88a {
    publishDir "$params.output/0555", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0556_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0556", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0557_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0557", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0558_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0558", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0559_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0559", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0560_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0560", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0561_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0561", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0562_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0562", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0563_fasterq_long__5U3XB88a {
    publishDir "$params.output/0563", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0564_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0564", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0565_fasterq_long__5U3XB88a {
    publishDir "$params.output/0565", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0566_fasterq_long__5U3XB88a {
    publishDir "$params.output/0566", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0567_fasterq_long__5U3XB88a {
    publishDir "$params.output/0567", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0568_fasterq_long__5U3XB88a {
    publishDir "$params.output/0568", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0569_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0569", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0570_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0570", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0571_fasterq_long__5U3XB88a {
    publishDir "$params.output/0571", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0572_fasterq_long__5U3XB88a {
    publishDir "$params.output/0572", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0573_fasterq_long__5U3XB88a {
    publishDir "$params.output/0573", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0574_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0574", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0575_fasterq_long__5U3XB88a {
    publishDir "$params.output/0575", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0576_fasterq_long__5U3XB88a {
    publishDir "$params.output/0576", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0577_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0577", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0578_fasterq_long__5U3XB88a {
    publishDir "$params.output/0578", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0579_fasterq_long__5U3XB88a {
    publishDir "$params.output/0579", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0580_fasterq_long__5U3XB88a {
    publishDir "$params.output/0580", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0581_fasterq_long__5U3XB88a {
    publishDir "$params.output/0581", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0582_fasterq_long__5U3XB88a {
    publishDir "$params.output/0582", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0583_fasterq_long__5U3XB88a {
    publishDir "$params.output/0583", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0584_fasterq_long__5U3XB88a {
    publishDir "$params.output/0584", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0585_fasterq_long__5U3XB88a {
    publishDir "$params.output/0585", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0586_fasterq_long__5U3XB88a {
    publishDir "$params.output/0586", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0587_fasterq_long__5U3XB88a {
    publishDir "$params.output/0587", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0588_fasterq_long__5U3XB88a {
    publishDir "$params.output/0588", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0589_fasterq_long__5U3XB88a {
    publishDir "$params.output/0589", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0590_fasterq_long__5U3XB88a {
    publishDir "$params.output/0590", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0591_fasterq_long__5U3XB88a {
    publishDir "$params.output/0591", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0592_fasterq_long__5U3XB88a {
    publishDir "$params.output/0592", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0593_fasterq_long__5U3XB88a {
    publishDir "$params.output/0593", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0594_fasterq_long__5U3XB88a {
    publishDir "$params.output/0594", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0595_fasterq_long__5U3XB88a {
    publishDir "$params.output/0595", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0596_fasterq_long__5U3XB88a {
    publishDir "$params.output/0596", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0597_fasterq_long__5U3XB88a {
    publishDir "$params.output/0597", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0598_fasterq_long__5U3XB88a {
    publishDir "$params.output/0598", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0599_fasterq_long__5U3XB88a {
    publishDir "$params.output/0599", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0600_fasterq_long__5U3XB88a {
    publishDir "$params.output/0600", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0601_fasterq_long__5U3XB88a {
    publishDir "$params.output/0601", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0602_fasterq_long__5U3XB88a {
    publishDir "$params.output/0602", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0603_fasterq_long__5U3XB88a {
    publishDir "$params.output/0603", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0604_fasterq_long__5U3XB88a {
    publishDir "$params.output/0604", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0605_fasterq_long__5U3XB88a {
    publishDir "$params.output/0605", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0606_fasterq_long__5U3XB88a {
    publishDir "$params.output/0606", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0607_fasterq_long__5U3XB88a {
    publishDir "$params.output/0607", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0608_fasterq_long__5U3XB88a {
    publishDir "$params.output/0608", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0609_fasterq_long__5U3XB88a {
    publishDir "$params.output/0609", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0610_fasterq_long__5U3XB88a {
    publishDir "$params.output/0610", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0611_fasterq_long__5U3XB88a {
    publishDir "$params.output/0611", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0612_fasterq_long__5U3XB88a {
    publishDir "$params.output/0612", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0613_fasterq_long__5U3XB88a {
    publishDir "$params.output/0613", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0614_fasterq_long__5U3XB88a {
    publishDir "$params.output/0614", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0615_fasterq_long__5U3XB88a {
    publishDir "$params.output/0615", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0616_fasterq_long__5U3XB88a {
    publishDir "$params.output/0616", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0617_fasterq_long__5U3XB88a {
    publishDir "$params.output/0617", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0618_fasterq_long__5U3XB88a {
    publishDir "$params.output/0618", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0619_fasterq_long__5U3XB88a {
    publishDir "$params.output/0619", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0620_fasterq_long__5U3XB88a {
    publishDir "$params.output/0620", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0621_fasterq_long__5U3XB88a {
    publishDir "$params.output/0621", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0622_fasterq_long__5U3XB88a {
    publishDir "$params.output/0622", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0623_fasterq_long__5U3XB88a {
    publishDir "$params.output/0623", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0624_fasterq_long__5U3XB88a {
    publishDir "$params.output/0624", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0625_fasterq_long__5U3XB88a {
    publishDir "$params.output/0625", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0626_fasterq_long__5U3XB88a {
    publishDir "$params.output/0626", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0627_fasterq_long__5U3XB88a {
    publishDir "$params.output/0627", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0628_fasterq_long__5U3XB88a {
    publishDir "$params.output/0628", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0629_fasterq_long__5U3XB88a {
    publishDir "$params.output/0629", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0630_fasterq_long__5U3XB88a {
    publishDir "$params.output/0630", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0631_fasterq_long__5U3XB88a {
    publishDir "$params.output/0631", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0632_fasterq_long__5U3XB88a {
    publishDir "$params.output/0632", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0633_fasterq_long__5U3XB88a {
    publishDir "$params.output/0633", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0634_fasterq_long__5U3XB88a {
    publishDir "$params.output/0634", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0635_fasterq_long__5U3XB88a {
    publishDir "$params.output/0635", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0636_fasterq_long__5U3XB88a {
    publishDir "$params.output/0636", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0637_fasterq_long__5U3XB88a {
    publishDir "$params.output/0637", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0638_fasterq_long__5U3XB88a {
    publishDir "$params.output/0638", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0639_fasterq_long__5U3XB88a {
    publishDir "$params.output/0639", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0640_fasterq_long__5U3XB88a {
    publishDir "$params.output/0640", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0641_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0641", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0642_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0642", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0643_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0643", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0644_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0644", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0645_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0645", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0646_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0646", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0647_fasterq_long__5U3XB88a {
    publishDir "$params.output/0647", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0648_fasterq_long__5U3XB88a {
    publishDir "$params.output/0648", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0649_fasterq_long__5U3XB88a {
    publishDir "$params.output/0649", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0650_fasterq_long__5U3XB88a {
    publishDir "$params.output/0650", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0651_fasterq_long__5U3XB88a {
    publishDir "$params.output/0651", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0652_fasterq_long__5U3XB88a {
    publishDir "$params.output/0652", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0653_fasterq_long__5U3XB88a {
    publishDir "$params.output/0653", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0654_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0654", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0655_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0655", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0656_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0656", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0657_fasterq_long__5U3XB88a {
    publishDir "$params.output/0657", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0658_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0658", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0659_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0659", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0660_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0660", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0661_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0661", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0662_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0662", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0663_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0663", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0664_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0664", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0665_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0665", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0666_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0666", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0667_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0667", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0668_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0668", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0669_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0669", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0670_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0670", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0671_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0671", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0672_fasterq_long__5U3XB88a {
    publishDir "$params.output/0672", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0673_fasterq_long__5U3XB88a {
    publishDir "$params.output/0673", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0674_fasterq_long__5U3XB88a {
    publishDir "$params.output/0674", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0675_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0675", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0676_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0676", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0677_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0677", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0678_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0678", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0679_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0679", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0680_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0680", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0681_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0681", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0682_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0682", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0683_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0683", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0684_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0684", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0685_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0685", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0686_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0686", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0687_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0687", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0688_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0688", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0689_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0689", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0690_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0690", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0691_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0691", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0692_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0692", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0693_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0693", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0694_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0694", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0695_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0695", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0696_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0696", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0697_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0697", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0698_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0698", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0699_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0699", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0700_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0700", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0701_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0701", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0702_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0702", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0703_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0703", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0704_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0704", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0705_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0705", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0706_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0706", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0707_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0707", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0708_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0708", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0709_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0709", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0710_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0710", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0711_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0711", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0712_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0712", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0713_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0713", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0714_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0714", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0715_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0715", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0716_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0716", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0717_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0717", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0718_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0718", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0719_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0719", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0720_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0720", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0721_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0721", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0722_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0722", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0723_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0723", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0724_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0724", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0725_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0725", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0726_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0726", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0727_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0727", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0728_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0728", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0729_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0729", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0730_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0730", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0731_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0731", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0732_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0732", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0733_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0733", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0734_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0734", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0735_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0735", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0736_fasterq_long__5U3XB88a {
    publishDir "$params.output/0736", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0737_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0737", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0738_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0738", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0739_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0739", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0740_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0740", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0741_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0741", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0742_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0742", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0743_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0743", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0744_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0744", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0745_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0745", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0746_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0746", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0747_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0747", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0748_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0748", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0749_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0749", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0750_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0750", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}

process s0751_fasterq_short__JDyPTfMn {
    publishDir "$params.output/0751", mode: "copy", pattern: "dump.fastq"

    input:
        val step_index
        path _01 // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
        path _02 // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]

    output:
        path "dump.fastq"

    script:
    """
    ${params.bootstrap}
    echo "$task.cpus/$task.memory" >.command.resources
    bootstrap ${params.workspace} $step_index
    """
}


workflow {
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053403") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4029432") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053256") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155742") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732502") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853558") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287305") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14129449") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26781016") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939209") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053240") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31418077") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853601") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655902") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638124") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053266") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287284") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053360") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655896") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33627862") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR720274") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29871651") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053314") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053316") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655872") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287271") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053254") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939171") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30463421") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4029435") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4029437") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4030110") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31418076") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053337") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR19282831") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209232") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _BojeR4jY = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/helpers.py") // std::helpers_script [<{data:Python script,format:.py,provides:helpers.py}:BojeR4jY>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020265") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287279") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479505") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25462022") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020278") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR19573023") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895360") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053341") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026048") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33436066") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155743") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939167") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14790590") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053346") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053410") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853586") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29849878") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25705839") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209228") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053388") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR10839718") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287294") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053367") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053408") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26619834") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053264") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31393013") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939206") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27911466") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR28397937") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053259") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479511") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33577396") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053327") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053342") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053354") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33963973") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27732368") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020292") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR16038737") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655888") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939188") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33531175") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25516027") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27503513") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939194") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287290") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020284") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26742023") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655874") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479506") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR582985") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853600") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _UdTli8Kg = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/samtools.oci.uri") // std::oci_image_samtools [<{data:OCI,format:Software container,provides:samtools}:UdTli8Kg>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853574") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053365") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053340") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34963227") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209231") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053307") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053326") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053241") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29871220") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895351") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209243") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053366") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287314") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939203") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020289") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287324") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26035255") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4030108") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053389") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287325") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _L64ygWKM = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/fasterq_dump.oci.uri") // std::oci_image_fasterq_dump [<{data:OCI,format:Software container,provides:fasterq_dump}:L64ygWKM>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29694084") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026047") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26742021") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30628367") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853576") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29896084") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30853069") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR338138") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR35108113") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053377") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853556") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026042") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26490688") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053304") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR24855809") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939211") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053361") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020267") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053306") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _qy7fV1ty = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/cazy_visualize.py") // std::cazy_visualize_script [<{data:Python script,format:.py,provides:cazy_visualize.py}:qy7fV1ty>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655861") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026040") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053258") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287280") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853583") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155738") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053394") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655897") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053320") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053248") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655851") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053296") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26619839") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655875") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638125") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655890") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287267") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287329") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR13387963") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287263") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020263") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053287") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053310") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209547") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33577395") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853591") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732499") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939210") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053401") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026046") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939204") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479508") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838648") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287333") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655842") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853562") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053232") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR583012") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053281") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853551") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31899932") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32600066") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31899935") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053285") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26296785") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853604") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _3svnmtco = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/qc_table.py") // std::qc_table_script [<{data:Python script,format:.py,provides:qc_table.py}:3svnmtco>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655881") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287276") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33531176") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853557") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655892") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209548") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287317") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29190653") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026041") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155746") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR13387962") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853590") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053278") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655882") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209230") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939197") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30628364") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053384") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287270") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155741") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655905") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053308") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939200") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939180") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053374") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895355") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053376") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26621742") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853579") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853569") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26301289") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732490") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29010202") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053271") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287307") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155745") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020290") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29892608") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053247") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287275") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053370") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33532330") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33961694") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053280") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020291") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020283") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838640") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026043") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26490690") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26781011") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34963863") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32600069") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29871652") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053263") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853587") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895350") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053330") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053299") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053227") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27111369") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939207") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655841") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053255") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26147846") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053352") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655898") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053291") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33926287") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020274") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020287") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655895") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853603") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26742022") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939173") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR28391617") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655894") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR35108320") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4030109") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287315") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287292") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33583475") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _eDXsaY6B = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/trimmomatic.oci.uri") // std::oci_image_trimmomatic [<{data:OCI,format:Software container,provides:trimmomatic}:eDXsaY6B>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209246") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155845") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939172") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053355") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939181") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638122") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30628366") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27742817") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053261") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838649") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939177") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053386") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053378") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053391") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895358") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31393015") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR583185") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287318") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638119") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053242") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053277") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29892607") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053323") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020279") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853568") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853553") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732498") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053404") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020277") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853565") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053324") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479509") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287289") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053345") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR28415873") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053357") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31393016") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287319") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053333") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053375") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053399") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR35109383") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053301") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655886") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853561") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29731130") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287268") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853552") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR24855810") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853570") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287312") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053379") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _Gsaihvev = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/cazy_annotation.py") // std::cazy_annotation_script [<{data:Python script,format:.py,provides:cazy_annotation.py}:Gsaihvev>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053288") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR338139") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053393") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655877") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853592") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020280") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895357") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895361") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655856") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287308") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287330") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209086") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638123") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655837") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655871") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053268") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _Q5iZoUuF = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/fastqc.oci.uri") // std::oci_image_fastqc [<{data:OCI,format:Software container,provides:fastqc}:Q5iZoUuF>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287293") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32600068") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020273") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053372") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR583018") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053343") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33577394") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27558508") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _YtiKAjV9 = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/pilon.oci.uri") // std::oci_image_pilon [<{data:OCI,format:Software container,provides:pilon}:YtiKAjV9>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053319") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853572") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053298") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25462021") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25516029") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053293") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939183") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479510") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR508037") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053411") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30628365") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895363") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053279") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053382") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33627863") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _o9eSmQ5N = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/miniasm.oci.uri") // std::oci_image_miniasm [<{data:OCI,format:Software container,provides:miniasm}:o9eSmQ5N>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26621743") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053300") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053339") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838651") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR513679") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053239") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053269") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155747") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR18918233") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895354") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14129445") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655864") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655906") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053282") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732501") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939178") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655900") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853599") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655907") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29731131") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020288") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053400") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR18863879") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287313") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053236") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053315") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053373") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655836") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26619836") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853577") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053233") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287304") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287281") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939170") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853573") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27490419") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853596") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655859") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4029433") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25004942") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR582968") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655903") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655876") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838653") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053251") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR720273") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR35107389") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26619837") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053336") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27742818") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053358") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _jbt3CHkr = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/filtlong.oci.uri") // std::oci_image_filtlong [<{data:OCI,format:Software container,provides:filtlong}:jbt3CHkr>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287264") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053356") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33475715") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33627864") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25516028") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32315654") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287309") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR16038736") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32600072") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30628363") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939187") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053302") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053234") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838654") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053332") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287298") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638121") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853578") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895362") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287273") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020276") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053270") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053276") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053309") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053335") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053318") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053250") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053286") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287265") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14155376") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26781038") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27553575") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR28391618") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053305") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR35108114") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053244") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655884") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR513685") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053249") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287311") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053297") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287278") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34097264") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853564") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853598") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27911467") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287332") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939189") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939176") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655878") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34963864") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287296") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638126") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14129444") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31393017") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053334") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26035512") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053397") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR24855808") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655840") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053313") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053350") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26490689") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895356") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053396") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053402") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053364") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655883") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853550") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853566") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287301") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853597") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655904") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287316") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853554") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655899") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020282") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939196") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR582966") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020285") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655870") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR13387964") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _sOUqG4yg = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/busco_visualize.py") // std::busco_visualize_script [<{data:Python script,format:.py,provides:busco_visualize.py}:sOUqG4yg>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053407") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287274") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33436065") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287322") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR24855811") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053238") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053398") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155857") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053273") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939174") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _eNiO47nk = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/diamond.oci.uri") // std::oci_image_diamond [<{data:OCI,format:Software container,provides:diamond}:eNiO47nk>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26619835") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939186") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29644585") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053349") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053267") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR551727") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053387") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287272") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287277") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053272") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053289") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287282") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26781035") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939182") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053260") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26619838") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020275") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27945399") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053380") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155740") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33014876") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020262") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053265") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32315651") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838647") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020261") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29894949") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053246") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020266") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053257") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287299") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287266") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR23588367") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29815637") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053348") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31899933") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053290") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27967422") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655868") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27911469") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020260") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853575") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020271") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26035254") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32600071") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053294") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287310") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853567") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853581") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053409") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209612") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR610317") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838641") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14129446") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939201") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR610343") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939175") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053369") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27911461") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _3ozMVljb = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/minimap2.oci.uri") // std::oci_image_minimap2 [<{data:OCI,format:Software container,provides:minimap2}:3ozMVljb>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053329") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655858") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020270") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655880") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053237") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939208") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853580") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655866") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053344") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14129447") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053371") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287320") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939193") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655901") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053311") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287287") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32693635") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287288") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053245") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26035509") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR582990") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _5a3igX17 = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/longqc.oci.uri") // std::oci_image_longqc [<{data:OCI,format:Software container,provides:longqc}:5a3igX17>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853595") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053322") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053363") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26035511") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053253") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR1805320") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853602") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053274") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838645") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12459022") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26035510") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR10839719") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053381") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732489") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853584") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838644") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR18918234") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287331") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939169") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27111584") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655867") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14129450") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053275") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053392") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287286") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853555") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29849879") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479507") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939185") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287291") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838646") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020269") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853594") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939192") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655889") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287269") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287306") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32600067") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29364898") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020268") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053235") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _rygeClcL = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/busco_annotation.py") // std::busco_annotation_script [<{data:Python script,format:.py,provides:busco_annotation.py}:rygeClcL>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655887") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053303") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838650") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053405") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053362") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053231") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27558437") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29871221") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287295") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053331") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838639") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287327") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020281") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655854") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _QGnq1Rur = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/megahit.oci.uri") // std::oci_image_megahit [<{data:OCI,format:Software container,provides:megahit}:QGnq1Rur>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053252") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655893") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939198") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020264") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155856") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _Wxyy353y = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/prodigal.oci.uri") // std::oci_image_prodigal [<{data:OCI,format:Software container,provides:prodigal}:Wxyy353y>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209234") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053395") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32600070") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895353") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR14129448") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053347") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30628368") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053390") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053383") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26621750") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30853084") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4029431") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053325") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479504") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30628362") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655860") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25405051") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895349") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732488") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053229") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR18863880") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655869") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853560") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26621751") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053321") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020272") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30479512") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _G9t4jwWI = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/bakta.oci.uri") // std::oci_image_bakta [<{data:OCI,format:Software container,provides:bakta}:G9t4jwWI>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053359") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939190") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33583474") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939202") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25231490") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655849") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053328") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _bSbFSawZ = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/kofamscan.oci.uri") // std::oci_image_kofamscan [<{data:OCI,format:Software container,provides:kofamscan}:bSbFSawZ>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655857") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29010201") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838652") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853571") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020286") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939191") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209069") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853559") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655873") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895359") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR285493") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053368") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655863") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026044") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732500") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853585") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR33531174") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30638120") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287328") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053230") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287323") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29894950") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR496916") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053228") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053284") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27742816") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838643") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853593") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31899934") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR27558939") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026039") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655852") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR30985848") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287326") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29895352") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053351") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655879") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053385") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34732496") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287321") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655853") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939205") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939212") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853563") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053412") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287303") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853582") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053243") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053338") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655862") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053353") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4020259") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853549") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287285") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29010200") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR4026045") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32838642") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR4029436") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR26209553") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287297") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053283") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR13447460") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287283") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _M8gkT5nr = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/flye.oci.uri") // std::oci_image_flye [<{data:OCI,format:Software container,provides:flye}:M8gkT5nr>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053295") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _Uv6p8gh8 = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/atK3VRhtKmfg/script_runner.oci.uri") // std::oci_image_script_runner [<{data:OCI,format:Software container,provides:[GNU coreutils,curl,uv]}:Uv6p8gh8>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287302") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853589") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34097265") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155844") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/ERR12155739") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655909") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053406") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053292") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR20853588") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939184") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053312") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR32655891") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR28415872") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR29694083") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053317") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939168") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939199") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939179") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR513684") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31053262") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR25516030") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR31393014") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4pOv1Zyo = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/SRR34939195") // std::short_reads_accession [<{data:Accession number associated with short reads,format:Plaintext file}:4pOv1Zyo>]
    _4HMb09gR = Channel.fromPath("${params.home}/runs/p5X9uuT8/_metasmith/task/data/nkEpTTWdVaXC/DRR287300") // std::long_reads_accession [<{data:Accession number associated with long reads,format:Plaintext file}:4HMb09gR>]

    _eWB2yuLt = s0001_fasterq_short__JDyPTfMn(1, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0002_fasterq_long__5U3XB88a(2, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0003_fasterq_long__5U3XB88a(3, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0004_fasterq_long__5U3XB88a(4, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0005_fasterq_long__5U3XB88a(5, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0006_fasterq_long__5U3XB88a(6, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0007_fasterq_long__5U3XB88a(7, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0008_fasterq_long__5U3XB88a(8, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0009_fasterq_long__5U3XB88a(9, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0010_fasterq_long__5U3XB88a(10, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0011_fasterq_long__5U3XB88a(11, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0012_fasterq_long__5U3XB88a(12, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0013_fasterq_long__5U3XB88a(13, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0014_fasterq_long__5U3XB88a(14, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0015_fasterq_long__5U3XB88a(15, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0016_fasterq_long__5U3XB88a(16, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0017_fasterq_long__5U3XB88a(17, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0018_fasterq_long__5U3XB88a(18, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0019_fasterq_long__5U3XB88a(19, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0020_fasterq_long__5U3XB88a(20, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0021_fasterq_long__5U3XB88a(21, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0022_fasterq_long__5U3XB88a(22, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0023_fasterq_long__5U3XB88a(23, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0024_fasterq_long__5U3XB88a(24, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0025_fasterq_long__5U3XB88a(25, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0026_fasterq_long__5U3XB88a(26, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0027_fasterq_long__5U3XB88a(27, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0028_fasterq_long__5U3XB88a(28, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0029_fasterq_long__5U3XB88a(29, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0030_fasterq_long__5U3XB88a(30, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0031_fasterq_long__5U3XB88a(31, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0032_fasterq_long__5U3XB88a(32, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0033_fasterq_long__5U3XB88a(33, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0034_fasterq_long__5U3XB88a(34, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0035_fasterq_long__5U3XB88a(35, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0036_fasterq_long__5U3XB88a(36, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0037_fasterq_long__5U3XB88a(37, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0038_fasterq_long__5U3XB88a(38, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0039_fasterq_long__5U3XB88a(39, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0040_fasterq_long__5U3XB88a(40, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0041_fasterq_long__5U3XB88a(41, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0042_fasterq_long__5U3XB88a(42, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0043_fasterq_long__5U3XB88a(43, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0044_fasterq_long__5U3XB88a(44, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0045_fasterq_long__5U3XB88a(45, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0046_fasterq_long__5U3XB88a(46, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0047_fasterq_long__5U3XB88a(47, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0048_fasterq_long__5U3XB88a(48, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0049_fasterq_long__5U3XB88a(49, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0050_fasterq_long__5U3XB88a(50, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0051_fasterq_long__5U3XB88a(51, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0052_fasterq_long__5U3XB88a(52, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0053_fasterq_long__5U3XB88a(53, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0054_fasterq_long__5U3XB88a(54, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0055_fasterq_long__5U3XB88a(55, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0056_fasterq_long__5U3XB88a(56, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0057_fasterq_long__5U3XB88a(57, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0058_fasterq_long__5U3XB88a(58, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0059_fasterq_long__5U3XB88a(59, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0060_fasterq_long__5U3XB88a(60, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0061_fasterq_long__5U3XB88a(61, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0062_fasterq_long__5U3XB88a(62, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0063_fasterq_long__5U3XB88a(63, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0064_fasterq_long__5U3XB88a(64, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0065_fasterq_long__5U3XB88a(65, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0066_fasterq_long__5U3XB88a(66, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0067_fasterq_long__5U3XB88a(67, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0068_fasterq_long__5U3XB88a(68, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0069_fasterq_long__5U3XB88a(69, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0070_fasterq_long__5U3XB88a(70, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0071_fasterq_long__5U3XB88a(71, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0072_fasterq_long__5U3XB88a(72, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0073_fasterq_short__JDyPTfMn(73, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0074_fasterq_short__JDyPTfMn(74, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0075_fasterq_short__JDyPTfMn(75, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0076_fasterq_short__JDyPTfMn(76, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0077_fasterq_short__JDyPTfMn(77, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0078_fasterq_short__JDyPTfMn(78, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0079_fasterq_long__5U3XB88a(79, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0080_fasterq_short__JDyPTfMn(80, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0081_fasterq_short__JDyPTfMn(81, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0082_fasterq_short__JDyPTfMn(82, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0083_fasterq_short__JDyPTfMn(83, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0084_fasterq_short__JDyPTfMn(84, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0085_fasterq_short__JDyPTfMn(85, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0086_fasterq_short__JDyPTfMn(86, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0087_fasterq_long__5U3XB88a(87, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0088_fasterq_long__5U3XB88a(88, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0089_fasterq_short__JDyPTfMn(89, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0090_fasterq_short__JDyPTfMn(90, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0091_fasterq_short__JDyPTfMn(91, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0092_fasterq_short__JDyPTfMn(92, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0093_fasterq_short__JDyPTfMn(93, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0094_fasterq_short__JDyPTfMn(94, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0095_fasterq_short__JDyPTfMn(95, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0096_fasterq_short__JDyPTfMn(96, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0097_fasterq_short__JDyPTfMn(97, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0098_fasterq_short__JDyPTfMn(98, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0099_fasterq_short__JDyPTfMn(99, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0100_fasterq_short__JDyPTfMn(100, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0101_fasterq_short__JDyPTfMn(101, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0102_fasterq_short__JDyPTfMn(102, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0103_fasterq_short__JDyPTfMn(103, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0104_fasterq_short__JDyPTfMn(104, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0105_fasterq_long__5U3XB88a(105, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0106_fasterq_short__JDyPTfMn(106, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0107_fasterq_short__JDyPTfMn(107, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0108_fasterq_short__JDyPTfMn(108, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0109_fasterq_short__JDyPTfMn(109, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0110_fasterq_short__JDyPTfMn(110, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0111_fasterq_short__JDyPTfMn(111, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0112_fasterq_short__JDyPTfMn(112, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0113_fasterq_short__JDyPTfMn(113, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0114_fasterq_short__JDyPTfMn(114, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0115_fasterq_short__JDyPTfMn(115, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0116_fasterq_short__JDyPTfMn(116, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0117_fasterq_short__JDyPTfMn(117, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0118_fasterq_short__JDyPTfMn(118, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0119_fasterq_short__JDyPTfMn(119, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0120_fasterq_short__JDyPTfMn(120, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0121_fasterq_short__JDyPTfMn(121, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0122_fasterq_short__JDyPTfMn(122, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0123_fasterq_short__JDyPTfMn(123, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0124_fasterq_short__JDyPTfMn(124, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0125_fasterq_short__JDyPTfMn(125, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0126_fasterq_short__JDyPTfMn(126, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0127_fasterq_short__JDyPTfMn(127, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0128_fasterq_short__JDyPTfMn(128, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0129_fasterq_short__JDyPTfMn(129, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0130_fasterq_short__JDyPTfMn(130, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0131_fasterq_short__JDyPTfMn(131, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0132_fasterq_short__JDyPTfMn(132, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0133_fasterq_short__JDyPTfMn(133, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0134_fasterq_short__JDyPTfMn(134, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0135_fasterq_short__JDyPTfMn(135, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0136_fasterq_short__JDyPTfMn(136, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0137_fasterq_short__JDyPTfMn(137, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0138_fasterq_short__JDyPTfMn(138, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0139_fasterq_short__JDyPTfMn(139, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0140_fasterq_short__JDyPTfMn(140, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0141_fasterq_short__JDyPTfMn(141, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0142_fasterq_short__JDyPTfMn(142, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0143_fasterq_short__JDyPTfMn(143, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0144_fasterq_short__JDyPTfMn(144, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0145_fasterq_short__JDyPTfMn(145, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0146_fasterq_short__JDyPTfMn(146, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0147_fasterq_short__JDyPTfMn(147, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0148_fasterq_short__JDyPTfMn(148, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0149_fasterq_short__JDyPTfMn(149, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0150_fasterq_short__JDyPTfMn(150, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0151_fasterq_short__JDyPTfMn(151, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0152_fasterq_short__JDyPTfMn(152, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0153_fasterq_short__JDyPTfMn(153, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0154_fasterq_short__JDyPTfMn(154, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0155_fasterq_short__JDyPTfMn(155, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0156_fasterq_short__JDyPTfMn(156, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0157_fasterq_short__JDyPTfMn(157, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0158_fasterq_short__JDyPTfMn(158, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0159_fasterq_long__5U3XB88a(159, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0160_fasterq_long__5U3XB88a(160, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0161_fasterq_long__5U3XB88a(161, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0162_fasterq_long__5U3XB88a(162, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0163_fasterq_short__JDyPTfMn(163, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0164_fasterq_short__JDyPTfMn(164, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0165_fasterq_long__5U3XB88a(165, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0166_fasterq_short__JDyPTfMn(166, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0167_fasterq_long__5U3XB88a(167, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0168_fasterq_short__JDyPTfMn(168, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0169_fasterq_short__JDyPTfMn(169, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0170_fasterq_long__5U3XB88a(170, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0171_fasterq_short__JDyPTfMn(171, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0172_fasterq_short__JDyPTfMn(172, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0173_fasterq_short__JDyPTfMn(173, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0174_fasterq_short__JDyPTfMn(174, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0175_fasterq_short__JDyPTfMn(175, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0176_fasterq_short__JDyPTfMn(176, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0177_fasterq_short__JDyPTfMn(177, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0178_fasterq_long__5U3XB88a(178, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0179_fasterq_short__JDyPTfMn(179, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0180_fasterq_short__JDyPTfMn(180, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0181_fasterq_short__JDyPTfMn(181, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0182_fasterq_short__JDyPTfMn(182, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0183_fasterq_short__JDyPTfMn(183, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0184_fasterq_short__JDyPTfMn(184, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0185_fasterq_short__JDyPTfMn(185, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0186_fasterq_short__JDyPTfMn(186, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0187_fasterq_short__JDyPTfMn(187, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0188_fasterq_long__5U3XB88a(188, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0189_fasterq_short__JDyPTfMn(189, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0190_fasterq_short__JDyPTfMn(190, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0191_fasterq_short__JDyPTfMn(191, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0192_fasterq_short__JDyPTfMn(192, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0193_fasterq_long__5U3XB88a(193, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0194_fasterq_short__JDyPTfMn(194, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0195_fasterq_short__JDyPTfMn(195, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0196_fasterq_short__JDyPTfMn(196, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0197_fasterq_short__JDyPTfMn(197, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0198_fasterq_short__JDyPTfMn(198, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0199_fasterq_short__JDyPTfMn(199, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0200_fasterq_long__5U3XB88a(200, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0201_fasterq_short__JDyPTfMn(201, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0202_fasterq_short__JDyPTfMn(202, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0203_fasterq_short__JDyPTfMn(203, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0204_fasterq_short__JDyPTfMn(204, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0205_fasterq_short__JDyPTfMn(205, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0206_fasterq_long__5U3XB88a(206, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0207_fasterq_short__JDyPTfMn(207, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0208_fasterq_short__JDyPTfMn(208, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0209_fasterq_short__JDyPTfMn(209, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0210_fasterq_short__JDyPTfMn(210, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0211_fasterq_short__JDyPTfMn(211, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0212_fasterq_short__JDyPTfMn(212, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0213_fasterq_short__JDyPTfMn(213, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0214_fasterq_short__JDyPTfMn(214, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0215_fasterq_short__JDyPTfMn(215, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0216_fasterq_short__JDyPTfMn(216, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0217_fasterq_short__JDyPTfMn(217, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0218_fasterq_short__JDyPTfMn(218, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0219_fasterq_short__JDyPTfMn(219, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0220_fasterq_short__JDyPTfMn(220, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0221_fasterq_short__JDyPTfMn(221, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0222_fasterq_long__5U3XB88a(222, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0223_fasterq_short__JDyPTfMn(223, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0224_fasterq_short__JDyPTfMn(224, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0225_fasterq_short__JDyPTfMn(225, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0226_fasterq_short__JDyPTfMn(226, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0227_fasterq_short__JDyPTfMn(227, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0228_fasterq_short__JDyPTfMn(228, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0229_fasterq_short__JDyPTfMn(229, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0230_fasterq_short__JDyPTfMn(230, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0231_fasterq_short__JDyPTfMn(231, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0232_fasterq_long__5U3XB88a(232, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0233_fasterq_short__JDyPTfMn(233, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0234_fasterq_short__JDyPTfMn(234, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0235_fasterq_long__5U3XB88a(235, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0236_fasterq_short__JDyPTfMn(236, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0237_fasterq_long__5U3XB88a(237, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0238_fasterq_short__JDyPTfMn(238, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0239_fasterq_long__5U3XB88a(239, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0240_fasterq_short__JDyPTfMn(240, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0241_fasterq_short__JDyPTfMn(241, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0242_fasterq_long__5U3XB88a(242, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0243_fasterq_short__JDyPTfMn(243, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0244_fasterq_long__5U3XB88a(244, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0245_fasterq_long__5U3XB88a(245, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0246_fasterq_long__5U3XB88a(246, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0247_fasterq_short__JDyPTfMn(247, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0248_fasterq_short__JDyPTfMn(248, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0249_fasterq_short__JDyPTfMn(249, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0250_fasterq_short__JDyPTfMn(250, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0251_fasterq_short__JDyPTfMn(251, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0252_fasterq_short__JDyPTfMn(252, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0253_fasterq_short__JDyPTfMn(253, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0254_fasterq_short__JDyPTfMn(254, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0255_fasterq_short__JDyPTfMn(255, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0256_fasterq_short__JDyPTfMn(256, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0257_fasterq_short__JDyPTfMn(257, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0258_fasterq_short__JDyPTfMn(258, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0259_fasterq_short__JDyPTfMn(259, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0260_fasterq_short__JDyPTfMn(260, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0261_fasterq_short__JDyPTfMn(261, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0262_fasterq_short__JDyPTfMn(262, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0263_fasterq_short__JDyPTfMn(263, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0264_fasterq_short__JDyPTfMn(264, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0265_fasterq_short__JDyPTfMn(265, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0266_fasterq_short__JDyPTfMn(266, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0267_fasterq_short__JDyPTfMn(267, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0268_fasterq_short__JDyPTfMn(268, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0269_fasterq_short__JDyPTfMn(269, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0270_fasterq_short__JDyPTfMn(270, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0271_fasterq_short__JDyPTfMn(271, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0272_fasterq_short__JDyPTfMn(272, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0273_fasterq_short__JDyPTfMn(273, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0274_fasterq_short__JDyPTfMn(274, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0275_fasterq_long__5U3XB88a(275, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0276_fasterq_long__5U3XB88a(276, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0277_fasterq_long__5U3XB88a(277, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0278_fasterq_short__JDyPTfMn(278, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0279_fasterq_short__JDyPTfMn(279, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0280_fasterq_short__JDyPTfMn(280, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0281_fasterq_short__JDyPTfMn(281, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0282_fasterq_short__JDyPTfMn(282, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0283_fasterq_short__JDyPTfMn(283, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0284_fasterq_short__JDyPTfMn(284, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0285_fasterq_short__JDyPTfMn(285, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0286_fasterq_short__JDyPTfMn(286, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0287_fasterq_short__JDyPTfMn(287, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0288_fasterq_long__5U3XB88a(288, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0289_fasterq_long__5U3XB88a(289, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0290_fasterq_long__5U3XB88a(290, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0291_fasterq_long__5U3XB88a(291, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0292_fasterq_short__JDyPTfMn(292, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0293_fasterq_short__JDyPTfMn(293, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0294_fasterq_short__JDyPTfMn(294, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0295_fasterq_short__JDyPTfMn(295, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0296_fasterq_short__JDyPTfMn(296, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0297_fasterq_short__JDyPTfMn(297, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0298_fasterq_short__JDyPTfMn(298, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0299_fasterq_short__JDyPTfMn(299, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0300_fasterq_short__JDyPTfMn(300, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0301_fasterq_long__5U3XB88a(301, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0302_fasterq_long__5U3XB88a(302, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0303_fasterq_short__JDyPTfMn(303, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0304_fasterq_short__JDyPTfMn(304, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0305_fasterq_short__JDyPTfMn(305, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0306_fasterq_short__JDyPTfMn(306, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0307_fasterq_short__JDyPTfMn(307, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0308_fasterq_short__JDyPTfMn(308, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0309_fasterq_short__JDyPTfMn(309, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0310_fasterq_short__JDyPTfMn(310, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0311_fasterq_short__JDyPTfMn(311, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0312_fasterq_long__5U3XB88a(312, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0313_fasterq_short__JDyPTfMn(313, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0314_fasterq_short__JDyPTfMn(314, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0315_fasterq_long__5U3XB88a(315, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0316_fasterq_short__JDyPTfMn(316, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0317_fasterq_short__JDyPTfMn(317, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0318_fasterq_short__JDyPTfMn(318, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0319_fasterq_long__5U3XB88a(319, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0320_fasterq_long__5U3XB88a(320, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0321_fasterq_short__JDyPTfMn(321, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0322_fasterq_long__5U3XB88a(322, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0323_fasterq_short__JDyPTfMn(323, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0324_fasterq_long__5U3XB88a(324, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0325_fasterq_short__JDyPTfMn(325, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0326_fasterq_long__5U3XB88a(326, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0327_fasterq_short__JDyPTfMn(327, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0328_fasterq_long__5U3XB88a(328, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0329_fasterq_long__5U3XB88a(329, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0330_fasterq_long__5U3XB88a(330, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0331_fasterq_long__5U3XB88a(331, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0332_fasterq_long__5U3XB88a(332, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0333_fasterq_long__5U3XB88a(333, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0334_fasterq_long__5U3XB88a(334, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0335_fasterq_long__5U3XB88a(335, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0336_fasterq_long__5U3XB88a(336, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0337_fasterq_long__5U3XB88a(337, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0338_fasterq_long__5U3XB88a(338, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0339_fasterq_long__5U3XB88a(339, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0340_fasterq_long__5U3XB88a(340, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0341_fasterq_long__5U3XB88a(341, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0342_fasterq_long__5U3XB88a(342, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0343_fasterq_long__5U3XB88a(343, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0344_fasterq_long__5U3XB88a(344, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0345_fasterq_short__JDyPTfMn(345, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0346_fasterq_short__JDyPTfMn(346, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0347_fasterq_short__JDyPTfMn(347, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0348_fasterq_short__JDyPTfMn(348, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0349_fasterq_short__JDyPTfMn(349, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0350_fasterq_short__JDyPTfMn(350, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0351_fasterq_short__JDyPTfMn(351, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0352_fasterq_short__JDyPTfMn(352, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0353_fasterq_short__JDyPTfMn(353, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0354_fasterq_long__5U3XB88a(354, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0355_fasterq_short__JDyPTfMn(355, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0356_fasterq_short__JDyPTfMn(356, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0357_fasterq_short__JDyPTfMn(357, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0358_fasterq_short__JDyPTfMn(358, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0359_fasterq_short__JDyPTfMn(359, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0360_fasterq_short__JDyPTfMn(360, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0361_fasterq_long__5U3XB88a(361, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0362_fasterq_short__JDyPTfMn(362, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0363_fasterq_short__JDyPTfMn(363, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0364_fasterq_short__JDyPTfMn(364, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0365_fasterq_short__JDyPTfMn(365, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0366_fasterq_short__JDyPTfMn(366, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0367_fasterq_short__JDyPTfMn(367, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0368_fasterq_short__JDyPTfMn(368, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0369_fasterq_long__5U3XB88a(369, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0370_fasterq_long__5U3XB88a(370, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0371_fasterq_short__JDyPTfMn(371, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0372_fasterq_long__5U3XB88a(372, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0373_fasterq_short__JDyPTfMn(373, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0374_fasterq_long__5U3XB88a(374, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0375_fasterq_short__JDyPTfMn(375, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0376_fasterq_long__5U3XB88a(376, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0377_fasterq_short__JDyPTfMn(377, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0378_fasterq_short__JDyPTfMn(378, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0379_fasterq_short__JDyPTfMn(379, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0380_fasterq_long__5U3XB88a(380, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0381_fasterq_short__JDyPTfMn(381, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0382_fasterq_long__5U3XB88a(382, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0383_fasterq_short__JDyPTfMn(383, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0384_fasterq_long__5U3XB88a(384, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0385_fasterq_short__JDyPTfMn(385, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0386_fasterq_long__5U3XB88a(386, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0387_fasterq_short__JDyPTfMn(387, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0388_fasterq_short__JDyPTfMn(388, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0389_fasterq_long__5U3XB88a(389, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0390_fasterq_long__5U3XB88a(390, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0391_fasterq_short__JDyPTfMn(391, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0392_fasterq_long__5U3XB88a(392, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0393_fasterq_short__JDyPTfMn(393, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0394_fasterq_long__5U3XB88a(394, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0395_fasterq_short__JDyPTfMn(395, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0396_fasterq_long__5U3XB88a(396, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0397_fasterq_short__JDyPTfMn(397, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0398_fasterq_short__JDyPTfMn(398, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0399_fasterq_long__5U3XB88a(399, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0400_fasterq_short__JDyPTfMn(400, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0401_fasterq_short__JDyPTfMn(401, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0402_fasterq_long__5U3XB88a(402, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0403_fasterq_short__JDyPTfMn(403, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0404_fasterq_long__5U3XB88a(404, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0405_fasterq_short__JDyPTfMn(405, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0406_fasterq_long__5U3XB88a(406, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0407_fasterq_short__JDyPTfMn(407, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0408_fasterq_long__5U3XB88a(408, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0409_fasterq_short__JDyPTfMn(409, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0410_fasterq_long__5U3XB88a(410, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0411_fasterq_long__5U3XB88a(411, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0412_fasterq_short__JDyPTfMn(412, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0413_fasterq_short__JDyPTfMn(413, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0414_fasterq_short__JDyPTfMn(414, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0415_fasterq_long__5U3XB88a(415, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0416_fasterq_short__JDyPTfMn(416, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0417_fasterq_short__JDyPTfMn(417, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0418_fasterq_long__5U3XB88a(418, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0419_fasterq_short__JDyPTfMn(419, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0420_fasterq_short__JDyPTfMn(420, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0421_fasterq_short__JDyPTfMn(421, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0422_fasterq_short__JDyPTfMn(422, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0423_fasterq_long__5U3XB88a(423, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0424_fasterq_short__JDyPTfMn(424, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0425_fasterq_short__JDyPTfMn(425, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0426_fasterq_long__5U3XB88a(426, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0427_fasterq_short__JDyPTfMn(427, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0428_fasterq_long__5U3XB88a(428, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0429_fasterq_short__JDyPTfMn(429, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0430_fasterq_long__5U3XB88a(430, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0431_fasterq_short__JDyPTfMn(431, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0432_fasterq_long__5U3XB88a(432, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0433_fasterq_long__5U3XB88a(433, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0434_fasterq_short__JDyPTfMn(434, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0435_fasterq_long__5U3XB88a(435, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0436_fasterq_long__5U3XB88a(436, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0437_fasterq_short__JDyPTfMn(437, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0438_fasterq_long__5U3XB88a(438, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0439_fasterq_short__JDyPTfMn(439, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0440_fasterq_long__5U3XB88a(440, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0441_fasterq_short__JDyPTfMn(441, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0442_fasterq_short__JDyPTfMn(442, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0443_fasterq_short__JDyPTfMn(443, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0444_fasterq_long__5U3XB88a(444, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0445_fasterq_short__JDyPTfMn(445, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0446_fasterq_short__JDyPTfMn(446, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0447_fasterq_long__5U3XB88a(447, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0448_fasterq_short__JDyPTfMn(448, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0449_fasterq_long__5U3XB88a(449, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0450_fasterq_short__JDyPTfMn(450, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0451_fasterq_short__JDyPTfMn(451, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0452_fasterq_short__JDyPTfMn(452, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0453_fasterq_short__JDyPTfMn(453, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0454_fasterq_long__5U3XB88a(454, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0455_fasterq_short__JDyPTfMn(455, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0456_fasterq_long__5U3XB88a(456, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0457_fasterq_short__JDyPTfMn(457, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0458_fasterq_long__5U3XB88a(458, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0459_fasterq_short__JDyPTfMn(459, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0460_fasterq_short__JDyPTfMn(460, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0461_fasterq_long__5U3XB88a(461, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0462_fasterq_short__JDyPTfMn(462, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0463_fasterq_long__5U3XB88a(463, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0464_fasterq_short__JDyPTfMn(464, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0465_fasterq_short__JDyPTfMn(465, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0466_fasterq_long__5U3XB88a(466, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0467_fasterq_short__JDyPTfMn(467, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0468_fasterq_short__JDyPTfMn(468, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0469_fasterq_long__5U3XB88a(469, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0470_fasterq_short__JDyPTfMn(470, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0471_fasterq_long__5U3XB88a(471, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0472_fasterq_short__JDyPTfMn(472, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0473_fasterq_long__5U3XB88a(473, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0474_fasterq_short__JDyPTfMn(474, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0475_fasterq_long__5U3XB88a(475, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0476_fasterq_short__JDyPTfMn(476, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0477_fasterq_long__5U3XB88a(477, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0478_fasterq_short__JDyPTfMn(478, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0479_fasterq_short__JDyPTfMn(479, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0480_fasterq_short__JDyPTfMn(480, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0481_fasterq_long__5U3XB88a(481, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0482_fasterq_short__JDyPTfMn(482, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0483_fasterq_long__5U3XB88a(483, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0484_fasterq_short__JDyPTfMn(484, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0485_fasterq_long__5U3XB88a(485, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0486_fasterq_short__JDyPTfMn(486, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0487_fasterq_long__5U3XB88a(487, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0488_fasterq_short__JDyPTfMn(488, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0489_fasterq_long__5U3XB88a(489, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0490_fasterq_long__5U3XB88a(490, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0491_fasterq_short__JDyPTfMn(491, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0492_fasterq_long__5U3XB88a(492, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0493_fasterq_short__JDyPTfMn(493, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0494_fasterq_short__JDyPTfMn(494, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0495_fasterq_long__5U3XB88a(495, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0496_fasterq_short__JDyPTfMn(496, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0497_fasterq_long__5U3XB88a(497, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0498_fasterq_short__JDyPTfMn(498, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0499_fasterq_long__5U3XB88a(499, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0500_fasterq_long__5U3XB88a(500, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0501_fasterq_short__JDyPTfMn(501, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0502_fasterq_long__5U3XB88a(502, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0503_fasterq_short__JDyPTfMn(503, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0504_fasterq_long__5U3XB88a(504, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0505_fasterq_short__JDyPTfMn(505, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0506_fasterq_long__5U3XB88a(506, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0507_fasterq_short__JDyPTfMn(507, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0508_fasterq_long__5U3XB88a(508, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0509_fasterq_short__JDyPTfMn(509, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0510_fasterq_short__JDyPTfMn(510, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0511_fasterq_short__JDyPTfMn(511, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0512_fasterq_short__JDyPTfMn(512, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0513_fasterq_long__5U3XB88a(513, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0514_fasterq_short__JDyPTfMn(514, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0515_fasterq_long__5U3XB88a(515, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0516_fasterq_short__JDyPTfMn(516, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0517_fasterq_long__5U3XB88a(517, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0518_fasterq_short__JDyPTfMn(518, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0519_fasterq_long__5U3XB88a(519, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0520_fasterq_short__JDyPTfMn(520, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0521_fasterq_short__JDyPTfMn(521, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0522_fasterq_long__5U3XB88a(522, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0523_fasterq_short__JDyPTfMn(523, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0524_fasterq_long__5U3XB88a(524, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0525_fasterq_short__JDyPTfMn(525, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0526_fasterq_long__5U3XB88a(526, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0527_fasterq_short__JDyPTfMn(527, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0528_fasterq_long__5U3XB88a(528, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0529_fasterq_short__JDyPTfMn(529, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0530_fasterq_long__5U3XB88a(530, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0531_fasterq_short__JDyPTfMn(531, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0532_fasterq_long__5U3XB88a(532, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0533_fasterq_long__5U3XB88a(533, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0534_fasterq_short__JDyPTfMn(534, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0535_fasterq_long__5U3XB88a(535, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0536_fasterq_short__JDyPTfMn(536, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0537_fasterq_long__5U3XB88a(537, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0538_fasterq_short__JDyPTfMn(538, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0539_fasterq_long__5U3XB88a(539, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0540_fasterq_short__JDyPTfMn(540, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0541_fasterq_long__5U3XB88a(541, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0542_fasterq_short__JDyPTfMn(542, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0543_fasterq_short__JDyPTfMn(543, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0544_fasterq_short__JDyPTfMn(544, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0545_fasterq_long__5U3XB88a(545, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0546_fasterq_short__JDyPTfMn(546, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0547_fasterq_short__JDyPTfMn(547, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0548_fasterq_long__5U3XB88a(548, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0549_fasterq_short__JDyPTfMn(549, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0550_fasterq_long__5U3XB88a(550, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0551_fasterq_short__JDyPTfMn(551, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0552_fasterq_long__5U3XB88a(552, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0553_fasterq_short__JDyPTfMn(553, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0554_fasterq_long__5U3XB88a(554, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0555_fasterq_long__5U3XB88a(555, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0556_fasterq_short__JDyPTfMn(556, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0557_fasterq_short__JDyPTfMn(557, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0558_fasterq_short__JDyPTfMn(558, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0559_fasterq_short__JDyPTfMn(559, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0560_fasterq_short__JDyPTfMn(560, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0561_fasterq_short__JDyPTfMn(561, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0562_fasterq_short__JDyPTfMn(562, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0563_fasterq_long__5U3XB88a(563, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0564_fasterq_short__JDyPTfMn(564, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0565_fasterq_long__5U3XB88a(565, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0566_fasterq_long__5U3XB88a(566, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0567_fasterq_long__5U3XB88a(567, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0568_fasterq_long__5U3XB88a(568, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0569_fasterq_short__JDyPTfMn(569, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0570_fasterq_short__JDyPTfMn(570, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0571_fasterq_long__5U3XB88a(571, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0572_fasterq_long__5U3XB88a(572, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0573_fasterq_long__5U3XB88a(573, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0574_fasterq_short__JDyPTfMn(574, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0575_fasterq_long__5U3XB88a(575, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0576_fasterq_long__5U3XB88a(576, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0577_fasterq_short__JDyPTfMn(577, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0578_fasterq_long__5U3XB88a(578, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0579_fasterq_long__5U3XB88a(579, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0580_fasterq_long__5U3XB88a(580, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0581_fasterq_long__5U3XB88a(581, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0582_fasterq_long__5U3XB88a(582, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0583_fasterq_long__5U3XB88a(583, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0584_fasterq_long__5U3XB88a(584, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0585_fasterq_long__5U3XB88a(585, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0586_fasterq_long__5U3XB88a(586, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0587_fasterq_long__5U3XB88a(587, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0588_fasterq_long__5U3XB88a(588, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0589_fasterq_long__5U3XB88a(589, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0590_fasterq_long__5U3XB88a(590, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0591_fasterq_long__5U3XB88a(591, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0592_fasterq_long__5U3XB88a(592, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0593_fasterq_long__5U3XB88a(593, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0594_fasterq_long__5U3XB88a(594, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0595_fasterq_long__5U3XB88a(595, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0596_fasterq_long__5U3XB88a(596, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0597_fasterq_long__5U3XB88a(597, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0598_fasterq_long__5U3XB88a(598, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0599_fasterq_long__5U3XB88a(599, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0600_fasterq_long__5U3XB88a(600, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0601_fasterq_long__5U3XB88a(601, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0602_fasterq_long__5U3XB88a(602, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0603_fasterq_long__5U3XB88a(603, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0604_fasterq_long__5U3XB88a(604, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0605_fasterq_long__5U3XB88a(605, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0606_fasterq_long__5U3XB88a(606, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0607_fasterq_long__5U3XB88a(607, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0608_fasterq_long__5U3XB88a(608, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0609_fasterq_long__5U3XB88a(609, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0610_fasterq_long__5U3XB88a(610, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0611_fasterq_long__5U3XB88a(611, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0612_fasterq_long__5U3XB88a(612, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0613_fasterq_long__5U3XB88a(613, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0614_fasterq_long__5U3XB88a(614, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0615_fasterq_long__5U3XB88a(615, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0616_fasterq_long__5U3XB88a(616, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0617_fasterq_long__5U3XB88a(617, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0618_fasterq_long__5U3XB88a(618, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0619_fasterq_long__5U3XB88a(619, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0620_fasterq_long__5U3XB88a(620, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0621_fasterq_long__5U3XB88a(621, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0622_fasterq_long__5U3XB88a(622, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0623_fasterq_long__5U3XB88a(623, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0624_fasterq_long__5U3XB88a(624, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0625_fasterq_long__5U3XB88a(625, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0626_fasterq_long__5U3XB88a(626, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0627_fasterq_long__5U3XB88a(627, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0628_fasterq_long__5U3XB88a(628, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0629_fasterq_long__5U3XB88a(629, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0630_fasterq_long__5U3XB88a(630, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0631_fasterq_long__5U3XB88a(631, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0632_fasterq_long__5U3XB88a(632, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0633_fasterq_long__5U3XB88a(633, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0634_fasterq_long__5U3XB88a(634, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0635_fasterq_long__5U3XB88a(635, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0636_fasterq_long__5U3XB88a(636, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0637_fasterq_long__5U3XB88a(637, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0638_fasterq_long__5U3XB88a(638, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0639_fasterq_long__5U3XB88a(639, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0640_fasterq_long__5U3XB88a(640, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0641_fasterq_short__JDyPTfMn(641, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0642_fasterq_short__JDyPTfMn(642, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0643_fasterq_short__JDyPTfMn(643, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0644_fasterq_short__JDyPTfMn(644, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0645_fasterq_short__JDyPTfMn(645, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0646_fasterq_short__JDyPTfMn(646, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0647_fasterq_long__5U3XB88a(647, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0648_fasterq_long__5U3XB88a(648, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0649_fasterq_long__5U3XB88a(649, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0650_fasterq_long__5U3XB88a(650, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0651_fasterq_long__5U3XB88a(651, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0652_fasterq_long__5U3XB88a(652, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0653_fasterq_long__5U3XB88a(653, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0654_fasterq_short__JDyPTfMn(654, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0655_fasterq_short__JDyPTfMn(655, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0656_fasterq_short__JDyPTfMn(656, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0657_fasterq_long__5U3XB88a(657, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0658_fasterq_short__JDyPTfMn(658, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0659_fasterq_short__JDyPTfMn(659, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0660_fasterq_short__JDyPTfMn(660, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0661_fasterq_short__JDyPTfMn(661, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0662_fasterq_short__JDyPTfMn(662, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0663_fasterq_short__JDyPTfMn(663, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0664_fasterq_short__JDyPTfMn(664, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0665_fasterq_short__JDyPTfMn(665, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0666_fasterq_short__JDyPTfMn(666, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0667_fasterq_short__JDyPTfMn(667, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0668_fasterq_short__JDyPTfMn(668, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0669_fasterq_short__JDyPTfMn(669, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0670_fasterq_short__JDyPTfMn(670, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0671_fasterq_short__JDyPTfMn(671, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0672_fasterq_long__5U3XB88a(672, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0673_fasterq_long__5U3XB88a(673, _4HMb09gR, _L64ygWKM)
    _bJtTFS8I = s0674_fasterq_long__5U3XB88a(674, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0675_fasterq_short__JDyPTfMn(675, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0676_fasterq_short__JDyPTfMn(676, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0677_fasterq_short__JDyPTfMn(677, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0678_fasterq_short__JDyPTfMn(678, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0679_fasterq_short__JDyPTfMn(679, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0680_fasterq_short__JDyPTfMn(680, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0681_fasterq_short__JDyPTfMn(681, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0682_fasterq_short__JDyPTfMn(682, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0683_fasterq_short__JDyPTfMn(683, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0684_fasterq_short__JDyPTfMn(684, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0685_fasterq_short__JDyPTfMn(685, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0686_fasterq_short__JDyPTfMn(686, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0687_fasterq_short__JDyPTfMn(687, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0688_fasterq_short__JDyPTfMn(688, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0689_fasterq_short__JDyPTfMn(689, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0690_fasterq_short__JDyPTfMn(690, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0691_fasterq_short__JDyPTfMn(691, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0692_fasterq_short__JDyPTfMn(692, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0693_fasterq_short__JDyPTfMn(693, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0694_fasterq_short__JDyPTfMn(694, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0695_fasterq_short__JDyPTfMn(695, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0696_fasterq_short__JDyPTfMn(696, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0697_fasterq_short__JDyPTfMn(697, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0698_fasterq_short__JDyPTfMn(698, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0699_fasterq_short__JDyPTfMn(699, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0700_fasterq_short__JDyPTfMn(700, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0701_fasterq_short__JDyPTfMn(701, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0702_fasterq_short__JDyPTfMn(702, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0703_fasterq_short__JDyPTfMn(703, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0704_fasterq_short__JDyPTfMn(704, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0705_fasterq_short__JDyPTfMn(705, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0706_fasterq_short__JDyPTfMn(706, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0707_fasterq_short__JDyPTfMn(707, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0708_fasterq_short__JDyPTfMn(708, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0709_fasterq_short__JDyPTfMn(709, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0710_fasterq_short__JDyPTfMn(710, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0711_fasterq_short__JDyPTfMn(711, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0712_fasterq_short__JDyPTfMn(712, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0713_fasterq_short__JDyPTfMn(713, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0714_fasterq_short__JDyPTfMn(714, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0715_fasterq_short__JDyPTfMn(715, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0716_fasterq_short__JDyPTfMn(716, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0717_fasterq_short__JDyPTfMn(717, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0718_fasterq_short__JDyPTfMn(718, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0719_fasterq_short__JDyPTfMn(719, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0720_fasterq_short__JDyPTfMn(720, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0721_fasterq_short__JDyPTfMn(721, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0722_fasterq_short__JDyPTfMn(722, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0723_fasterq_short__JDyPTfMn(723, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0724_fasterq_short__JDyPTfMn(724, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0725_fasterq_short__JDyPTfMn(725, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0726_fasterq_short__JDyPTfMn(726, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0727_fasterq_short__JDyPTfMn(727, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0728_fasterq_short__JDyPTfMn(728, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0729_fasterq_short__JDyPTfMn(729, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0730_fasterq_short__JDyPTfMn(730, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0731_fasterq_short__JDyPTfMn(731, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0732_fasterq_short__JDyPTfMn(732, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0733_fasterq_short__JDyPTfMn(733, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0734_fasterq_short__JDyPTfMn(734, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0735_fasterq_short__JDyPTfMn(735, _4pOv1Zyo, _L64ygWKM)
    _bJtTFS8I = s0736_fasterq_long__5U3XB88a(736, _4HMb09gR, _L64ygWKM)
    _eWB2yuLt = s0737_fasterq_short__JDyPTfMn(737, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0738_fasterq_short__JDyPTfMn(738, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0739_fasterq_short__JDyPTfMn(739, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0740_fasterq_short__JDyPTfMn(740, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0741_fasterq_short__JDyPTfMn(741, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0742_fasterq_short__JDyPTfMn(742, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0743_fasterq_short__JDyPTfMn(743, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0744_fasterq_short__JDyPTfMn(744, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0745_fasterq_short__JDyPTfMn(745, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0746_fasterq_short__JDyPTfMn(746, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0747_fasterq_short__JDyPTfMn(747, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0748_fasterq_short__JDyPTfMn(748, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0749_fasterq_short__JDyPTfMn(749, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0750_fasterq_short__JDyPTfMn(750, _4pOv1Zyo, _L64ygWKM)
    _eWB2yuLt = s0751_fasterq_short__JDyPTfMn(751, _4pOv1Zyo, _L64ygWKM)
}
