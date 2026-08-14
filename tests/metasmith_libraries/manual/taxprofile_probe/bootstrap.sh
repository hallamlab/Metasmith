#!/usr/bin/env bash
# Bootstrap the sockeye taxprofile probe workspace:
#   - module-load apptainer
#   - pull biocontainer SIFs
#   - fetch tiny paired-end FASTQ from ENA (no sra-tools required)
#   - download tiny RefSeq genomes for sylph/ganon2/centrifuger mini-DB builds
#
# Run once on a login node (interactive_cpu allocation not needed — only
# network downloads + tiny CPU work).

set -euo pipefail

WORK=${TAXPROFILE_WORK:-/scratch/st-shallam-1/txyliu/taxprofile_probe}
mkdir -p "$WORK"/{containers,dbs,fixtures/tiny_refs,runs}
cd "$WORK"

module load gcc/9.4.0 apptainer/1.3.1 2>/dev/null || true
command -v apptainer >/dev/null || { echo "apptainer not on PATH after module load"; exit 1; }
apptainer --version

# ---------------------------------------------------------------
# Pull containers (biocontainers; pinned tags). Apptainer caches under
# ~/.apptainer/cache by default — overridable via APPTAINER_CACHEDIR.
# ---------------------------------------------------------------
pull() {
    local name="$1" uri="$2"
    local sif="containers/${name}.sif"
    if [[ -f "$sif" ]]; then
        echo "[skip] $name already pulled"
        return
    fi
    echo "[pull] $name <- $uri"
    apptainer pull "$sif" "$uri"
}

pull kraken2     docker://quay.io/biocontainers/kraken2:2.1.6--pl5321h077b44d_0
pull bracken     docker://quay.io/biocontainers/bracken:3.0--h9948957_2
pull centrifuger docker://quay.io/biocontainers/centrifuger:1.1.1--h3be2455_0
pull sylph       docker://quay.io/biocontainers/sylph:0.8.0--ha6fb395_0
pull ganon       docker://quay.io/biocontainers/ganon:2.4.1--py312h88d62d1_0
pull metaphlan   docker://quay.io/biocontainers/metaphlan:4.2.4--pyhdfd78af_0

# ---------------------------------------------------------------
# Tiny paired-end fixture from ENA HTTPS (no sra-tools needed).
# Default: SRR2584863 = E. coli K-12 MG1655 short-read MiSeq run, ~140 MB.
# Small enough to download in <2 min on sockeye's DTN nodes; we'll subsample
# to 5 000 read pairs after.
# Override with SRA_ACCESSION env var to use a community sample instead.
# ---------------------------------------------------------------
ACC=${SRA_ACCESSION:-SRR2584863}
TINY_R1=fixtures/tiny_pe_R1.fq.gz
TINY_R2=fixtures/tiny_pe_R2.fq.gz

if [[ ! -f "$TINY_R1" || ! -f "$TINY_R2" ]]; then
    # ENA layout: /vol1/fastq/<PREFIX6>/<SUBDIR>/<ACC>/<ACC>_{1,2}.fastq.gz
    # where SUBDIR is the last (LEN-6) digits of the numeric part, zero-padded
    # to 3 chars. For SRR2584863 (7-digit numeric), SUBDIR=003. For 6-digit
    # numeric, SUBDIR is omitted.
    NUM="${ACC#[SED]RR}"
    LEN=${#NUM}
    if (( LEN <= 6 )); then
        SUBDIR=""
    else
        EXTRA=$((LEN - 6))
        SUBDIR=$(printf "%03d" "$(( 10#${NUM: -$EXTRA} ))")
    fi
    PREFIX6="${ACC:0:6}"
    if [[ -n "$SUBDIR" ]]; then
        BASE="https://ftp.sra.ebi.ac.uk/vol1/fastq/${PREFIX6}/${SUBDIR}/${ACC}/${ACC}"
    else
        BASE="https://ftp.sra.ebi.ac.uk/vol1/fastq/${PREFIX6}/${ACC}/${ACC}"
    fi
    echo "[ena] fetching ${BASE}_1.fastq.gz and ${BASE}_2.fastq.gz"
    if [[ ! -f "fixtures/${ACC}_1.fastq.gz" ]]; then
        curl -fL "${BASE}_1.fastq.gz" -o "fixtures/${ACC}_1.fastq.gz" \
            || { echo "ENA download failed; try a different SRA_ACCESSION"; exit 1; }
    fi
    if [[ ! -f "fixtures/${ACC}_2.fastq.gz" ]]; then
        curl -fL "${BASE}_2.fastq.gz" -o "fixtures/${ACC}_2.fastq.gz"
    fi
    # Subsample to 5 000 pairs using awk (avoids needing seqtk on login node).
    # Same seed for both halves preserves pairing.
    subsample() {
        local in="$1" out="$2" n=5000
        zcat "$in" | awk -v n="$n" 'BEGIN{srand(42)} NR%4==1{r=rand()} r<n/2500000' | head -n "$((n*4))" | gzip -c > "$out"
    }
    # Simpler: just take first 5000 pairs (deterministic, same in both halves).
    # `head` closing early gives zcat SIGPIPE; disable pipefail around these.
    set +o pipefail
    zcat "fixtures/${ACC}_1.fastq.gz" | head -n 20000 | gzip -c > "$TINY_R1"
    zcat "fixtures/${ACC}_2.fastq.gz" | head -n 20000 | gzip -c > "$TINY_R2"
    set -o pipefail
    echo "[ok] tiny paired-end reads: $(zcat "$TINY_R1" | wc -l | awk '{print $1/4}') pairs"
fi

# ---------------------------------------------------------------
# Tiny reference genomes for sylph / centrifuger / ganon2 mini-DB builds.
# Use ENA/NCBI direct HTTPS (skip the datasets CLI install).
# 5 small bacterial genomes, ~25 MB total.
# ---------------------------------------------------------------
TINY_REFS_DIR=fixtures/tiny_refs
download_ref() {
    local name="$1" url="$2"
    local out="$TINY_REFS_DIR/${name}.fna"
    [[ -f "$out" || -f "${out}.gz" ]] && return
    echo "[ref] $name"
    curl -fL "$url" -o "${out}.gz"
    gunzip "${out}.gz"
}

# Each URL is an NCBI assembly genomic.fna.gz
download_ref ecoli_k12 \
  https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/005/845/GCF_000005845.2_ASM584v2/GCF_000005845.2_ASM584v2_genomic.fna.gz
download_ref saureus_n315 \
  https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/645/GCF_000009645.1_ASM964v1/GCF_000009645.1_ASM964v1_genomic.fna.gz
download_ref bsubtilis_168 \
  https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/045/GCF_000009045.1_ASM904v1/GCF_000009045.1_ASM904v1_genomic.fna.gz
download_ref ppputida_kt2440 \
  https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/007/565/GCF_000007565.2_ASM756v2/GCF_000007565.2_ASM756v2_genomic.fna.gz
download_ref lactobacillus_acidophilus \
  https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/011/985/GCF_000011985.1_ASM1198v1/GCF_000011985.1_ASM1198v1_genomic.fna.gz

echo "[done] bootstrap complete:"
echo "  containers: $(ls containers/*.sif 2>/dev/null | wc -l)/6"
echo "  reads:      $TINY_R1, $TINY_R2 ($(du -h $TINY_R1 | cut -f1) + $(du -h $TINY_R2 | cut -f1))"
echo "  tiny_refs:  $(ls $TINY_REFS_DIR/*.fna 2>/dev/null | wc -l) genomes"
echo
echo "Next: ./probe_<tool>.sh (each starts/reuses one salloc allocation)"
echo "Pre-built Kraken2 standard DB available at /scratch/st-shallam-1/k2_standard_16_GB_20251015 - probe_kraken2.sh uses it directly"
