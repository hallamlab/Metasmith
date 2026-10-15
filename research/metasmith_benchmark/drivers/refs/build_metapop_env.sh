#!/bin/bash
# Build MetaPop 0.0.60's environment (the Python pipeline Pratama ran; bioconda's metapop 1.0.2 is the
# older R one) and tar it to /scratch as one file. Run on a login node: it downloads.
#   bash <checkout>/research/metasmith_benchmark/drivers/refs/build_metapop_env.sh
# Then pack the tar into an image with build_metapop_env_image.sbatch.
# CAUTION a conda prefix hardcodes its path in shebangs and in R's home, so the env is built inside the
# base container at /opt/metapop, the path the bench MetaPop transform binds the image at.
set -euo pipefail
module load apptainer
OUT=/scratch/phyberos/refs/_metapop_0.0.60_env.tar
BASE=/scratch/phyberos/cache/apptainer/docker..quay.io_biocontainers_metapop..1.0.2--hdfd78af_1.sif
[ ! -e "$OUT" ] || { echo "$OUT already exists"; exit 1; }
W=$(mktemp -d /tmp/metapop_env.XXXX)
trap 'rm -rf "$W"' EXIT
mkdir -p "$W/bin" "$W/opt" "$W/root"
curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/2.0.5 | tar -xj -C "$W" bin/micromamba

# micromamba refuses an existing non-conda directory as the prefix, so /opt is the bind and the prefix
# directory is created inside it.
in_base() {
  apptainer exec --cleanenv --bind "$W/opt:/opt" --bind "$W:/build" --bind /etc/pki \
    --env MAMBA_ROOT_PREFIX=/build/root,SSL_CERT_FILE=/etc/pki/tls/certs/ca-bundle.crt,PATH=/opt/metapop/bin:/usr/local/bin:/usr/bin:/bin \
    "$BASE" sh -c "$1"
}
in_base '/build/bin/micromamba create -y -p /opt/metapop -c conda-forge -c bioconda --strict-channel-priority \
  python=3.10 pip numpy pysam samtools bcftools prodigal bowtie2 \
  r-base r-data.table r-ggplot2 r-ggrepel r-rcolorbrewer r-doparallel r-cowplot r-bit64 r-gggenes r-stringr r-vegan r-compositions r-pheatmap'
in_base 'pip install --no-deps --no-cache-dir metapop==0.0.60'
in_base 'metapop --help | head -3
  Rscript -e "for (p in c(\"doParallel\",\"data.table\",\"stringr\",\"ggplot2\",\"ggrepel\",\"cowplot\",\"vegan\",\"compositions\",\"pheatmap\",\"RColorBrewer\",\"bit64\",\"gggenes\")) stopifnot(requireNamespace(p, quietly=TRUE)); cat(\"R packages ok\n\")"
  samtools --version | head -1; bcftools --version | head -1; prodigal -v 2>&1 | tail -1
  /build/bin/micromamba list -p /opt/metapop > /opt/metapop/conda-list.txt'
grep -E "^\s*(python|pysam|numpy|samtools|bcftools|prodigal|bowtie2|r-base) " "$W/opt/metapop/conda-list.txt" || true
echo "prefix: $(du -sh "$W/opt/metapop" | cut -f1), $(find "$W/opt/metapop" | wc -l) entries"
tar -C "$W/opt" -cf "$OUT.tmp" metapop
mv "$OUT.tmp" "$OUT"
echo "ENV_TAR_OK $OUT $(stat -c %s "$OUT") bytes"
