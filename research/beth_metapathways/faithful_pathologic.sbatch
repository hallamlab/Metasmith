#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=beth_mp4_faithful
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/beth_mp/pffix/logs/faithful_%j.log
# Usage: sbatch faithful_pathologic.sbatch SAMPLE LANES_RUN_KEY CHECKOUT
#
# The PathoLogic half of the port on one assembly, outside the engine: the annotation table of the
# four MetaPathways 4 searches (the mp4lanes group's run), then ptools_annotation_gather,
# pathologic and pathologic_mag as their protocols run them, from CHECKOUT (a copy of this repository).
# The engine route reads an annotation table over every lane, which on
# these assemblies would re-run lanes the PathoLogic input never reads.
set -euo pipefail
S=$1 R=/scratch/phyberos/beth_mp/port/metasmith/runs/$2 L=$3
B=/scratch/phyberos/beth_mp
O=$B/pffix/faithful/$S
IMG=$B/port/metasmith/container_images
PY_SIF=$IMG/docker..quay.io_hallamlab_python_for_data_science..1.4.0.sif
PT_SIF=/scratch/phyberos/engcyc/licensed/pathologic.sif
MP4_SIF=$B/images/metapathways_4.0.3.sif
MSM_SIF=$IMG/docker..quay.io_hallamlab_metasmith..0.24.2.sif
LIB=$L/src/metasmith_libraries/resources/lib
LOCAL=${SLURM_TMPDIR:?run inside a Slurm allocation}
mkdir -p $O $LOCAL/ptools_home
py() { apptainer exec --cleanenv -B /scratch/phyberos $PY_SIF python3 "$@"; }
one() { ls $R/results/annotation-$1/* | head -1; }

orfs=$(ls $R/results/sequences-orfs/* | head -1)
lanes=()
for db in uniref50 uniref90 swissprot metacyc; do
    lanes+=(--lane $db "$(one diamond_${db}_results)" "$(one diamond_${db}_descriptions)")
done
py $LIB/fabfos_gpr/annotation_table.py --ev-lib $LIB/fabfos_evidence.py --orfs $orfs --source $S \
    "${lanes[@]}" --out $O/annotation_table.parquet
apptainer exec --cleanenv --env PYTHONPATH=$LIB -B /scratch/phyberos,/project/6004975/phyberos/licensed/pathway_tools_27.0 $PY_SIF python3 $LIB/pathologic/gather.py \
    --orfs $orfs --annotations $O/annotation_table.parquet --ec-maps /scratch/phyberos/databases/uniprot_2025_04/ec_maps \
    --metacyc-reactions /project/6004975/phyberos/licensed/pathway_tools_27.0/MetaCyc-monomer-rxn-pairs.tsv \
    --out $O/ptools_annotation_table.parquet

build() {  # NAME [BIN_FASTA]
    local name=$1 w=$O/pgdb/$1
    rm -rf $w && mkdir -p $w
    apptainer exec --cleanenv --env PYTHONPATH=$LIB -B /scratch/phyberos $PY_SIF python3 $LIB/pathologic/build_pf.py \
        --annotations $O/ptools_annotation_table.parquet --name $name ${2:+--bin-fasta $2} --out-dir $w/ptin
    mkdir -p $LOCAL/pathologic_$name
    (cd $w && apptainer exec --cleanenv --no-home --env TMPDIR=/lscratch/pathologic_$name \
        -B $LOCAL:/lscratch -B $LOCAL/ptools_home:$HOME -B /scratch/phyberos $PT_SIF bash $LIB/pathologic/run.sh ptin pgdb) \
        || { echo "PathoLogic failed for $name"; return 0; }
    (cd $w && apptainer exec --cleanenv -B /scratch/phyberos,$LOCAL $MP4_SIF python3 $LIB/pathologic/pathway_table.py \
        --pgdb pgdb --sample $name --out $w/pathway_table.tsv --work $LOCAL/camelot_$name)
}
build $S
awk -F'\t' -v s=$S 'NR>1 && $1==s {print $2, $3}' $L/research/beth_metapathways/port/mags.tsv | while read bin fasta; do
    build $bin $fasta
done

apptainer exec --cleanenv -B /scratch/phyberos $MSM_SIF python3 $B/pffix/scripts/export_pgdb.py --sample $S \
    --bins $B/mp4/mag_maps/$S.tsv --pipeline port --out-dir $O/pgdb_port $O/pgdb/*/pgdb
apptainer exec --cleanenv -B /scratch/phyberos $MSM_SIF python3 $B/pffix/scripts/pathway_recheck.py $S $O/pgdb_port \
    | tee $O/pathways.tsv
