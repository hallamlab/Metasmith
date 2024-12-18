HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PATH=$HERE/mock:$PATH
mkdir -p ./cache
cd ./cache
# nextflow -C ../main.cfg -log ./.nextflow_logs/log \
#     run ../main.nf -resume

# rm -rf .nextflow* work results
rm -r results
    # --bootstrap 
nextflow -C ../config.nf -log ./.nextflow_logs/log \
    run ../test.2.nf \
    --given_OVtA given/2beaver_fosmid_seqs.fna given/contigs.fna \
    --given_bAYL given/swissprot_fastal_ref \
    --account st-shallam-1
