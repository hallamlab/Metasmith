
HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
sed '/^[^>]/s/\*//g' $HERE/../test_data/Ana_PS.faa > $HERE/../test_data/Ana_PS.no_star.faa

mkdir -p cache/interproscan_test
apptainer exec -B /home/tony/workspace/MetasmithLibraries/tests/test_data:/inputs,/home/tony/workspace/MetasmithLibraries/tests/test_data/interproscan_data/data:/opt/interproscan/data docker://interpro/interproscan:5.67-99.0 \
    /opt/interproscan/interproscan.sh \
        --disable-precalc \
        --verbose \
        --seqtype p \
        --cpu 6 \
        -i /inputs/Ana_PS.no_star.faa \
        -f json,gff3 \
        -d ./cache/interproscan_test
