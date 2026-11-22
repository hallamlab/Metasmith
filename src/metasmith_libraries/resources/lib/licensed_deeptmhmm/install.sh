#!/bin/sh
# Runs as root inside the build, from the DeepTMHMM academic zip
# (DeepTMHMM-Academic-License-v1.0.zip): predict.py beside pickled torch models
# and the ESM-1b weights. Its README pins Python 3.8 and torch 1.5.0, and the
# models are whole pickled modules that need that torch, so the base is
# python:3.8 and torch is the 1.5.0 CPU build in place of its cu92 pin.
# predict.py and utils.py open the weights relative to the working directory;
# they are patched to open them beside the script, so `deeptmhmm` runs anywhere.
set -eu
mkdir -p /opt/deeptmhmm /tmp/deeptmhmm
python3 -m zipfile -e "$1" /tmp/deeptmhmm
src=$(dirname "$(find /tmp/deeptmhmm -name predict.py -not -path '*__MACOSX*' | head -1)")
cp -r "$src"/. /opt/deeptmhmm/
rm -rf /tmp/deeptmhmm
cd /opt/deeptmhmm
here="os.path.dirname(os.path.abspath(__file__))"
sed -i "s#load_model_from_disk(f'{model_paths\[i\]}')#load_model_from_disk(os.path.join($here, model_paths[i]))#" predict.py
sed -i "s#torch\.load('\(esm_model_[a-z_]*\.pt\)')#torch.load(os.path.join($here, '\1'))#" utils.py
[ "$(cat predict.py utils.py | grep -cF "$here")" -eq 4 ] || { echo "DeepTMHMM weight paths not patched" >&2; exit 1; }
pip install --no-cache-dir -f https://download.pytorch.org/whl/torch_stable.html torch==1.5.0+cpu
grep -v '^torch==' requirements.txt > /tmp/requirements.txt
pip install --no-cache-dir -r /tmp/requirements.txt
rm /tmp/requirements.txt
printf '#!/bin/sh\nexec python3 /opt/deeptmhmm/predict.py "$@"\n' > /usr/local/bin/deeptmhmm
chmod 755 /usr/local/bin/deeptmhmm
chmod -R a+rX /opt/deeptmhmm
