#!/bin/sh
# UNVERIFIED against a real download. Runs as root inside the build, from a
# DeepTMHMM source archive (the /openprotein tree of the BioLib image: predict.py
# beside its model files). predict.py writes /deeptmhmm_results.md, which a
# read-only image refuses, so the `deeptmhmm` wrapper runs a copy from the
# working directory with that path made relative.
set -eu
mkdir -p /opt/deeptmhmm /tmp/deeptmhmm
tar -xf "$1" -C /tmp/deeptmhmm
src=$(dirname "$(find /tmp/deeptmhmm -name predict.py | head -1)")
cp -r "$src"/. /opt/deeptmhmm/
rm -rf /tmp/deeptmhmm
pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.1.2
[ -f /opt/deeptmhmm/requirements.txt ] && pip install --no-cache-dir "numpy<2" -r /opt/deeptmhmm/requirements.txt
cat > /usr/local/bin/deeptmhmm <<'WRAP'
#!/bin/sh
set -eu
for f in /opt/deeptmhmm/*; do [ -e "$(basename "$f")" ] || ln -s "$f" .; done
rm -f predict.py
sed "s|open('/deeptmhmm_results.md'|open('deeptmhmm_results.md'|" /opt/deeptmhmm/predict.py > predict.py
exec python3 predict.py "$@"
WRAP
chmod 755 /usr/local/bin/deeptmhmm
chmod -R a+rX /opt/deeptmhmm
