#!/bin/sh
# Runs as root inside the build, from the DTU SignalP 6.0 archive the user
# downloaded (signalp-6.0*.fast.tar.gz or the slow_sequential one). Follows the
# package's own README: pip install the package directory, then copy its model
# weights into the installed module. torch comes from the CPU wheel index, and
# numpy stays below 2, which torch 2.1 was built against.
set -eu
mkdir -p /tmp/signalp
tar -xf "$1" -C /tmp/signalp
pkg=$(dirname "$(find /tmp/signalp -name setup.py -path '*signalp-6-package*' | head -1)")
[ -d "$pkg/models" ] || { echo "no signalp-6-package/models in [$1]" >&2; exit 1; }
pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.1.2
pip install --no-cache-dir "numpy<2" "$pkg"
weights=$(python3 -c 'import os, signalp; print(os.path.join(os.path.dirname(signalp.__file__), "model_weights"))')
mkdir -p "$weights"
cp -r "$pkg"/models/* "$weights"/
rm -rf /tmp/signalp
signalp6 --help >/dev/null
