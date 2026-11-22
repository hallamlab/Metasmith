#!/bin/sh
# UNVERIFIED against a real download. Runs as root inside the build, from a
# DeepLocPro package archive holding a pip-installable directory (setup.py or
# pyproject.toml), which installs the `deeplocpro` command.
set -eu
mkdir -p /tmp/deeplocpro
tar -xf "$1" -C /tmp/deeplocpro
pkg=$(dirname "$(find /tmp/deeplocpro \( -name setup.py -o -name pyproject.toml \) | head -1)")
pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.1.2
pip install --no-cache-dir "numpy<2" "$pkg"
rm -rf /tmp/deeplocpro
command -v deeplocpro
