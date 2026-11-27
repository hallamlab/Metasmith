#!/bin/sh
# Runs as root inside the build. DeepLocPro is public (CC BY-NC-SA 4.0), so the
# source comes from its GitHub repository at a pinned commit. Its ESM-2 650M
# weights download on first use into TORCH_HOME, which the image sets to
# /opt/torch and fills here, so a run needs no network and no writable home.
set -eu
COMMIT=32c0b37c7a31a0686ce022a697f48998fc2997b1
pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu torch==2.1.2+cpu
pip install --no-cache-dir "numpy<2" "setuptools<81" \
    "https://github.com/Jaimomar99/deeplocpro/archive/$COMMIT.tar.gz"
python3 -c 'from esm import pretrained; pretrained.load_model_and_alphabet("esm2_t33_650M_UR50D")'
chmod -R a+rX "$TORCH_HOME"
deeplocpro --help >/dev/null
