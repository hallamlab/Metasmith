#!/bin/bash
# Build the Python env a driver job's client runs in. Run once, on a fir login node.
#
# The client runs on the host, not in the metasmith image: its agent shell calls `module`,
# `apptainer` and the `msm` wrapper, and the image has none of them. These packages are the
# engine's import-time dependencies. Nextflow and every step still run inside the image.
set -euo pipefail

ENV="${BENCH_CLIENT_ENV:-$HOME/bench_client_env}"
module load python/3.12.4
python -m venv "$ENV"
"$ENV/bin/pip" install --upgrade pip
"$ENV/bin/pip" install blake3 cbor2 coolname numpy pyyaml pandas ipython
# CAUTION ~/.local holds its own yaml, so a bare import proves nothing. Check where each one loads from.
PYTHONNOUSERSITE=1 "$ENV/bin/python" -c "
import sys, blake3, cbor2, coolname, numpy, yaml, pandas, IPython
stray = [m.__name__ for m in (blake3, cbor2, coolname, numpy, yaml, pandas, IPython) if not m.__file__.startswith(sys.prefix)]
sys.exit(f'loaded outside {sys.prefix}: {stray}') if stray else print('client env ok')"
