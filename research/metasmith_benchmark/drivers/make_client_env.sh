#!/bin/bash
# Build the Python env a driver job's client runs in. Run once, on a fir login node.
#
# The client runs on the host, not in the metasmith image: its agent shell calls `module`,
# `apptainer` and the `msm` wrapper, and the image has none of them. These packages are the
# engine's import-time dependencies. Nextflow and every step still run inside the image.
set -euo pipefail

ENV="${BENCH_CLIENT_ENV:-$HOME/bench_client_env}"
module load python/3.12
python -m venv "$ENV"
"$ENV/bin/pip" install --upgrade pip
"$ENV/bin/pip" install blake3 cbor2 coolname numpy pyyaml pandas ipython
PYTHONNOUSERSITE=1 "$ENV/bin/python" -c "import blake3, cbor2, coolname, numpy, yaml, pandas, IPython; print('client env ok')"
