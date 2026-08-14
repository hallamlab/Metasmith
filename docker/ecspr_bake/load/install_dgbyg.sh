#!/bin/bash
# Install dGbyG so that its 100 PRE-TRAINED HEADS can actually be found.
#
# WHY THIS IS NOT `pip install git+https://github.com/f-wc/dGbyG.git`. That is what
# envs/build-refs-equilibrator.yml has done all along, and it produces an importable
# package that cannot load a single weight. Two separate reasons, either one fatal:
#
#   1. THE WEIGHTS ARE NOT IN THE PACKAGE. `models/mpnn_A139_B23_E300_L2_v2/` is 100
#      .pt files (~105 MB) sitting at the REPOSITORY root, outside `src/dGbyG/`, and
#      nothing declares them as package data. A pip install lands 344 KB of python and
#      no tensors.
#   2. THE PATH RESOLUTION ASSUMES A SOURCE CHECKOUT. `api.py` computes
#      `infer_model_path = __file__.split('src')[0] + 'models/mpnn_A139_B23_E300_L2_v2'`
#      -- which only means anything when the file lives at `<repo>/src/dGbyG/api.py`.
#      Under site-packages there is no 'src' in the path, `split` returns the whole
#      string unchanged, and the model folder resolves to a path under `api.py/`.
#
# So the repo is CLONED, kept whole, and put on the path as a source tree. The clone
# location must contain no earlier `src` component, or the same split lands in the wrong
# place -- /opt/dGbyG is chosen for that reason and is not arbitrary.
#
# A .pth FILE, NOT `ENV PYTHONPATH`. Every transform in this build sets PYTHONPATH to
# its own buildlib directory, which REPLACES an image-level one; a member that imports
# only when nobody set PYTHONPATH is a member that fails exactly when it is used. A .pth
# in site-packages is read by the interpreter itself and survives that.
#
# THE DEPENDENCIES DO NOT COME FROM HERE. dGbyG's setup.py declares no `install_requires`
# whatsoever, so neither a clone nor a pip install resolves anything -- the first import
# dies on `pubchempy`. They are pinned in the env file beside the rest of the image, which
# is also the only place they can be reconciled with our torch and rdkit pins.
#
# THE WEIGHTS ARE BAKED IN, and that is a deliberate tier decision matching how CLEAN's
# ESM-1b and the ProteinBERT bundle are handled: weights that ship with a method are part
# of the method, not a `data/` chunk, so no transform acquires them and no run can be
# short of them. 105 MB is a cheap layer for making the member unconditional.
set -euo pipefail

REPO_DIR="${1:-/opt/dGbyG}"
# PINNED BY COMMIT, not by branch. The heads are the model: a moving `main` would change
# what this member says without changing anything anyone recorded.
DGBYG_COMMIT="${DGBYG_COMMIT:-2202606a88f09423bcbf9e0989b7e12d79f32cc0}"
HEADS="models/mpnn_A139_B23_E300_L2_v2"

case "$REPO_DIR" in
    */src/*) echo "[dgbyg] refusing to clone into $REPO_DIR: the path contains an 'src'" \
                  "component before the repo's own, and api.py resolves its model" \
                  "folder with __file__.split('src')[0]" >&2; exit 1 ;;
esac

git clone --filter=blob:none --no-checkout https://github.com/f-wc/dGbyG.git "$REPO_DIR"
git -C "$REPO_DIR" checkout --quiet "$DGBYG_COMMIT"

SITE=$(python -c 'import site; print(site.getsitepackages()[0])')
echo "$REPO_DIR/src" > "$SITE/dgbyg.pth"

# Assert the member can be BUILT, not merely imported. An import proves the python
# parses; only instantiating the inference model proves the heads were found and load.
python - "$REPO_DIR" "$HEADS" <<'PY'
import sys
from pathlib import Path

repo, heads = Path(sys.argv[1]), sys.argv[2]
folder = repo / heads
pts = sorted(folder.glob("*.pt"))
assert pts, f"no .pt heads under {folder} -- the clone did not bring the weights"

import dGbyG.api as api
resolved = Path(api.infer_model_path[0])
assert resolved == folder, (
    f"dGbyG resolves its heads to {resolved}, not {folder}. That is the "
    f"__file__.split('src') path assumption breaking -- see this script's header.")

from dGbyG.model.inference import Inference_Model
Inference_Model(str(folder), device="cpu")
print(f"dgbyg OK: {len(pts)} heads loaded from {folder}")
PY
