"""ESM-C 600M -- the HuggingFace snapshot, as served, into `esm_c/<revision>/`.

**The repository is not gated, and the weights need no token.** This file used to say
the opposite, and acted on it by refusing without `HF_TOKEN`. Measured 2026-07-27 from
a machine with no token at all: `biohub/esmc-600m-2024-12` answers
`/api/models/...` with 200 and `"private": false`, a ranged GET of the checkpoint
returns 206, and `EvolutionaryScale/esmc-600m-2024-12` -- the name the model is
published under -- now answers **307, redirecting to `biohub/...`**. The weights are
the same bytes either way: the blob under both names hashes to
`8ef856e1a237ee3f995442df997a962e70057faadecf38fc0c8561bd3c2f4324`. So the ownership
moved and the gate went with it, and the old refusal was blocking a lane over a
condition that no longer holds. A token is still USED when present, because a
rate-limited anonymous fetch is a real failure mode; it is simply not required.

The size and member checks stay, and are the part that was always load-bearing. A
truncated multi-gigabyte transfer leaves a file that exists, and so does an error body
written by a naive `wget -O` -- neither is caught by anything except checking what
landed.

WHY THE REVISION IS READ FROM THE SERVER FIRST. `main` moves. The snapshot is
resolved to a commit sha before any bytes are fetched, and that sha names the
release directory -- so a directory in the originals tier always says exactly which
revision it holds, and re-fetching that revision later is possible.

WHAT LANDS. The repository as `huggingface_hub` returns it, tarred into
`esmc_600m.tgz` with the layout the consumer expects:

    data/weights/esmc_600m_2024_12_v0.pth     the 600M checkpoint
    config.json  README.md  .gitattributes    the rest of the repo, verbatim

The tar rather than a directory is not a carve: `functionalAnnotation/esm_c.py`
untars it into a working directory and `chdir`s there, because the ESM SDK's
`ESMC.from_pretrained` only accepts registered model NAMES and resolves
`data/weights/<file>.pth` relative to the process's cwd. The archive IS the unit
that layout belongs to.

The checkpoint is 2,300,275,866 bytes and fir already holds it in a HuggingFace cache
from the deployed method's run, so a rebuild there costs nothing; the fetch path is
what makes a clean machine work.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::esm_c"))

# The canonical name 307-redirects here; fetching the target directly means the
# recorded revision is the one actually served rather than one hop upstream of it.
REPO_ID = "biohub/esmc-600m-2024-12"
ARCHIVE = "esmc_600m.tgz"
# The one file everything else is for. Checked by name and by size, because a gated
# 401 body and a truncated transfer both produce a file that exists.
WEIGHT_MEMBER = "data/weights/esmc_600m_2024_12_v0.pth"
MIN_WEIGHT_BYTES = 2_000_000_000

STAGE = "_incoming"

DRIVER = r'''
import hashlib, json, os, subprocess, sys, tarfile
from pathlib import Path

OUT = Path("{out}")
STAGE = OUT / "{stage}"
REPO_ID = "{repo_id}"

# Used when present, never required -- the repository is public. See the module
# docstring for the measurement.
token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
print(f"[esm_c] hf token: {{'present' if token else 'none (repo is public)'}}", flush=True)

from huggingface_hub import HfApi, snapshot_download

# The revision is resolved BEFORE any bytes move: `main` moves, and a release
# directory that cannot say which revision it holds is not a pin.
api = HfApi(token=token)
info = api.model_info(REPO_ID)
rev = info.sha
print(f"[esm_c] {{REPO_ID}} @ {{rev}}", flush=True)

STAGE.mkdir(parents=True, exist_ok=True)
local = snapshot_download(repo_id=REPO_ID, revision=rev, token=token,
                          local_dir=str(STAGE / "repo"))
print(f"[esm_c] snapshot at {{local}}", flush=True)

w = Path(local) / "{weight_member}"
if not w.exists():
    raise SystemExit(f"[esm_c] the snapshot has no {{w.name}} -- upstream moved the "
                     f"checkpoint, and the consumer resolves it by that exact path")
if w.stat().st_size < {min_bytes}:
    raise SystemExit(f"[esm_c] {{w}} is {{w.stat().st_size:,}} bytes, under the "
                     f"{{{min_bytes}:,}} floor. An error body written by a naive "
                     f"download and a truncated transfer both leave a file that exists")

dest = OUT / rev
dest.mkdir(parents=True, exist_ok=True)
archive = dest / "{archive}"
with tarfile.open(archive, "w:gz") as tf:
    for p in sorted(Path(local).iterdir()):
        tf.add(p, arcname=p.name)
print(f"[esm_c] {{archive}} ({{archive.stat().st_size:,}} bytes)", flush=True)

# Upstream publishes no checksum for the tarball -- it does not exist upstream, this
# step makes it -- so the recorded digest is over the MEMBER, which does.
h = hashlib.sha256()
with open(w, "rb") as fh:
    for chunk in iter(lambda: fh.read(1 << 20), b""):
        h.update(chunk)
(dest / "PROVENANCE.json").write_text(json.dumps({{
    "repo_id": REPO_ID, "revision": rev,
    "weight_member": "{weight_member}",
    "weight_bytes": w.stat().st_size,
    "weight_sha256": h.hexdigest(),
}}, indent=2) + "\n")

subprocess.run(["rm", "-rf", str(STAGE)], check=True)
print(f"[esm_c] release {{rev}}", flush=True)
'''


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    driver = DRIVER.format(
        out=iout.container, stage=STAGE, repo_id=REPO_ID, archive=ARCHIVE,
        weight_member=WEIGHT_MEMBER, min_bytes=MIN_WEIGHT_BYTES,
    )
    context.LocalShell("cat > _acquire_esm_c.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="pip install --quiet huggingface_hub && python3 _acquire_esm_c.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _acquire_esm_c.py")

    releases = [p for p in iout.local.iterdir() if p.is_dir() and p.name != STAGE] \
        if iout.local.exists() else []
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(releases) == 1 and (releases[0] / ARCHIVE).exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=4)),
)
