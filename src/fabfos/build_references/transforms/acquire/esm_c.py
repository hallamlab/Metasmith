from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::esm_c"))

REPO_ID = "biohub/esmc-600m-2024-12"
ARCHIVE = "esmc_600m.tgz"
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
