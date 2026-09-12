from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
source  = model.AddRequirement(lib.GetType("fabfos_data::esm_c"))
weights = model.AddProduct(lib.GetType("ref::esm_c_600m_weights"))

ARCHIVE = "esmc_600m.tgz"
WEIGHT_MEMBER = "data/weights/esmc_600m_2024_12_v0.pth"
MIN_WEIGHT_BYTES = 2_000_000_000

DRIVER = r'''
import os, sys, tarfile
from pathlib import Path

ROOT = Path("{source}")
OUT = Path("{out}")

subs = sorted(p for p in ROOT.iterdir() if p.is_dir())
if len(subs) != 1:
    raise SystemExit(
        f"[esmc] expected exactly one release under {{ROOT}}, found {{len(subs)}} "
        f"({{[p.name for p in subs]}}). Two snapshots of a model is not a case to "
        f"resolve by taking the newest -- an embedding pool and its query are only "
        f"comparable if they came from the same weights, and which ones is not "
        f"recoverable from the embeddings")
REL = subs[0]
src = REL / "{archive}"
if not src.exists():
    raise SystemExit(f"[esmc] {{src}} is absent -- the source folder's layout changed")
print(f"[esmc] release {{REL.name}}, {{src.stat().st_size:,}} bytes", flush=True)

# Verify the member WITHOUT unpacking 2 GB: tarfile reads the index.
with tarfile.open(src, "r:gz") as tf:
    members = {{m.name.lstrip("./"): m for m in tf.getmembers()}}
want = "{weight_member}"
m = members.get(want)
if m is None:
    raise SystemExit(
        f"[esmc] the archive has no {{want}}. The ESM SDK's from_pretrained resolves "
        f"that exact relative path from the process cwd, so a moved checkpoint is a "
        f"silent load failure downstream. Archive holds: "
        f"{{sorted(members)[:10]}}")
if m.size < {min_bytes}:
    raise SystemExit(
        f"[esmc] {{want}} is {{m.size:,}} bytes, under the {{{min_bytes}:,}} floor. A "
        f"gated 401 body and a truncated transfer both leave a member that exists")
print(f"[esmc] {{want}}  {{m.size:,}} bytes", flush=True)

OUT.parent.mkdir(parents=True, exist_ok=True)
os.link(src, OUT) if os.stat(src).st_dev == os.stat(OUT.parent).st_dev else None
if not OUT.exists():
    import shutil
    shutil.copyfile(src, OUT)
print(f"[esmc] -> {{OUT}}", flush=True)
'''


def protocol(context: ExecutionContext):
    isrc = context.Input(source)
    iout = context.Output(weights)
    driver = DRIVER.format(source=isrc.container, out=iout.container,
                           archive=ARCHIVE, weight_member=WEIGHT_MEMBER,
                           min_bytes=MIN_WEIGHT_BYTES)
    context.LocalShell("cat > _esm_c_weights.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv(env=image, cmd="python3 _esm_c_weights.py")

    return ExecutionResult(
        manifest=[{weights: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > MIN_WEIGHT_BYTES,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)
