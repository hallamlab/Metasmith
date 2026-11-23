import fcntl
import json
import zlib
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::env"))
record  = model.AddProduct(lib.GetType("env::pulled_container"))

# A setup run pulls every tool image of a plan with one step of this per image
# (`setup_subplan`). The image stays in the agent's store, which may be on another
# filesystem; the product is a record of where, so nothing is linked or copied.
#
# Every pull is its own Nextflow process, so no per-process limit caps them. They run
# on the driver's host, a shared login node whose per-user cgroup on fir is 16 GiB and
# where apptainer unpacks into node-local temp space, so at most SLOTS fetch at once.
SLOTS = 3


def _take_slot(key: str):
    d = AgentPaths.HOME_ROOT/AgentPaths.CONTAINER_CACHE/".pull_slots"
    d.mkdir(parents=True, exist_ok=True)
    slots = [open(d/str(i), "a") for i in range(SLOTS)]
    for f in slots:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return f
        except BlockingIOError:
            continue
    f = slots[zlib.crc32(key.encode()) % SLOTS]
    fcntl.flock(f, fcntl.LOCK_EX)
    return f


def protocol(context: ExecutionContext):
    out = context.Output(record)
    env = context.GetContainerModel(image)
    with _take_slot(env.image):
        ok = context.MaterialiseImage(env)
    stored = None
    if env.GetLocalPath() is not None:
        found = context.external_shell.Exec(
            f'for p in "{env.GetLocalPath()}" "{env.GetSandboxPath()}"; do [ -e "$p" ] && echo "$p"; done',
            history=True,
        )
        stored = next((x.strip() for x in found.out if x.strip()), None)
    ref = env.image.split("://", 1)[-1]
    with open(out.local, "w") as f:
        json.dump({
            "image": env.image,
            "digest": ref.split("@", 1)[1] if "@" in ref else None,
            "runtime": env.runtime.name,
            "store": stored,
        }, f, indent=1)
    return ExecutionResult(manifest=[{record: out.local}], success=ok)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    # A cached record would say an image is in the store after someone removed it.
    cacheable=False,
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=2),
    )
)
