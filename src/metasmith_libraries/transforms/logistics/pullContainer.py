from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::env"))
log     = model.AddProduct(lib.GetType("env::pulled_container"))

def protocol(context: ExecutionContext):
    ilog=context.Output(log)
    # Generic env: prefetch only applies to a container image. An env with no
    # container: for the active runtime (conda-only), or a runtime with no image
    # cache (mamba/native), has nothing to pull -> no-op success.
    try:
        container = context.GetContainerModel(image)
    except AssertionError as e:
        msg = f"no container image to prefetch for this runtime: {e}"
        Log.Info(msg)
        with open(ilog.local, "w") as f:
            f.write(msg)
        return ExecutionResult(manifest=[{log: ilog.local}], success=True)
    if container.GetLocalPath() is None:
        msg = "runtime has no container image cache; nothing to prefetch"
        Log.Info(msg)
        with open(ilog.local, "w") as f:
            f.write(msg)
        return ExecutionResult(manifest=[{log: ilog.local}], success=True)
    success = Path("pull_success")
    context.external_shell.Exec(f"[[ -e {container.GetLocalPath()} ]] && touch {success}")
    if success.exists():
        msg = f"local image already exists at [{container.GetLocalPath()}]"
        Log.Info(msg)
        with open(ilog.local, "w") as f:
            f.write(msg)
    else:
        Log.Info(f"pulling [{container.image}]")
        context.external_shell.Exec(f'bash -c "{container.MakePullCommand()} 2>&1 && touch {success}" | tee {ilog.external}')
    return ExecutionResult(
        manifest=[{log: ilog.local}],
        success=success.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(1),
        duration=Duration(hours=1),
    )
)
