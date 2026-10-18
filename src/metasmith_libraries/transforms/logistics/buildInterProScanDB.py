from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
img_ipr = model.AddRequirement(lib.GetType("env::interproscan.env"))
sources = model.AddRequirement(lib.GetType("ref::interproscan_sources"))
data    = model.AddProduct(lib.GetType("ref::interproscan_data"))


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idata = context.Output(data)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        "mkdir -p ipr_data",
        f"tar xzf {isrc.local} -C ipr_data --strip-components=1",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # setup.py and interproscan.properties are the IMAGE's copies at
    # /opt/interproscan, and the container's working directory is the task's /ws,
    # so the indexing has to run from there. The downloaded `data/` is bound over
    # the image's missing one, so what runs is the installed tool indexing the
    # models just unpacked.
    ipr_data = context.Mount(context.external_cwd/"ipr_data/data", "/opt/interproscan/data")
    context.ExecWithEnv(
        env=img_ipr,
        binds=[ipr_data],
        cmd=f"""
            cd /opt/interproscan
            python3 setup.py -f interproscan.properties --force
        """
    )

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    if context.LocalShell(f"mv ipr_data/data ./ && tar -I 'pigz {threads}' -cf {idata.local} ./data") != 0:
        return ExecutionResult(manifest=[], success=False)

    # hmmpress writes these four beside every HMM library it indexes, so their
    # presence is what setup.py having run looks like.
    pressed = list(Path("data").rglob("*.h3i"))
    Log.Info(f"setup.py pressed [{len(pressed)}] HMM libraries")
    return ExecutionResult(
        manifest=[{data: idata.local}],
        success=idata.local.exists() and len(pressed) > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    labels=["noscratch"],
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=8),
    ),
)
