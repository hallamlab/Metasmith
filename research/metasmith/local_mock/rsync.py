from pathlib import Path
from metasmith.coms.terminals import LiveShell
from metasmith.env import ContainerDef, Environment, Runtime
from metasmith.constants import AgentPaths, VERSION
from local.constants import WORKSPACE_ROOT
import sys, os

DEFAULT_HOME = "local"
if len(sys.argv)==1:
    k = DEFAULT_HOME
else:
    k = sys.argv[1]

switch = {
    "local" :   f"{WORKSPACE_ROOT}/research/metasmith/local_mock/cache/local_home",
    "lib":      f"/home/tony/workspace/tools/MetasmithLibraries/tests/test_msm_home",
    "scratch":  f"/home/tony/agentic_workspace/data/metasmith/scratch/agent_home",
    "sockeye":  f"sockeye:~/scratch/metasmith",
    "cosmos":   f"cosmos:/home/tony/workspace/metasmith_ws",
    "fir":      f"fir:/scratch/phyberos/metasmith",
    "chamois":  f"chamois:/home/tliu/metasmith",
}
if k not in switch:
    print(f"[{k}] is not registered")
    sys.exit(1)
    k = DEFAULT_HOME
home = switch[k]

print(f"injecting updates to [{home}]")
with LiveShell() as shell:
    shell.RegisterOnOut(lambda x: print(x))
    shell.RegisterOnErr(lambda x: print(f"E: {x}"))
    lpath = Environment(
        image=f"docker://quay.io/hallamlab/metasmith:{VERSION}",
        runtime=Runtime.APPTAINER,
        container=ContainerDef(cache=Path(home)/AgentPaths.CONTAINER_CACHE),
    ).GetLocalPath()
    sif_src = f"{WORKSPACE_ROOT}/metasmith.sif"
    if os.path.exists(sif_src):
        shell.Exec(f"rsync -au --progress {sif_src} {lpath}")
    else:
        print(f"W: skipping .sif rsync — source missing [{sif_src}]")

    dist = "target/x86_64-unknown-linux-musl/release"
    relay_src = f"{WORKSPACE_ROOT}/src/bash_relay/{dist}/msm_relay"
    if os.path.exists(relay_src):
        shell.Exec(f"rsync -ac --progress {relay_src} {home}/relay/msm_relay")
    else:
        print(f"W: skipping msm_relay rsync — source missing [{relay_src}]")
    if ":" in home:
        host, path = home.split(":")
        pre = f'ssh {host} mkdir -p "{path}/dev" && '
    else:
        pre = f'mkdir -p "{home}/dev" && '
    shell.Exec(f"{pre}rsync -ac --progress --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/ {home}/dev/metasmith")
    shell.Exec(
        f'MSMTAR=$(mktemp -d)/metasmith.tar'
        f' && tar -c --exclude=__pycache__ -C {WORKSPACE_ROOT}/src -f "$MSMTAR" metasmith'
        f' && rsync -ac --progress "$MSMTAR" {home}/dev/metasmith.tar'
        f' && rm -rf "$(dirname "$MSMTAR")"'
    )
    shell.Exec(f"rsync -ac --progress --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/nextflow_config {home}/lib/")
