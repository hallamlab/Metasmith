from pathlib import Path
from metasmith.coms.terminals import LiveShell
from metasmith.coms.containers import Container, ContainerRuntime
from metasmith.constants import AgentPaths, VERSION
from local.constants import WORKSPACE_ROOT
import sys, os

DEFAULT_HOME = "local"
if len(sys.argv)==1:
    k = DEFAULT_HOME
else:
    k = sys.argv[1]

switch = {
    "local" :   f"{WORKSPACE_ROOT}/main/local_mock/cache/local_home",
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
    lpath = Container(
        image=f"docker://quay.io/hallamlab/metasmith:{VERSION}",
        container_cache=Path(home)/AgentPaths.CONTAINER_CACHE,
        runtime=ContainerRuntime.APPTAINER
    ).GetLocalPath()
    sif_src = f"{WORKSPACE_ROOT}/metasmith.sif"
    if os.path.exists(sif_src):
        shell.Exec(f"rsync -au --progress {sif_src} {lpath}")
    else:
        print(f"W: skipping .sif rsync — source missing [{sif_src}]")

    dist = "target/x86_64-unknown-linux-musl/release"
    relay_src = f"{WORKSPACE_ROOT}/main/relay_agent/{dist}/msm_relay"
    if os.path.exists(relay_src):
        shell.Exec(f"rsync -ac --progress {relay_src} {home}/relay/msm_relay")
    else:
        print(f"W: skipping msm_relay rsync — source missing [{relay_src}]")
    if ":" in home: # is remote
        host, path = home.split(":")
        pre = f'ssh {host} mkdir -p "{path}/dev" && '
    else:
        pre = f'mkdir -p "{home}/dev" && '
    shell.Exec(f"{pre}rsync -ac --progress --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/ {home}/dev/metasmith")
    shell.Exec(f"rsync -ac --progress --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/nextflow_config {home}/lib/")


# In[2]:


# import logging

# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# logger.handlers.clear()
# logger.addHandler(logging.StreamHandler())
# logger.addHandler(logging.FileHandler(f"./x.log"))

# logger.info("asdf")
# # logger.handlers[0].flush()


# In[3]:


# with LiveShell() as shell:
#     shell.RegisterOnOut(lambda x: print(x))
#     shell.RegisterOnErr(lambda x: print(f"E: {x}"))
#     shell.Exec(f"sleep 300 && echo asdf")


# In[4]:


# with LiveShell() as shell:
#     shell.RegisterOnOut(lambda x: print(x))
#     shell.RegisterOnErr(lambda x: print(f"E: {x}"))
#     shell.Exec(
#         f"""
#         cd /home/tony/workspace/tools/Metasmith/main/local_mock/cache/ws1/run_container
#         apptainer run --no-home --workdir /ws \
#             --bind ./:/ws,.msm:/msm_home \
#             --bind ./:/agent_home \
#             --bind /home/tony/workspace/tools/Metasmith/src/metasmith:/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith \
#             /home/tony/workspace/tools/Metasmith/metasmith.sif nextflow

#         apptainer run -B ./:/ws
#         cd ~/downloads
#         mkdir x && cd x
#         /home/tony/workspace/metasmith/lib/msm_bootstrap
#         """
#     )
