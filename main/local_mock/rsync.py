#!/usr/bin/env python
# coding: utf-8

from metasmith.coms.terminals import LiveShell
from local.constants import WORKSPACE_ROOT
import sys

DEFAULT_HOME = "local"
if len(sys.argv)==1:
    k = DEFAULT_HOME
else:
    k = sys.argv[1]

switch = {
    "local" :   f"{WORKSPACE_ROOT}/main/local_mock/cache/local_home",
    "sockeye":  f"sockeye:~/scratch/metasmith_home",
    "cosmos":   f"cosmos:/home/tony/workspace/metasmith_home",
    "fir":      f"fir:/scratch/phyberos/metasmith",
}
if k not in switch:
    print(f"[{k}] is not registered")
    k = DEFAULT_HOME
home = switch[k]

print(f"injecting updates to [{home}]")
with LiveShell() as shell:
    shell.RegisterOnOut(lambda x: print(x))
    shell.RegisterOnErr(lambda x: print(f"E: {x}"))
    shell.Exec(f"rsync -ac --progress --mkpath {WORKSPACE_ROOT}/metasmith.sif {home}/metasmith.sif")
    shell.Exec(f"rsync -ac --progress --mkpath {WORKSPACE_ROOT}/main/relay_agent/dist/msm_relay {home}/relay/msm_relay")
    shell.Exec(f"rsync -ac --progress --mkpath --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/ {home}/dev/metasmith")
    shell.Exec(f"rsync -ac --progress --mkpath --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/nextflow_config {home}/lib/")


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
