#!/usr/bin/env python
# coding: utf-8

from metasmith.coms.ipc import LiveShell
from lib.local.constants import WORKSPACE_ROOT


# host = "sockeye"
# home = f"{host}:~/scratch/metasmith_home"

# host = "cosmos"
# home = f"{host}:/home/tony/workspace/metasmith_home"

# home = f"{WORKSPACE_ROOT}/main/local_mock/cache/local_home"
home = f"{WORKSPACE_ROOT}/main/local_mock/std_home"

# home = f"{WORKSPACE_ROOT}/main/docs/metasmith_home"

with LiveShell() as shell:
    shell.RegisterOnOut(lambda x: print(x))
    shell.RegisterOnErr(lambda x: print(f"E: {x}"))
    # shell.Exec(f"rsync -ac --progress --mkpath {WORKSPACE_ROOT}/metasmith.sif {home}/metasmith.sif")
    shell.Exec(f"rsync -acu --progress --mkpath {WORKSPACE_ROOT}/main/relay_agent/dist/relay {home}/relay/msm_relay")
    shell.Exec(f"rsync -acu --progress --mkpath --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/ {home}/dev/metasmith")
    shell.Exec(f"rsync -acu --progress --mkpath --exclude=__pycache__ {WORKSPACE_ROOT}/src/metasmith/nextflow_config {home}/lib/")


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
