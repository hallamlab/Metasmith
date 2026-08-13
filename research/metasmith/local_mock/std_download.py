# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
# ---

# %% [markdown]
# - Import required modules
# - Load data types (endpoints), data instances (container definitions), and transforms into locals from Std
# - Define local for test dataset

# %%
from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()

base_file = Path().resolve()


# %% [markdown]
# Set up agent

# %%
path_to_agent_home = Path("./std_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


task = smith.GenerateWorkflow(
    given      = [containers],
    transforms = [transforms],
    targets    = [dtypes["kofamscan_ko_list"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
