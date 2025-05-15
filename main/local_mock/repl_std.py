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
# - Save the `containers` DataInstanceLibrary for introspection

# %%
from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()
containers.Save()

# %% [markdown]
# Create a new DataInstanceLibrary to hold input data, and save it for introspection

# %%
base_file = Path().resolve()
data = DataInstanceLibrary("inputs.xgdb")
data.AddTypeLibrary("std", dtypes)
data.Add(
    items = [
        (base_file / "sample_data/test_dataset.fastq", "data.fastq", "std::short_reads")
    ]
)
data.Save()

# %% [markdown]
# Deploy agent to generate and run workflow

# %%
path_to_agent_home = Path("./metasmith_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()

task = smith.GenerateWorkflow(
    [containers, data],
    [transforms],
    [dtypes["read_stats"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)

smith.CheckWorkflow(task)
