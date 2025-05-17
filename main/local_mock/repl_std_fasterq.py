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
accession_path = base_file / "sample_data/test_accession"


# %% [markdown]
# Create a new DataInstanceLibrary to hold accession number

# %%
accession = DataInstanceLibrary("std_fasterq_accession.xgdb")
accession.Add(
    items = [
        (accession_path, "accession", "std::fasterq_accession")
    ]
)


# %% [markdown]
# Deploy agent

# %%
path_to_agent_home = Path("./std_fasterq_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


# %% [markdown]
# Create `short_reads`

# %%
task = smith.GenerateWorkflow(
    given      = [containers, accession],
    transforms = [transforms],
    targets    = [dtypes["short_reads"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
smith.CheckWorkflow(task)


# %% [markdown]
# Create `long_reads`

# %%
task = smith.GenerateWorkflow(
    given      = [containers, accession],
    transforms = [transforms],
    targets    = [dtypes["long_reads"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
smith.CheckWorkflow(task)


# %% [markdown]
# Create `read_stats`

# %%
# task = smith.GenerateWorkflow(
#     given      = [containers, accession],
#     transforms = [transforms],
#     targets    = [dtypes["read_stats"].WithLineage([dtypes["short_reads"]])]
# )
#
# smith.StageWorkflow(task, "clear")
# smith.RunWorkflow(task)
# smith.CheckWorkflow(task)
