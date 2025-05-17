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
path_to_agent_home = Path("./std_fasterq_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


# %% [markdown]
# Fetch `short_reads`

# %%
accession_short = DataInstanceLibrary("std_fasterq_accession_short.xgdb")
accession_short.Add(
    items = [
        (base_file / "sample_data/accession_short", "accession", "std::short_reads_accession")
    ]
)

task = smith.GenerateWorkflow(
    given      = [containers, accession_short],
    transforms = [transforms],
    targets    = [dtypes["read_stats"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
smith.CheckWorkflow(task)


# %% [markdown]
# Fetch `long_reads`

# %%
accession_long = DataInstanceLibrary("std_fasterq_accession_long.xgdb")
accession_long.Add(
    items = [
        (base_file / "sample_data/accession_long", "accession", "std::long_reads_accession")
    ]
)

task = smith.GenerateWorkflow(
    given      = [containers, accession_long],
    transforms = [transforms],
    targets    = [dtypes["read_stats"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
smith.CheckWorkflow(task)
