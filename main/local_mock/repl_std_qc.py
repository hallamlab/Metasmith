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
dataset_path = base_file / "sample_data/test_dataset.fastq"


# %% [markdown]
# Create a new DataInstanceLibrary to hold short read data

# %%
short_reads = DataInstanceLibrary("std_qc_short_reads.xgdb")
short_reads.AddTypeLibrary("std", dtypes)
short_reads.Add(
    items = [
        (dataset_path, "data.fastq", "std::short_reads")
    ]
)


# %% [markdown]
# Deploy agent to generate and run workflow

# %%
path_to_agent_home = Path("./std_qc_out").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()

task = smith.GenerateWorkflow(
    [containers, short_reads],
    [transforms],
    [dtypes["read_stats"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
smith.CheckWorkflow(task)



# %% [markdown]
# Create a new DataInstanceLibrary to hold long read data

# %%
long_reads = DataInstanceLibrary("std_qc_long_reads.xgdb")
long_reads.AddTypeLibrary("std", dtypes)
long_reads.Add(
    items = [
        (dataset_path, "data.fastq", "std::long_reads")
    ]
)


# %% [markdown]
# Generate and run workflow

# %%
task = smith.GenerateWorkflow(
    [containers, long_reads],
    [transforms],
    [dtypes["read_stats"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
smith.CheckWorkflow(task)
