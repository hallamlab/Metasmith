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
from os import read
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


# %% [markdown]

# %%
inputs = DataInstanceLibrary("std_data.xgdb")
inputs.Add(
    items = [
        (base_file / "sample_data/short_reads_subsample.fastq", "short", "std::short_reads"),
        (base_file / "sample_data/long_reads_subsample.fastq", "long", "std::long_reads"),
        (base_file / "sample_data/ko_list", "ko_list", "std::kofamscan_ko_list"),
        (base_file / "sample_data/profiles/", "profiles/", "std::kofamscan_profile"),
        (base_file / "sample_data/bakta_db", "bakta_db", "std::bakta_database"),
        (base_file / "sample_data/cazy.fa", "cazy_db", "std::cazy_ref"),
        (base_file / "sample_data/busco.faa", "busco_db", "std::busco_ref"),
        (base_file / "sample_data/species.info", "busco_map", "std::busco_map"),
        # (base_file / "sample_data/assembly.fasta", "assembly.fasta", "std::long_reads_assembly"),
    ]
)

# %%


task = smith.GenerateWorkflow(
    given      = [containers, inputs],
    transforms = [transforms],
    targets    = [dtypes["hybrid_assembly"]]
)


from IPython.display import display_png
task.RenderDAG("dag", format="png")
with open("dag.png", "rb") as f:
    display_png(f.read(), raw=True)

# %%
smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
# %%
smith.CheckWorkflow(task)
