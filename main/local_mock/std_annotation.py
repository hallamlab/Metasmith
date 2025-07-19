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


# %% [markdown]

# %%
inputs = DataInstanceLibrary("std_fasterq_assembly.xgdb")
inputs.Add(
    items = [
        (base_file / "sample_data/assembly.fna", "assembly", "std::assembly"),
        (base_file / "sample_data/ko_list", "ko_list", "std::kofamscan_ko_list"),
        (base_file / "sample_data/kofamscan.tsv", "kofamscan", "std::kofamscan_annotations"),
        (base_file / "sample_data/profiles/", "profiles/", "std::kofamscan_profile"),
        (base_file / "sample_data/bakta_db", "bakta_db", "std::bakta_database"),
        (base_file / "sample_data/bakta_out", "bakta_out", "std::bakta_annotations"),
        (base_file / "sample_data/cazy.fa", "cazy_db", "std::cazy_ref"),
        (base_file / "sample_data/cazy.tsv", "cazy_annotations", "std::cazy_annotations"),
        (base_file / "sample_data/busco.faa", "busco_db", "std::busco_ref"),
        (base_file / "sample_data/busco.tsv", "busco_annotations", "std::busco_annotations"),
        (base_file / "sample_data/species.info", "busco_map", "std::busco_map"),
    ]
)

task = smith.GenerateWorkflow(
    given      = [containers, inputs],
    transforms = [transforms],
    targets    = [dtypes["functional_annotations"]]
)

smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
