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

# %%
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
        (base_file / "sample_data/short_reads_subsample.fastq", "short.fastq", "std::short_reads"),
        (base_file / "sample_data/long_reads_subsample.fastq", "long.fastq", "std::long_reads"),
        (base_file / "sample_data/ko_list", "ko_list", "std::kofamscan_ko_list"),
        (base_file / "sample_data/profiles/", "profiles/", "std::kofamscan_profile"),
        (base_file / "sample_data/bakta_db", "bakta_db", "std::bakta_database_light"),
        (base_file / "sample_data/cazy.fa", "cazy_db", "std::cazy_ref"),
        # (base_file / "sample_data/cazy.tsv", "cazy_annotations", "std::cazy_annotations"),
        (base_file / "sample_data/busco.faa", "busco_db", "std::busco_ref"),
        # (base_file / "sample_data/busco.tsv", "busco_annotations", "std::busco_annotations"),
        (base_file / "sample_data/species.info", "busco_map", "std::busco_map"),
        (base_file / "sample_data/pilon.fasta", "pilon", "std::hybrid_assembly"),
        # (base_file / "sample_data/assembly.fasta", "assembly.fasta", "std::long_reads_assembly"),
    ]
)

# %%
task = smith.GenerateWorkflow(
    given      = [containers, inputs],
    transforms = [transforms],
    targets    = [dtypes["cazy_annotations"], dtypes["busco_annotations"]]
)

# %%
for item in task.plan.steps:
    print(item.transform.name)
# %%

from IPython.display import display_png
task.RenderDAG("dag", format="png")
with open("dag.png", "rb") as f:
    display_png(f.read(), raw=True)

# %%
smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
# %%
smith.CheckWorkflow(task)
