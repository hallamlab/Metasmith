from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()
base_dir = Path.cwd()


path_to_agent_home = Path("./std_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


inputs = DataInstanceLibrary("std_assembly_data.xgdb")
inputs.Add(
    items = [
        (base_dir / "sample_data/accession_short", "acc_short", "std::short_reads_accession"),
        (base_dir / "sample_data/accession_long", "acc_long", "std::long_reads_accession"),
    ]
)

task = smith.GenerateWorkflow(
    given = [containers, inputs],
    transforms = [transforms],
    targets = [dtypes["hybrid_assembly"]]
)
smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
