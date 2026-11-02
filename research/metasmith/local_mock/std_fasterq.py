from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()

base_file = Path().resolve()


path_to_agent_home = Path("./std_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


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


accession_long = DataInstanceLibrary("std_fasterq_accession_long.xgdb")
accession_long.Add(
    items = [
        (base_file / "sample_data/accession_long.fastq", "accession", "std::long_reads")
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
