from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()

base_file = Path().resolve()


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
