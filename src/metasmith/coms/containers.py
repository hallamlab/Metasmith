from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

from ..coms.ipc import LiveShell

class CONTAINER_RUNTIME(Enum):
    DOCKER = "docker"
    APPTAINER = "apptainer"

@dataclass
class Container:
    image: str
    container_cache: Path = Path("./")
    workdir: Path|None = None
    binds: list[tuple[Path, Path]] = field(default_factory=list)
    runtime: CONTAINER_RUNTIME = CONTAINER_RUNTIME.DOCKER

    def SetRuntime(self, runtime: CONTAINER_RUNTIME):
        self.runtime = runtime

    def _get_local_path(self):
        name = self.image.split("/")[-1]
        if ":" in name:
            name = name.split(":")[0]
        return self.container_cache/f"{name}.sif"

    def MakePullCommand(self):
        if self.runtime == CONTAINER_RUNTIME.APPTAINER:
            return f"{self.runtime.value} pull {self._get_local_path()} {self.image}"
        else:
            return f"{self.runtime.value} pull {self.image}"

    def MakeRunCommand(self, local: bool|str = False):
        image = self.image
        default_binds = [("./", "/ws")]
        binds = {str(d):str(s) for s, d in default_binds+self.binds}
        binds = [(s, d) for d, s in binds.items()]
        if self.runtime == CONTAINER_RUNTIME.DOCKER:
            others = ["--rm", "-u $(id -u):$(id -g)"]
            workdir = f'--workdir="{self.workdir}"' if self.workdir is not None else ""
            binds = [f'--mount type=bind,source="{src}",target="{dst}"' for src, dst in binds]
            binds = " ".join(binds)
        elif self.runtime == CONTAINER_RUNTIME.APPTAINER:
            others = ["--no-home"]
            workdir = f"--workdir {self.workdir}" if self.workdir is not None else ""
            binds = [f'{src}:{dst}' for src, dst in binds]
            binds = f'--bind {",".join(binds)}'
            if not isinstance(local, bool):
                image = local
            elif local:
                image = self._get_local_path()

        toks = [
            f"{self.runtime.value}",
            "run",
            *others,
            workdir,
            binds,
            image,
        ]
        return " ".join(str(x) for x in toks if x != "")

    def Run(self, command: str):
        with LiveShell() as shell:
            shell.Exec(
                f"{self.MakeRunCommand()} {command}",
            )
