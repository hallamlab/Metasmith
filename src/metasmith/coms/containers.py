from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

from ..coms.terminals import LiveShell

class ContainerRuntime(Enum):
    DOCKER = "docker"
    APPTAINER = "apptainer"

@dataclass
class Container:
    image: str
    container_cache: Path = Path("./")
    workdir: Path|None = None
    binds: list[tuple[Path|str, Path|str]] = field(default_factory=list)
    runtime: ContainerRuntime = ContainerRuntime.DOCKER

    def SetRuntime(self, runtime: ContainerRuntime):
        self.runtime = runtime

    def _get_local_path(self):
        name = self.image.split("/")[-1]
        if ":" in name:
            name = name.split(":")[0]
        return self.container_cache/f"{name}.sif"

    def MakePullCommand(self):
        if self.runtime == ContainerRuntime.APPTAINER:
            return f"{self.runtime.value} pull {self._get_local_path()} {self.image}"
        else:
            return f"{self.runtime.value} pull {self.image}"

    def MakeBindsParam(self, defaults:bool=True):
        default_binds = [("./", "/ws")] if defaults else []
        binds = {str(d):str(s) for s, d in default_binds+self.binds}
        binds = [(s, d) for d, s in binds.items()]
        if len(binds)==0: return ""
        match self.runtime:
            case ContainerRuntime.DOCKER:
                binds = [f'--mount type=bind,source="{src}",target="{dst}"' for src, dst in binds]
                binds = " ".join(binds)
            case ContainerRuntime.APPTAINER:
                binds = [f'{src}:{dst}' for src, dst in binds]
                binds = f'--bind {",".join(binds)}'
            case _: # default
                raise TypeError(f"unsupported runtime [{self.runtime}]")
        return binds

    def MakeRunCommand(self, local: bool|str = False, custom_bind_param: str|None=None):
        image = self.image
        binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
        match self.runtime:
            case ContainerRuntime.DOCKER:
                others = ["--rm", "-u $(id -u):$(id -g)"]
                workdir = f'--workdir="{self.workdir}"' if self.workdir is not None else ""
            case ContainerRuntime.APPTAINER:
                others = ["--no-home"]
                workdir = f"--workdir {self.workdir}" if self.workdir is not None else ""
                binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
                if not isinstance(local, bool):
                    image = local
                elif local:
                    image = self._get_local_path()
            case _: # default
                raise TypeError(f"unsupported runtime [{self.runtime}]")
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
