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
    workdir: Path|str|None = None
    binds: list[tuple[Path|str, Path|str]] = field(default_factory=list)
    runtime: ContainerRuntime = ContainerRuntime.DOCKER

    def SetRuntime(self, runtime: ContainerRuntime):
        self.runtime = runtime

    def _get_image(self):
        image = self.image
        if self.runtime == ContainerRuntime.DOCKER:
            DOCKER_DOMAIN = "docker://"
            if image.startswith(DOCKER_DOMAIN):
                image = image.replace(DOCKER_DOMAIN, "")
        return image

    def GetLocalPath(self):
        # todo: docker-daemon local?
        match self.runtime:
            case ContainerRuntime.APPTAINER:
                name = self.image.replace("://", "..").replace(":", "..").replace("/", "_")
                return self.container_cache/f"{name}.sif"

    def MakePullCommand(self):
        image = self._get_image()
        match self.runtime:
            case ContainerRuntime.APPTAINER:
                return f"{self.runtime.value} pull {self.GetLocalPath()} {image}"
            case ContainerRuntime.DOCKER:
                return f"{self.runtime.value} pull --platform=linux/amd64 {image}"
            case _:
                return f"{self.runtime.value} pull {image}"

    def MakeBindsParam(self):
        binds = {str(d):str(s) for s, d in self.binds}
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
        image = self._get_image()
        binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
        match self.runtime:
            case ContainerRuntime.DOCKER:
                # todo: detect if image for matching platform exists first before forcing amd64
                others = ['--platform=linux/amd64', '--rm', '-u $(id -u):$(id -g)', '--network=host', '-e TMPDIR=${TMPDIR-"/tmp"}', '--entrypoint=""']
                workdir = f'--workdir="{self.workdir}"' if self.workdir is not None else ''
                run = 'run'
            case ContainerRuntime.APPTAINER:
                others = ['--no-home', '--cleanenv', '--env TMPDIR=${TMPDIR-"/tmp"}']
                workdir = f'--workdir "{self.workdir}"' if self.workdir is not None else ''
                binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
                if not isinstance(local, bool):
                    image = local
                elif local:
                    image = self.GetLocalPath()
                run = 'exec'
            case _: # default
                raise TypeError(f'unsupported runtime [{self.runtime}]')
        toks = [
            f"{self.runtime.value}",
            run,
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
