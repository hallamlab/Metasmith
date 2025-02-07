from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

from ..coms.ipc import LiveShell

class CONTAINER_RT(Enum):
    DOCKER = "docker"
    APPTAINER = "apptainer"

@dataclass
class Container:
    image: str
    workdir: Path|None = None
    binds: list[tuple[Path, Path]] = field(default_factory=list)
    runtime: CONTAINER_RT = CONTAINER_RT.DOCKER

    def SetRuntime(self, runtime: CONTAINER_RT):
        self.runtime = runtime

    def Pull(self, cache_dir: Path=None):
        local_file = None
        if cache_dir is not None and self.runtime == CONTAINER_RT.APPTAINER:
            name = self.image.split("/")[-1]
            if ":" in name:
                name = name.split(":")[0]
            local_file = cache_dir/f"{name}.sif"
        
        with LiveShell() as shell:
            if local_file is not None and local_file.exists():
                shell.Exec(f"mv {local_file} {local_file}.bak")
            shell.Exec(f"{self.runtime} pull {local_file} {self.image}", timeout=None)
            if local_file is not None:
                if local_file.exists():
                    shell.Exec(f"rm {local_file}.bak")
                    self.image = f"{local_file}"
                else:
                    shell.Exec(f"mv {local_file}.bak {local_file}")

    def RunCommand(self, command: str):
        if self.runtime == CONTAINER_RT.DOCKER:
            others = ["--rm", "-u $(id -u):$(id -g)"]
            workdir = f'--workdir="{self.workdir}"' if self.workdir is not None else ""
            binds = [f'--mount type=bind,source="{src}",target="{dst}"' for src, dst in self.binds+[("./", "/ws")]]
            binds = " ".join(binds)
        elif self.runtime == CONTAINER_RT.APPTAINER:
            others = ["--no-home"]
            workdir = f"--workdir {self.workdir}" if self.workdir is not None else ""
            binds = [f'{src}:{dst}' for src, dst in self.binds+[("./", "/ws")]]
            binds = f'--bind {",".join(binds)}'

        toks = [
            f"{self.runtime.value}",
            "run",
            *others,
            workdir,
            binds,
            self.image,
            command,
        ]
        return " ".join(x for x in toks if x != "")

    def Run(self, command: str):
        with LiveShell() as shell:
            shell.Exec(
                self.RunCommand(command),
            )
