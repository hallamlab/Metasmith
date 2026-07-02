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
    extra_args: list[str] = field(default_factory=list)
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

    def _cached_name(self):
        return self.image.replace("://", "..").replace(":", "..").replace("/", "_")

    def _store_root(self):
        # Single point of control for the apptainer image-store location.
        # Prefer APPTAINER_CACHEDIR when set, else the agent-home default the
        # caller passed in `container_cache`. The value is a shell expression
        # expanded on the *execution host* (the same way `$AGENT_HOME` is in
        # these strings), so an HPC deploy picks up the cluster's setting and
        # the write side (pull/build) and read side (exec) can never diverge.
        # Both GetLocalPath and GetSandboxPath build off this so the .sif and
        # .sandbox always stay siblings under one root.
        return Path(f"${{APPTAINER_CACHEDIR:-{self.container_cache}}}")

    def GetLocalPath(self):
        # todo: docker-daemon local?
        match self.runtime:
            case ContainerRuntime.APPTAINER:
                return self._store_root()/f"{self._cached_name()}.sif"

    def GetSandboxPath(self):
        # Sibling of GetLocalPath for the APPTAINER `build --sandbox` artifact
        # used when starter-suid is unavailable (squashfuse_ll path is broken
        # under msm_relay's fork chain on WSL2 — Bug E.2). The bare directory
        # is what `apptainer exec` consumes; no extension.
        match self.runtime:
            case ContainerRuntime.APPTAINER:
                return self._store_root()/f"{self._cached_name()}.sandbox"

    def MakeSandboxDecisionProbe(self):
        # Emits either "use-sif" or "use-sandbox" on stdout, encoding the
        # host-local choice of rootfs delivery for APPTAINER. Two-axis static
        # check; no `apptainer exec` involved.
        #
        # use-sif (default) — either:
        #   (a) setuid starter-suid present → kernel squashfs mount, no FUSE
        #       (HPC with privileged apptainer: Sockeye), or
        #   (b) apptainer <1.4 without setuid → sandbox path falls back to
        #       fuse-overlayfs (race-prone under sbatch arrays; SIGBUS on fir
        #       1.3.5). SIF goes through squashfuse_ll which works fine on
        #       HPC batch nodes without the relay fork chain.
        #
        # use-sandbox — apptainer >=1.4 without setuid: SIF would engage
        # squashfuse_ll (wedges under msm_relay's fork chain on WSL2 — Bug
        # E.2), but the sandbox path routes through unprivileged kernel
        # overlayfs (no FUSE daemon in the chain).
        if self.runtime != ContainerRuntime.APPTAINER:
            return ""
        return (
            'APPTAINER_BIN=$(readlink -f "$(command -v apptainer)" 2>/dev/null); '
            'SUID="$(dirname "$APPTAINER_BIN")/../libexec/apptainer/bin/starter-suid"; '
            'if [ -u "$SUID" ]; then echo "use-sif"; '
            'else V=$(apptainer --version 2>/dev/null | awk \'NR==1{print $NF}\'); '
            'MAJ=${V%%.*}; REST=${V#*.}; MIN=${REST%%.*}; '
            'if [ "${MAJ:-0}" -ge 2 ] || { [ "${MAJ:-0}" -eq 1 ] && [ "${MIN:-0}" -ge 4 ]; }; '
            'then echo "use-sandbox"; else echo "use-sif"; fi; fi'
        )

    def MakeBuildSandboxCommand(self):
        sif = self.GetLocalPath()
        sandbox = self.GetSandboxPath()
        if sif is None or sandbox is None: return ""
        return f"apptainer build --force --sandbox {sandbox} {sif}"

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
                others = ['--no-home', '--cleanenv', '--env TMPDIR=${TMPDIR-"/tmp"}', '--env OPENBLAS_NUM_THREADS=1', '--env OMP_NUM_THREADS=1']
                workdir = f'--pwd "{self.workdir}"' if self.workdir is not None else ''
                binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
                if not isinstance(local, bool):
                    image = local
                elif local:
                    # Prefer the unpacked sandbox directory when deploy built
                    # one (host lacks setuid starter-suid); fall back to SIF
                    # otherwise. The conditional collapses cleanly in either
                    # direction without per-call probing.
                    sif = self.GetLocalPath()
                    sandbox = self.GetSandboxPath()
                    image = f'"$(if [ -d "{sandbox}" ]; then echo "{sandbox}"; else echo "{sif}"; fi)"'
                run = 'exec'
            case _: # default
                raise TypeError(f'unsupported runtime [{self.runtime}]')
        toks = [
            f"{self.runtime.value}",
            run,
            *others,
            workdir,
            binds,
            *self.extra_args,
            image,
        ]
        return " ".join(str(x) for x in toks if x != "")

    def Run(self, command: str):
        with LiveShell() as shell:
            shell.Exec(
                f"{self.MakeRunCommand()} {command}",
            )
