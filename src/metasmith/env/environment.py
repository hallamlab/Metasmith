"""The Environment abstraction — the one place that knows how a runtime is
provisioned, wrapped, invoked, and bridged.

`Environment` (formerly `Container`) owns all per-runtime routing: nothing
outside the `env` package should branch on a runtime. Today it covers the
two container runtimes (Docker, Apptainer); the mamba/native runtimes join
here without any caller learning a new name.

`Runtime` is the runtime discriminator (formerly `ContainerRuntime`). The
enum member *names* are the serialized form in `agent.yml`, so they are
stable wire identifiers — do not rename members without a migration.
"""

from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

from ..coms.terminals import LiveShell
from ..constants import AgentPaths


class Runtime(Enum):
    DOCKER = "docker"
    APPTAINER = "apptainer"
    MAMBA = "mamba"


# Runtimes that launch a tool across a container boundary, and therefore need
# the relay to bounce launches back to the host daemon. MAMBA does not — it
# runs tools in-process on the host filesystem.
_CONTAINER_RUNTIMES = (Runtime.DOCKER, Runtime.APPTAINER)


@dataclass
class Environment:
    image: str
    container_cache: Path = Path("./")
    workdir: Path|str|None = None
    binds: list[tuple[Path|str, Path|str]] = field(default_factory=list)
    extra_args: list[str] = field(default_factory=list)
    runtime: Runtime = Runtime.DOCKER
    # `native` is orthogonal to the runtime enum: it means "we are already
    # inside the target environment, emit no wrapper at all". It is not a
    # Runtime member because it composes with one (a native agent can still
    # describe its tools as mamba/docker for portability metadata).
    native: bool = False

    def SetRuntime(self, runtime: Runtime):
        self.runtime = runtime

    def _get_image(self):
        image = self.image
        if self.runtime == Runtime.DOCKER:
            DOCKER_DOMAIN = "docker://"
            if image.startswith(DOCKER_DOMAIN):
                image = image.replace(DOCKER_DOMAIN, "")
        return image

    def _cached_name(self):
        return self.image.replace("://", "..").replace(":", "..").replace("/", "_")

    def GetLocalPath(self):
        # todo: docker-daemon local?
        match self.runtime:
            case Runtime.APPTAINER:
                return self.container_cache/f"{self._cached_name()}.sif"

    def GetSandboxPath(self):
        # Sibling of GetLocalPath for the APPTAINER `build --sandbox` artifact
        # used when starter-suid is unavailable (squashfuse_ll path is broken
        # under msm_relay's fork chain on WSL2 — Bug E.2). The bare directory
        # is what `apptainer exec` consumes; no extension.
        match self.runtime:
            case Runtime.APPTAINER:
                return self.container_cache/f"{self._cached_name()}.sandbox"

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
        if self.runtime != Runtime.APPTAINER:
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
            case Runtime.APPTAINER:
                return f"{self.runtime.value} pull {self.GetLocalPath()} {image}"
            case Runtime.DOCKER:
                return f"{self.runtime.value} pull --platform=linux/amd64 {image}"
            case Runtime.MAMBA:
                # No remote image to fetch; provisioning is env creation,
                # handled in ProvisionSteps. Nothing to pull.
                return ""
            case _:
                return f"{self.runtime.value} pull {image}"

    def MakeBindsParam(self):
        # mamba/native run on the host filesystem — there is no boundary to
        # bind across, so binds collapse to nothing.
        if self.runtime == Runtime.MAMBA or self.native:
            return ""
        binds = {str(d):str(s) for s, d in self.binds}
        binds = [(s, d) for d, s in binds.items()]
        if len(binds)==0: return ""
        match self.runtime:
            case Runtime.DOCKER:
                binds = [f'--mount type=bind,source="{src}",target="{dst}"' for src, dst in binds]
                binds = " ".join(binds)
            case Runtime.APPTAINER:
                binds = [f'{src}:{dst}' for src, dst in binds]
                binds = f'--bind {",".join(binds)}'
            case _: # default
                raise TypeError(f"unsupported runtime [{self.runtime}]")
        return binds

    def MakeRunCommand(self, local: bool|str = False, custom_bind_param: str|None=None):
        # native: already inside the target environment — no wrapper, just
        # whatever extra args the caller asked for (usually none).
        if self.native:
            return " ".join(str(x) for x in self.extra_args if x != "")
        image = self._get_image()
        if self.runtime == Runtime.MAMBA:
            # `mamba run -n <env>` activates the conda env for the wrapped
            # command. No binds/workdir/cache — the host filesystem is shared.
            toks = ["mamba", "run", "-n", image, *self.extra_args]
            return " ".join(str(x) for x in toks if x != "")
        binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
        match self.runtime:
            case Runtime.DOCKER:
                # todo: detect if image for matching platform exists first before forcing amd64
                others = ['--platform=linux/amd64', '--rm', '-u $(id -u):$(id -g)', '--network=host', '-e TMPDIR=${TMPDIR-"/tmp"}', '--entrypoint=""']
                workdir = f'--workdir="{self.workdir}"' if self.workdir is not None else ''
                run = 'run'
            case Runtime.APPTAINER:
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

    # ------------------------------------------------------------------
    # Deploy-facing surface — the runtime-specific provisioning and the
    # shell that crosses (or doesn't cross) the runtime boundary. These
    # are the methods Agent.Deploy drives so that Deploy itself never
    # branches on a runtime. Container runtimes need the relay to launch
    # tools from inside the metasmith container; mamba/native do not.
    # ------------------------------------------------------------------

    @property
    def needs_relay(self) -> bool:
        # The relay exists solely to bounce tool launches across the
        # container boundary. Only the container runtimes have that
        # boundary; mamba/native run tools in-process on the host. A
        # native environment never crosses a boundary regardless of the
        # runtime it nominally carries.
        return not self.native and self.runtime in _CONTAINER_RUNTIMES

    @staticmethod
    def Detect() -> "Runtime":
        # Host-local default when no runtime was chosen at Deploy. Folded
        # in from direct_run._detect_runtime so the detection heuristic
        # lives with the rest of the runtime routing.
        import shutil
        if shutil.which("docker"):
            return Runtime.DOCKER
        if shutil.which("apptainer") or shutil.which("singularity"):
            return Runtime.APPTAINER
        return Runtime.DOCKER

    def ProvisionSteps(self, *, agent_home: Path, assertive: bool=False) -> list[tuple[str, str|None]]:
        # Deploy-time steps that make the image runnable on the host:
        # pull (if not cached) + the per-host SIF/sandbox decision. Returns
        # (cmd, display_cmd) pairs; empty for runtimes with nothing to pull
        # (mamba/native). The probe is a static two-axis check (setuid
        # starter-suid + apptainer major.minor); SIF is preferred when safe
        # (no disk doubling). Sandbox is built only on apptainer >=1.4
        # without setuid — the case where SIF engages squashfuse_ll (Bug
        # E.2 wedge under msm_relay on WSL2) and the sandbox path goes
        # through kernel overlayfs. On apptainer 1.3.x without setuid the
        # sandbox path itself falls back to fuse-overlayfs (Bug E.4 SIGBUS
        # on fir under SLURM array contention), so SIF is kept there too.
        # Verdict is re-evaluated on every Deploy(); a stale sandbox from a
        # prior host config is removed when the verdict flips.
        steps: list[tuple[str, str|None]] = []
        local_path = self.GetLocalPath()
        if not local_path:
            return steps
        pull_cmd = self.MakePullCommand()
        steps.append((
            f'mkdir -p "{local_path.parent}" && [ -e {local_path} ] || {pull_cmd}',
            f"{{if not exists}}: {pull_cmd.replace(str(agent_home), '$AGENT_HOME')}",
        ))
        sandbox_path = self.GetSandboxPath()
        probe = self.MakeSandboxDecisionProbe()
        build_sandbox = self.MakeBuildSandboxCommand()
        force = f'rm -rf {sandbox_path} && ' if assertive else ''
        steps.append((
            (
                f'{force}'
                f'VERDICT=$({probe}); '
                f'if [ "$VERDICT" = "use-sandbox" ]; then '
                f'[ -d {sandbox_path} ] || {build_sandbox}; '
                f'else rm -rf {sandbox_path}; fi'
            ),
            f"{{probe host; build sandbox iff apptainer>=1.4 and no setuid}}: apptainer build --sandbox {sandbox_path.name} {local_path.name}".replace(str(agent_home), '$AGENT_HOME'),
        ))
        return steps

    def RenderMsmWrapper(self, *, agent_home: Path, run_command: str, main_binds: str, dev_binds: str, dev_src: str) -> str:
        # Body of the `msm` convenience wrapper deployed into the agent
        # home: invoke `metasmith` inside the runtime. For container
        # runtimes this is the run-command (computed by the caller from the
        # role-specific binds, since the `msm` wrapper carries no --workdir)
        # with accumulated $BINDS; for mamba/native there is no container,
        # so metasmith runs directly under an optional wrapper prefix.
        if self.needs_relay:
            return f"""
                #!/bin/bash
                AGENT_HOME={agent_home}
                BINDS="$BINDS {main_binds}"
                if [ -e "{dev_src}" ]; then
                    echo "including dev binds"
                    BINDS="$BINDS {dev_binds}"
                fi
                echo "binds [$BINDS]"
                {run_command} metasmith $@
                """
        wrapper = self.MakeWrapperPrefix()
        prefix = f"{wrapper} " if wrapper else ""
        return f"""
            #!/bin/bash
            AGENT_HOME={agent_home}
            {prefix}metasmith $@
            """

    def RenderBootstrap(self, *, agent_home: Path, run_command: str, run_binds: str, dev_binds: str, dev_src: str, bind_file: str) -> str:
        # Body of `msm_bootstrap`, the per-step launcher Nextflow calls.
        # For container runtimes it (a) bounces back to the host via the
        # relay when invoked from inside the container, then (b) runs each
        # metasmith step inside a fresh container with the relay daemon
        # bridging tool launches. For mamba/native there is no boundary to
        # cross: the step runs in-process with no bounce and no relay.
        # `run_command` is computed by the caller from the bootstrap's
        # /ws-workdir sub-environment.
        if self.needs_relay:
            return f"""
                #!/bin/bash

                AGENT_HOME={agent_home}
                TASK_DIR=$1
                STEP=$2
                HOST_NAME=$3
                CWD=${{4:-$(pwd -P)}}
                cd $CWD
                if [ -e "{AgentPaths.HOME_ROOT}" ]; then
                    echo "bootstrap called from container, bouncing to external [$@]"
                    REL_CWD=$(realpath --relative-to="{AgentPaths.HOME_ROOT}" $CWD)
                    CMD="{AgentPaths.to_bootstrap(Path('$AGENT_HOME'))} $@ $AGENT_HOME/$REL_CWD"
                    /app/msm_relay.x86_64-linux --io {AgentPaths.to_relay().parent}/$HOST_NAME bounce "$CMD"
                    exit
                fi

                echo "bootstrap ======================"
                INTERNALS="_metasmith"
                [ -z $STEP ] && echo "no step provided" && exit 1
                echo "cwd [$(pwd -P)]"
                echo "task [$TASK_DIR]"
                echo "step [$STEP]"
                function run_container {{
                    BINDS="{run_binds}"
                    if [ -e "{dev_src}" ]; then
                        echo "including dev binds"
                        BINDS="$BINDS {dev_binds}"
                    fi
                    if [ -e "./{bind_file}" ]; then
                        echo "including linked data binds"
                        BINDS="$BINDS $(cat ./{bind_file})"
                    fi
                    echo "final binds:"
                    echo "$BINDS"
                    {run_command} $@
                }}
                echo "deploy relay ==================="
                run_container metasmith api deploy_from_container -a workspace=$INTERNALS architecture=$(uname -m) system=$(uname -s)
                find $INTERNALS/relay/
                echo "pre execute ===================="
                find .
                ls -lh .
                echo "relay =========================="
                $INTERNALS/relay/msm_relay start --local
                echo "execute ========================"
                run_container metasmith api execute_transform -a step_index=$STEP -a workspace=$TASK_DIR host=$(hostname)
                echo "post execute ==================="
                find .
                ls -lh .
                echo "cleanup ========================"
                $INTERNALS/relay/msm_relay stop
                echo "relay logs ====================="
                $INTERNALS/relay/msm_relay logs
                """
        wrapper = self.MakeWrapperPrefix()
        prefix = f"{wrapper} " if wrapper else ""
        return f"""
            #!/bin/bash

            AGENT_HOME={agent_home}
            TASK_DIR=$1
            STEP=$2
            HOST_NAME=$3
            CWD=${{4:-$(pwd -P)}}
            cd $CWD
            echo "bootstrap ======================"
            [ -z $STEP ] && echo "no step provided" && exit 1
            echo "cwd [$(pwd -P)]"
            echo "task [$TASK_DIR]"
            echo "step [$STEP]"
            {prefix}metasmith api execute_transform -a step_index=$STEP -a workspace=$TASK_DIR host=$(hostname)
            """

    def MakeWrapperPrefix(self) -> str:
        # The command prefix that places a bare `metasmith ...` call into
        # this environment without a container. Empty for container
        # runtimes (they wrap via MakeRunCommand) and for native (already
        # inside); mamba activates its env via `mamba run -n <env>`.
        if self.native:
            return ""
        if self.runtime == Runtime.MAMBA:
            return f"mamba run -n {self._get_image()}"
        return ""

    def ConnectShell(self, server_path: Path|None=None, setup_commands: list[str]|None=None):
        # The shell a caller should run tool commands on. Container runtimes
        # cross the boundary via the relay (RemoteShell bounces launches back
        # to the host daemon at `server_path`); mamba/native run in-process,
        # so a plain local shell suffices. The relay client is constructed
        # only here — no caller outside the env module builds a RemoteShell.
        if self.needs_relay:
            from ..coms.via_file_watcher import RemoteShell
            assert server_path is not None, "relay runtimes require a server path"
            return RemoteShell(server_path, timeout=60, setup_commands=setup_commands or [])
        return LiveShell()
