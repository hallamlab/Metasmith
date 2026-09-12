from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

from ..coms.terminals import LiveShell
from ..constants import AgentPaths


class Runtime(Enum):
    DOCKER = "docker"
    APPTAINER = "apptainer"
    MAMBA = "mamba"


class Rootfs(Enum):
    AUTO = "auto"
    SIF = "sif"
    SANDBOX = "sandbox"

    @classmethod
    def Parse(cls, value: "str|Rootfs|None") -> "Rootfs":
        if value is None: return cls.AUTO
        if isinstance(value, cls): return value
        try:
            return cls(str(value).strip().lower())
        except ValueError:
            raise ValueError(
                f"unknown rootfs mode [{value}]; expected one of "
                f"{[m.value for m in cls]}"
            )


_CONTAINER_RUNTIMES = (Runtime.DOCKER, Runtime.APPTAINER)


@dataclass
class ContainerDef:
    cache: Path = Path("./")
    workdir: Path|str|None = None
    binds: list[tuple[Path|str, Path|str]] = field(default_factory=list)


@dataclass
class Environment:
    image: str
    extra_args: list[str] = field(default_factory=list)
    runtime: Runtime = Runtime.DOCKER
    # `native` is orthogonal to the runtime enum: it means "we are already
    # inside the target environment, emit no wrapper at all". It is not a
    # Runtime member because it composes with one (a native agent can still
    # describe its tools as mamba/docker for portability metadata).
    native: bool = False
    container: ContainerDef = field(default_factory=ContainerDef)
    rootfs: Rootfs = Rootfs.AUTO
    # Site override for the GPU flags below. Some hosts need more than the
    # runtime's own switch to actually expose a device -- WSL2 is the live
    # example: apptainer's `--nv` library discovery misses the driver stack
    # under /usr/lib/wsl, so `nvidia-smi` finds its binary but NVML reports
    # "GPU access blocked by the operating system". Which extra flags a host
    # needs is a host fact, so it is configured (Agent.gpu_args) rather than
    # sniffed at run time.
    gpu_args: list[str] = field(default_factory=list)

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

    def _store_root(self):
        return Path(f"${{APPTAINER_CACHEDIR:-{self.container.cache}}}")

    def GetLocalPath(self):
        match self.runtime:
            case Runtime.APPTAINER:
                return self._store_root()/f"{self._cached_name()}.sif"

    def GetSandboxPath(self):
        match self.runtime:
            case Runtime.APPTAINER:
                return self._store_root()/f"{self._cached_name()}.sandbox"

    def _stamp_path(self, artifact: "Path|None"):
        if artifact is None: return None
        return Path(f"{artifact}.verified")

    def GetLocalStampPath(self):
        return self._stamp_path(self.GetLocalPath())

    def GetSandboxStampPath(self):
        return self._stamp_path(self.GetSandboxPath())

    def MakeVerifyCommand(self, *, sandbox: bool = False):
        # Prove the materialised artifact actually mounts, and record that it did.
        #
        # `apptainer exec <artifact> true` is the probe because mounting the
        # rootfs is the exact thing that failed: a SIF that downloaded with a bad
        # squashfs superblock satisfies the `[ -e ]` every arm of the chain used
        # to gate on, and only fails when a tool tries to read its own
        # filesystem — hops away from the pull that produced it, reported as
        # something else entirely. A header-only inspection (`sif list`) passes on
        # exactly those images, and `apptainer verify` answers a different
        # question: cryptographic signatures, which biocontainers do not carry.
        #
        # `--no-home --cleanenv` keep the probe answering about the image rather
        # than about host state. A false negative here is expensive — it deletes
        # a sound artifact of several hundred megabytes and fetches it again.
        #
        # Success writes a sibling stamp; failure removes the artifact *and* any
        # stamp and reports failure, so the caller's fallback chain advances to
        # its next rung instead of trusting what it just produced.
        artifact = self.GetSandboxPath() if sandbox else self.GetLocalPath()
        stamp = self.GetSandboxStampPath() if sandbox else self.GetLocalStampPath()
        if artifact is None or stamp is None: return ""
        return (
            f'{{ apptainer exec --no-home --cleanenv {artifact} true >/dev/null 2>&1 '
            f'&& : > {stamp}; }} || {{ rm -rf {artifact} {stamp}; false; }}'
        )

    def MakeBuildSandboxCommand(self, from_image: bool = False):
        sandbox = self.GetSandboxPath()
        if sandbox is None: return ""
        if from_image:
            return f"apptainer build --force --sandbox {sandbox} {self._get_image()}"
        sif = self.GetLocalPath()
        if sif is None: return ""
        return f"apptainer build --force --sandbox {sandbox} {sif}"

    def MakeBuildSifCommand(self, no_fragments: bool = False):
        sif = self.GetLocalPath()
        if sif is None: return ""
        args = ' --mksquashfs-args "-no-fragments"' if no_fragments else ""
        return f"apptainer build --force{args} {sif} {self._get_image()}"

    def MakePullCommand(self):
        image = self._get_image()
        match self.runtime:
            case Runtime.APPTAINER:
                return f"{self.runtime.value} pull {self.GetLocalPath()} {image}"
            case Runtime.DOCKER:
                return f"{self.runtime.value} pull --platform=linux/amd64 {image}"
            case Runtime.MAMBA:
                return ""
            case _:
                return f"{self.runtime.value} pull {image}"

    def MakeBindsParam(self):
        if self.runtime == Runtime.MAMBA or self.native:
            return ""
        binds = {str(d):str(s) for s, d in self.container.binds}
        binds = [(s, d) for d, s in binds.items()]
        if len(binds)==0: return ""
        match self.runtime:
            case Runtime.DOCKER:
                binds = [f'--mount type=bind,source="{src}",target="{dst}"' for src, dst in binds]
                binds = " ".join(binds)
            case Runtime.APPTAINER:
                binds = [f'{src}:{dst}' for src, dst in binds]
                binds = f'--bind {",".join(binds)}'
            case _:
                raise TypeError(f"unsupported runtime [{self.runtime}]")
        return binds

    def MakeGpuArgs(self) -> list[str]:
        if self.native: return list(self.gpu_args)
        match self.runtime:
            case Runtime.DOCKER:
                base = ["--gpus", "all"]
            case Runtime.APPTAINER:
                base = ["--nv"]
            case _:
                base = []
        return base + list(self.gpu_args)

    def MakeRunCommand(self, local: bool|str = False, custom_bind_param: str|None=None):
        if self.native:
            return " ".join(str(x) for x in self.extra_args if x != "")
        image = self._get_image()
        if self.runtime == Runtime.MAMBA:
            toks = ["mamba", "run", "-n", image, *self.extra_args]
            return " ".join(str(x) for x in toks if x != "")
        binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
        match self.runtime:
            case Runtime.DOCKER:
                # `--rm` hands the container to dockerd, so a killed client
                # leaves nothing to find it by. The label is that handle:
                # `docker ps --filter label=msm.run=<token>`. Not `--name` --
                # a retry would collide with a predecessor still dying.
                others = ['--platform=linux/amd64', '--rm', '-u $(id -u):$(id -g)', '--network=host', '-e TMPDIR=${TMPDIR-"/tmp"}', f'--label {AgentPaths.RUN_LABEL}=${{{AgentPaths.RUN_TOKEN_ENV}:-}}', '--entrypoint=""']
                workdir = f'--workdir="{self.container.workdir}"' if self.container.workdir is not None else ''
                run = 'run'
            case Runtime.APPTAINER:
                nthreads = '${SLURM_CPUS_PER_TASK:-1}'
                # `--cleanenv` drops the run token, and the token is what a
                # reap scans /proc for -- so it is put back explicitly.
                others = ['--no-home', '--cleanenv', '--env TMPDIR=${TMPDIR-"/tmp"}', f'--env OPENBLAS_NUM_THREADS={nthreads}', f'--env OMP_NUM_THREADS={nthreads}', f'--env {AgentPaths.RUN_TOKEN_ENV}=${{{AgentPaths.RUN_TOKEN_ENV}:-}}']
                workdir = f'--pwd "{self.container.workdir}"' if self.container.workdir is not None else ''
                binds = custom_bind_param if custom_bind_param is not None else self.MakeBindsParam()
                if not isinstance(local, bool):
                    image = local
                elif local:
                    sif = self.GetLocalPath()
                    sandbox = self.GetSandboxPath()
                    match self.rootfs:
                        case Rootfs.SIF:
                            image = f'"{sif}"'
                        case Rootfs.SANDBOX:
                            image = f'"{sandbox}"'
                        case _:
                            image = f'"$(if [ -d "{sandbox}" ]; then echo "{sandbox}"; else echo "{sif}"; fi)"'
                run = 'exec'
            case _:
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


    @property
    def needs_relay(self) -> bool:
        return not self.native and self.runtime in _CONTAINER_RUNTIMES

    @staticmethod
    def Detect() -> "Runtime":
        import shutil
        if shutil.which("docker"):
            return Runtime.DOCKER
        if shutil.which("apptainer") or shutil.which("singularity"):
            return Runtime.APPTAINER
        return Runtime.DOCKER

    def MakeMaterialiseCommand(self, *, force: bool=False):
        if self.runtime == Runtime.DOCKER:
            image = self._get_image()
            return (
                f'{self.MakePullCommand()} || '
                f'docker image inspect "{image}" >/dev/null 2>&1 || '
                f'{{ echo "ERROR: could not pull [{image}] and no local copy is cached" >&2; false; }}'
            )
        sif, sandbox = self.GetLocalPath(), self.GetSandboxPath()
        if sif is None or sandbox is None: return ""
        sif_stamp, sandbox_stamp = self.GetLocalStampPath(), self.GetSandboxStampPath()
        verify_sif = self.MakeVerifyCommand()
        verify_sandbox = self.MakeVerifyCommand(sandbox=True)
        # "Already materialised" is the artifact *and* its stamp. An artifact
        # standing alone is one nothing has ever mounted -- which is the state
        # every image predating this check is in, and the state a corrupt pull
        # leaves behind. So each mode opens with a rung that adopts what is
        # already there: mount it once, stamp it if it holds, and let the verify
        # remove it if it does not, which drops through to a real fetch. That
        # costs one container start, never a re-download, for a store full of
        # sound images.
        done_sif = f'{{ [ -e {sif} ] && [ -e {sif_stamp} ]; }}'
        done_sandbox = f'{{ [ -d {sandbox} ] && [ -e {sandbox_stamp} ]; }}'
        adopt_sif = f'{{ [ -e {sif} ] && {verify_sif}; }}'
        adopt_sandbox = f'{{ [ -d {sandbox} ] && {verify_sandbox}; }}'
        prefix = f'mkdir -p "{sif.parent}"; ' + (
            f'rm -rf {sandbox} {sif} {sandbox_stamp} {sif_stamp}; ' if force else ''
        )
        match self.rootfs:
            case Rootfs.SANDBOX:
                return (
                    prefix
                    + f'{done_sandbox} '
                    f'|| {adopt_sandbox} '
                    f'|| {{ rm -rf {sandbox} {sandbox_stamp}; '
                    f'{self.MakeBuildSandboxCommand(from_image=True)} && {verify_sandbox}; }}'
                )
            case Rootfs.SIF:
                return (
                    prefix
                    + f'rm -rf {sandbox} {sandbox_stamp}; '
                    + f'{done_sif} '
                    f'|| {adopt_sif} '
                    f'|| {{ rm -f {sif} {sif_stamp}; {self.MakePullCommand()} && {verify_sif}; }} '
                    f'|| {{ rm -f {sif}; {self.MakeBuildSifCommand(no_fragments=True)} && {verify_sif}; }}'
                )
            case _:
                return (
                    prefix
                    + f'{done_sif} '
                    f'|| {done_sandbox} '
                    f'|| {adopt_sif} '
                    f'|| {adopt_sandbox} '
                    f'|| {{ rm -f {sif} {sif_stamp}; {self.MakePullCommand()} && {verify_sif}; }} '
                    f'|| {{ rm -f {sif}; {self.MakeBuildSifCommand(no_fragments=True)} && {verify_sif}; }} '
                    f'|| {{ rm -rf {sandbox} {sandbox_stamp}; '
                    f'{self.MakeBuildSandboxCommand(from_image=True)} && {verify_sandbox}; }}'
                )

    def ProvisionSteps(self, *, agent_home: Path, assertive: bool=False) -> list[tuple[str, str|None]]:
        steps: list[tuple[str, str|None]] = []
        if self.runtime == Runtime.DOCKER and not self.native:
            steps.append((
                self.MakeMaterialiseCommand(force=assertive),
                "{docker pull (refresh) -> fall back to existing local image if unreachable/local-only}",
            ))
            return steps
        if not self.GetLocalPath():
            return steps
        display = {
            Rootfs.SIF: "{rootfs=sif: pull sif -> build sif (-no-fragments), each mount-tested}",
            Rootfs.SANDBOX: "{rootfs=sandbox: unpack sandbox from registry, mount-tested}",
        }.get(self.rootfs, "{pull sif -> build sif (-no-fragments) -> unpack sandbox, each mount-tested}")
        steps.append((self.MakeMaterialiseCommand(force=assertive), display))
        return steps

    def RenderMsmWrapper(self, *, agent_home: Path, run_command: str, main_binds: str, dev_binds: str, dev_src: str) -> str:
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
            export AGENT_HOME={agent_home}
            export METASMITH_HOME_ROOT={agent_home}
            export METASMITH_WORK_ROOT={agent_home}
            {prefix}metasmith $@
            """

    def RenderBootstrap(self, *, agent_home: Path, run_command: str, run_binds: str, dev_src: str, dev_target: str, bind_file: str) -> str:
        if self.needs_relay:
            dev_binds = Environment(
                image=self.image,
                runtime=self.runtime,
                native=self.native,
                container=ContainerDef(binds=[
                    ("$DEV_BIND_SRC", Path(dev_target)),
                ]),
            ).MakeBindsParam()
            return f"""
                #!/bin/bash

                AGENT_HOME={agent_home}
                TASK_DIR=$1
                STEP=$2
                HOST_NAME=$3
                CWD=${{4:-$(pwd -P)}}
                cd $CWD
                if [ -e "{AgentPaths.CONTAINER_HOME_ROOT}" ]; then
                    echo "bootstrap called from container, bouncing to external [$@]"
                    REL_CWD=$(realpath --relative-to="{AgentPaths.CONTAINER_HOME_ROOT}" $CWD)
                    CMD="{AgentPaths.to_bootstrap(Path('$AGENT_HOME'))} $@ $AGENT_HOME/$REL_CWD"
                    /app/msm_relay.x86_64-linux --io {AgentPaths.to_relay(AgentPaths.CONTAINER_HOME_ROOT).parent}/$HOST_NAME bounce "$CMD"
                    exit
                fi

                echo "bootstrap ======================"
                INTERNALS="_metasmith"
                [ -z $STEP ] && echo "no step provided" && exit 1
                echo "cwd [$(pwd -P)]"
                echo "task [$TASK_DIR]"
                echo "step [$STEP]"
                # --- adaptive de-synchronization of the array fan-out ----------
                # Under SLURM array fan-out ~N tasks bootstrap at the same instant
                # and all read the shared Lustre agent home (dev overlay, container
                # cache, control-plane) at once -> the metadata storm that yields
                # errno 108 (ESHUTDOWN) + partial reads -> exit 127. Spread the
                # starts over a window sized to the array so the peak start rate
                # stays bounded (~1 start / 3s): a big fan-out smears across up to
                # ~5 min (unnoticeable at that job scale) while a small array barely
                # waits (efficiency). Skipped for non-array / single-task runs, and
                # opt-out via METASMITH_NO_START_JITTER=1. RANDOM (<=32767) covers
                # the capped window directly. This is belt-and-suspenders on top of
                # the per-node-once overlay staging below, and also de-syncs the
                # container-extract / control-plane reads that staging doesn't cover.
                if [ -z "${{METASMITH_NO_START_JITTER:-}}" ] && [ -n "${{SLURM_ARRAY_TASK_COUNT:-}}" ] && [ "$SLURM_ARRAY_TASK_COUNT" -gt 1 ]; then
                    _win=$(( SLURM_ARRAY_TASK_COUNT * 3 )); [ "$_win" -gt 300 ] && _win=300
                    _delay=$(( RANDOM % (_win + 1) ))
                    echo "start jitter: sleep ${{_delay}}s (window ${{_win}}s, array=$SLURM_ARRAY_TASK_COUNT)"
                    sleep "$_delay"
                fi
                # Exponential backoff with full jitter (bounded), for transient
                # errno-108 retries in the staging paths below. Efficient (near-zero
                # wait) when there is no contention; backs off dynamically when reads
                # actually fail. Arg: attempt number (1-based).
                msm_backoff() {{ _a="$1"; _b=$(( 1 << _a )); [ "$_b" -gt 60 ] && _b=60; sleep "$(( RANDOM % (_b + 1) ))"; }}
                function run_container {{
                    BINDS="{run_binds}"
                    if [ -e "{dev_src}" ]; then
                        echo "including dev binds"
                        # Node-local staging of the dev overlay before binding it.
                        # Under SLURM array fan-out up to ~array-size tasks land on
                        # ONE node; if each reads the shared Lustre overlay tree at
                        # once (import-time, or an rsync tree-walk of ~70 files) the
                        # metadata storm evicts the Lustre client with errno 108
                        # (ESHUTDOWN) and returns a SILENTLY-INCOMPLETE copy -> a
                        # submodule (e.g. models.workflow) vanishes ->
                        # ModuleNotFoundError -> exit 127 (reproduced: 100-way naive
                        # fan-out -> 93/97 incomplete, 558 errno-108). Two-layer fix:
                        # (1) deliver the overlay as a single tarball so the per-node
                        # Lustre read is ONE streaming file (what Lustre stays healthy
                        # under) instead of a readdir walk, the ~70 small-file writes
                        # land on node-local disk during `tar -x`, and a truncated
                        # archive fails `tar -x` LOUDLY instead of silently; (2)
                        # collapse the N per-node reads to ONE with an flock. The cache
                        # is keyed by the tarball's own stat (mtime+size) -- a single
                        # Every path fails open to the shared-tree bind, so staging is
                        # never worse than not doing this at all. The stamp is content
                        # keyed rather than job keyed, so a reused node never serves a
                        # stale overlay and there is no dependence on the array job id.
                        DEV_BIND_SRC="{dev_src}"
                        DEV_TARBALL="{dev_src}.tar"
                        if [ -e "$DEV_TARBALL" ] && [ -n "$SLURM_TMPDIR" ] && command -v flock >/dev/null 2>&1; then
                            STAGE_KEY=$(stat -c '%Y-%s' "$DEV_TARBALL" 2>/dev/null || echo nokey)
                            STAGE_BASE="/tmp/msm_devstage_${{USER:-$(id -un)}}"
                            STAGE_DIR="$STAGE_BASE/$STAGE_KEY"
                            NODE_DEV="$STAGE_DIR/metasmith"
                            STAMP="$STAGE_DIR/.msm_stage_ok"
                            mkdir -p "$STAGE_BASE"
                            # best-effort prune of other overlays' stale stages (bounded disk)
                            find "$STAGE_BASE" -maxdepth 1 -mindepth 1 ! -name "$STAGE_KEY" -mmin +120 -exec rm -rf {{}} + 2>/dev/null || true
                            (
                                exec 9>"$STAGE_DIR.lock" 2>/dev/null || exec 9>"$STAGE_BASE/$STAGE_KEY.lock"
                                if flock -w 300 9; then
                                    if [ ! -e "$STAMP" ]; then
                                        _t=0
                                        while [ "$_t" -lt 3 ]; do
                                            _t=$((_t+1))
                                            rm -rf "$NODE_DEV"; mkdir -p "$STAGE_DIR"
                                            _lt="$SLURM_TMPDIR/msm_overlay.$STAGE_KEY.tar"
                                            # native copy to node-local, then extract, then bind
                                            if cp -f "$DEV_TARBALL" "$_lt" 2>"$STAGE_DIR/.stage.err" \
                                                && tar -xf "$_lt" -C "$STAGE_DIR" 2>>"$STAGE_DIR/.stage.err"; then
                                                _n=$(find "$NODE_DEV" -type f 2>/dev/null | wc -l)
                                                if [ -e "$NODE_DEV/models/workflow" ] && [ -e "$NODE_DEV/coms" ] && [ "$_n" -ge 50 ]; then
                                                    rm -f "$_lt" 2>/dev/null || true
                                                    : > "$STAMP"; break
                                                fi
                                            fi
                                            rm -f "$_lt" 2>/dev/null || true
                                            echo "dev overlay stage attempt $_t incomplete; retrying" >&2
                                            msm_backoff "$_t"
                                        done
                                    fi
                                fi
                            )
                            if [ -e "$STAMP" ]; then
                                DEV_BIND_SRC="$NODE_DEV"
                                echo "staged dev overlay (per-node-once tarball, key $STAGE_KEY) -> [$NODE_DEV]"
                            else
                                rm -rf "$NODE_DEV" 2>/dev/null || true
                                echo "dev overlay tarball staging failed; using shared Lustre read"
                            fi
                        fi
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
                # The stop on the last line is only reached when the task exits
                # normally; a killed task would otherwise leave its own watcher
                # daemon running. Same class of problem as msm_phantom_guard.
                trap '$INTERNALS/relay/msm_relay stop >/dev/null 2>&1' EXIT INT TERM
                echo "stage control-plane ============"
                # Under SLURM array fan-out, copy the small shared control-plane
                # subset into this task's node-local scratch so N tasks don't all
                # read the same files through the /msm_home bind (errno 108). Runs
                # bare on the host, so it reads the real $AGENT_HOME (Lustre), never
                # /msm_home. Fail-open: any failure leaves STAGE_ROOT empty and the
                # task reads the shared copy exactly as before.
                STAGE_ROOT=""
                if [ -n "$SLURM_TMPDIR" ] && command -v rsync >/dev/null 2>&1; then
                    KEY=$(basename "$TASK_DIR")
                    HOST_STAGE="$(pwd -P)/$INTERNALS/stage"
                    # This subset is task-specific (the task dir), so per-node-once
                    # sharing does not apply as it does for the dev overlay; but the
                    # same Lustre concurrent-read eviction (errno 108) hits it, so
                    # retry the copy with backoff before giving up. Fail-open: on
                    # persistent failure leave STAGE_ROOT empty and read the shared
                    # copy exactly as before.
                    _c=0
                    while [ "$_c" -lt 3 ]; do
                        _c=$((_c+1))
                        if mkdir -p "$HOST_STAGE/lib" "$HOST_STAGE/runs/$KEY/$INTERNALS" "$HOST_STAGE/data" \
                            && rsync -a "$AGENT_HOME/lib/agent.yml" "$HOST_STAGE/lib/agent.yml" \
                            && rsync -a "$AGENT_HOME/runs/$KEY/$INTERNALS/task" "$HOST_STAGE/runs/$KEY/$INTERNALS/" \
                            && rsync -a --prune-empty-dirs --include='*/' --include='_metadata/***' --exclude='*' "$AGENT_HOME/data/" "$HOST_STAGE/data/"; then
                            STAGE_ROOT="/ws/$INTERNALS/stage"
                            echo "staged control-plane -> [$HOST_STAGE] (container view [$STAGE_ROOT])"
                            break
                        fi
                        echo "control-plane staging attempt $_c failed; retrying" >&2
                        STAGE_ROOT=""
                        msm_backoff "$_c"
                    done
                    [ -z "$STAGE_ROOT" ] && echo "control-plane staging failed; falling back to shared read"
                else
                    echo "no SLURM_TMPDIR or rsync; using shared control-plane read"
                fi
                echo "execute ========================"
                run_container metasmith api execute_transform -a step_index=$STEP -a workspace=$TASK_DIR -a stage_root=$STAGE_ROOT host=$(hostname)
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

            export AGENT_HOME={agent_home}
            TASK_DIR=$1
            STEP=$2
            HOST_NAME=$3
            CWD=${{4:-$(pwd -P)}}
            cd $CWD
            export METASMITH_HOME_ROOT={agent_home}
            export METASMITH_WORK_ROOT="$(pwd -P)"
            echo "bootstrap ======================"
            [ -z $STEP ] && echo "no step provided" && exit 1
            echo "cwd [$(pwd -P)]"
            echo "task [$TASK_DIR]"
            echo "step [$STEP]"
            {prefix}metasmith api execute_transform -a step_index=$STEP -a workspace=$TASK_DIR host=$(hostname)
            """

    def MakeWrapperPrefix(self) -> str:
        if self.native:
            return ""
        if self.runtime == Runtime.MAMBA:
            return f"mamba run -n {self._get_image()}"
        return ""

    def ConnectShell(self, server_path: Path|None=None, setup_commands: list[str]|None=None):
        if self.needs_relay:
            from ..coms.via_file_watcher import RemoteShell
            assert server_path is not None, "relay runtimes require a server path"
            return RemoteShell(server_path, timeout=60, setup_commands=setup_commands or [])
        return LiveShell()
