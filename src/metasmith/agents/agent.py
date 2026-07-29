"""A metasmith installation on some other host, and how to put one there.

`Agent` is a client-side handle: a home directory reachable over ssh or the
local filesystem, an environment to run in, and the deploy that makes the two
true. Everything past deploy -- generating, staging, running, watching -- is
mixed in from `workflow_ops` and `run_control`, which keeps this module about
identity and provisioning.

`Deploy` is the largest thing here and deliberately so: it is the one operation
with no idempotent shortcut, and reading it end to end is how one learns what an
agent actually needs on the far side.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml

from ..constants import AgentPaths, CONTAINER_TAG, MODULE_PATH, VERSION
from ..coms.terminals import (
    IDLE_TIMEOUT, LiveShell, PROBE_TIMEOUT, RemoveLeadingIndent,
    SSH_CONNECT_TIMEOUT,
)
from ..env import ContainerDef, Environment, Runtime
from ..hashing import KeyGenerator
from ..logging import Log
from ..models.libraries import DataTypeLibrary, TransformInstanceLibrary
from ..models.remote import GlobusSource, Logistics, Source, SourceType, SshSource
from ..models.solver import Endpoint, Solution
from ..models.workflow import BIND_FILE
from .run_control import _RunControl
from .shell import AgentShell
from .workflow_ops import _WorkflowOps



@dataclass
class Agent(_WorkflowOps, _RunControl):
    home: Source
    # Stable identity, independent of anything a person types. The default home
    # is built from this, not from the agent's display name, so renaming an
    # agent can never relocate the directory it already has on disk. Set once,
    # at construction, and never reassigned by any save thereafter.
    id: str = field(default_factory=lambda: KeyGenerator().GenerateUID(l=8))
    setup_commands: list[str] = field(default_factory=list)
    container: str = f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"
    globus_uuid: str|None = None
    runtime: Runtime=Runtime.APPTAINER
    native: bool = False
    # Extra flags this host needs to expose its GPUs to a tool, appended after
    # the runtime's own switch. Empty on a normal Linux box; WSL2 needs
    # ["--bind", "/usr/lib/wsl:/usr/lib/wsl", "--env",
    #  "LD_LIBRARY_PATH=/usr/lib/wsl/lib"] because apptainer's `--nv` discovery
    # misses the WSL driver stack. A host fact, so it is declared here rather
    # than sniffed at run time.
    gpu_args: list[str] = field(default_factory=list)
    # Which nextflow config preset a run on this agent uses when the caller does
    # not name one. `None` means the built-in `local`, which is the right answer
    # for a workstation and the wrong one for every scheduler -- and the caller
    # that most often names nothing is a person clicking launch, who has no way
    # to know their cluster needs `slurm`. A property of the machine, so it is
    # declared on the machine.
    default_preset: str|None = None
    # Params every run on this agent starts from, layered under whatever the run
    # itself names. The preset above says *how* to submit; this is where the
    # facts that preset needs -- the cluster account, the partition -- come from,
    # and they are properties of the machine for the same reason.
    default_params: dict = field(default_factory=dict)
    real_path: Path|None = None

    def _environment(self) -> Environment:
        # The agent's own Environment — how metasmith itself runs on the host.
        return Environment(image=self.container, runtime=self.runtime, native=self.native)

    def _is_ssh(self):
        return self.home.type == SourceType.SSH

    def Pack(self) -> dict:
        optional = {k:str(v) for k, v in dict(
            globus_uuid=self.globus_uuid,
            default_preset=self.default_preset,
            real_path=self.real_path,
        ).items() if v is not None}
        # Packed outside `optional`, which stringifies everything it writes --
        # right for the three strings above, and silently fatal for a mapping,
        # which would reload as a quoted Python literal and produce no params at
        # all. Omitted entirely when empty, so an agent that sets none keeps a
        # file identical to the one it had before this field existed.
        if self.default_params:
            optional["default_params"] = dict(self.default_params)
        return dict(
            id=self.id,
            setup_commands=list(self.setup_commands),
            home=self.home.Pack(),
            container=self.container,
            runtime=self.runtime.name,
            native=self.native,
            gpu_args=list(self.gpu_args),
        ) | optional

    def Save(self, file_path: Path):
        with open(file_path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Unpack(cls, data):
        data["home"] = Source.Unpack(data["home"])
        data["runtime"] = Runtime[data["runtime"]]
        # `native` is newer than the original agent.yml format; legacy files
        # omit it and default to a wrapped (non-native) environment.
        data.setdefault("native", False)
        data.setdefault("gpu_args", [])
        # newer again; an agent file that never set one names no preset, and
        # `RunWorkflow` falls back to `local` exactly as it always did
        data.setdefault("default_preset", None)
        data.setdefault("default_params", {})
        k = "real_path"
        if k in data:
            data[k] = Path(data[k])
        return cls(**data)

    @classmethod
    def Load(cls, file_path: Path):
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        agent = cls.Unpack(data)
        # A legacy file has no `id` at all -- `Unpack`'s `cls(**data)` still
        # produces one, via the dataclass default, but a fresh random one on
        # every read is not an identity. Persist it the first time so it is
        # the same id on every read after this one.
        if "id" not in data:
            agent.Save(file_path)
        return agent
    
    def _get_realpath(self):
        """realpath is resolved upon deployment"""
        assert self.real_path is not None, "not resolved"
        return self.real_path

    def _run_setup(self, shell: LiveShell, timeout: int|None = None):
        if self._is_ssh():
            ssh_src = SshSource.Parse(self.home.address)
            Log.Info(f"starting ssh to [{ssh_src.host}]")
            # ConnectTimeout rather than a shell bound: this Exec deliberately
            # inherits stdin so a key passphrase can be typed, and a bound here
            # would cut that off. ConnectTimeout bounds only the handshake, so
            # a host that accepts nothing fails in seconds instead of holding
            # the calling thread for the life of the process.
            shell.Exec(
                f"ssh -o ConnectTimeout={SSH_CONNECT_TIMEOUT:g} {ssh_src.host}",
                inherit_stdin=True,
            )
            SUCCESS = f"ssh_connected_flag.{KeyGenerator.FromInt(2**42)}"
            def on_out(x):
                if SUCCESS in x: return
                Log.Info(f"{x}")
            def on_err(x):
                Log.Error(f"{x}")
            shell.RegisterOnOut(on_out)
            shell.RegisterOnErr(on_err)
            res = shell.Exec(
                f'[ ! -z "$SSH_CONNECTION" ] && echo "{SUCCESS}"',
                timeout=timeout, history=True,
                idle_timeout=PROBE_TIMEOUT, what=f"ssh probe on [{ssh_src.host}]",
            )
            shell.RemoveOnOut(on_out)
            shell.RemoveOnErr(on_err)
            if not any(SUCCESS in x for x in res.out):
                assert False, f"ssh connection failed {res.err}"

        for cmd in self.setup_commands:
            shell.Exec(
                cmd, timeout=timeout,
                idle_timeout=IDLE_TIMEOUT, what="agent setup command",
            )

    def _run_cleanup(self, shell: LiveShell):
        pass

    def Deploy(self, assertive: bool=False, runtime: Runtime|None=None, image: str|None=None, native: bool|None=None):
        # Deploy is the entry point where the runtime is chosen and then
        # persisted into agent.yml; everything downstream reads it back
        # transparently. Passing nothing keeps the agent's current runtime.
        # `native=True` selects the orthogonal "already inside, no wrapper"
        # mode (not a runtime — composes with one).
        if runtime is not None:
            self.runtime = runtime
        if image is not None:
            self.container = image
        if native is not None:
            self.native = native
        Log.Info(f"deploying agent version [{VERSION}] to [{self.home.address}] using runtime [{self.runtime.name}] (native={self.native})")
        with LiveShell() as shell, tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            _quiet = False
            shell.RegisterOnOut(lambda x: (Log.Info(x) if not _quiet else None))
            shell.RegisterOnErr(lambda x: (Log.Error(x) if not _quiet else None))

            def do_step(cmd: str, display_cmd: str|None=None, timeout:float|None=None):
                if display_cmd is not None: Log.Info(f">>> {display_cmd}")
                str_cmd = RemoveLeadingIndent(cmd)
                for x in str_cmd.split("\n"):
                    if display_cmd is None: Log.Info(f">>> {x}")
                return shell.Exec(cmd, timeout=timeout, history=True)

            _staged = []
            def _remote_file(x: str|Path, dest: str|Path, executable=False):
                if not isinstance(dest, Path): dest = Path(dest)
                assert not dest.is_absolute() or dest.is_relative_to(self.home.GetPath()), f"dest [{dest}] must be relative to [{self.home.GetPath()}]"
                (tmpdir/dest).parent.mkdir(parents=True, exist_ok=True)
                if isinstance(x, str):
                    x = RemoveLeadingIndent(x)
                    fpath = tmpdir/dest
                    with open(fpath, "w") as f:
                        f.write(x)
                    if executable: os.chmod(fpath, 0o755)
                else:
                    shutil.copytree(x, tmpdir/dest)
                _staged.append(dest)
                Log.Info(f"staged [{dest}]")

            def _sync_remote_files():
                mover = Logistics()
                mover.QueueTransfer(
                    src=Source.FromLocal(tmpdir),
                    dest=self.home,
                )
                Log.Info(f"deploying [{len(_staged)}] staged files")
                res = mover.ExecuteTransfers()
                for e in res.errors:
                    Log.Error(e)
                assert len(res.completed) == 1, f"failed to deploy files"

            _quiet = True
            self._run_setup(shell)
            _quiet = False

            shell.Exec(f'mkdir -p "{self.home.GetPath()}"')
            _quiet = True
            cmds = [
                f'realpath {self.home.GetPath()}',
                f'realpath ~',
                f'hostname',
            ]
            res = shell.Exec('\n'.join(cmds), history=True)
            _quiet = False
            assert len(res.out)==len(cmds), res.out
            resolved_agent_home, resolved_home, hostname = [x.strip() for x in res.out]
            resolved_agent_home = Path(resolved_agent_home)
            resolved_home = Path(resolved_home)

            dev_src = "$AGENT_HOME/dev/metasmith"
            dev_target = "/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith"
            dev_mock = Environment(
                image=self.container,
                runtime=self.runtime,
                native=self.native,
                container=ContainerDef(binds=[
                    (dev_src, Path(dev_target)),
                ]),
            )
            # The bootstrap may stage the dev overlay to node-local scratch
            # before binding it (SLURM array fan-out), so it binds whatever
            # $DEV_BIND_SRC resolves to at run time rather than dev_src.
            dev_mock_staged = Environment(
                image=self.container,
                runtime=self.runtime,
                native=self.native,
                container=ContainerDef(binds=[
                    ("$DEV_BIND_SRC", Path(dev_target)),
                ]),
            )

            container = Environment(
                image=self.container,
                runtime=self.runtime,
                native=self.native,
                container=ContainerDef(
                    cache=Path("$AGENT_HOME")/AgentPaths.CONTAINER_CACHE,
                    binds=[
                        ("$(pwd -P)", Path("/ws")),
                        ("$AGENT_HOME", Path("/msm_home")),
                        ("$AGENT_HOME", Path(str(resolved_agent_home))),
                        ('${TMPDIR-"/tmp"}', '${TMPDIR-"/tmp"}'),
                        (resolved_home/".globus", resolved_home/".globus"),
                        (resolved_home/".globusonline", resolved_home/".globusonline"),
                    ],
                ),
            )
            _cmds = [
                f"AGENT_HOME={resolved_agent_home}"
            ] + [
                f"mkdir -p {p}" for p, _ in container.container.binds
            ]
            do_step("\n".join(_cmds))
            # Per-host provisioning (image pull + SIF/sandbox decision) is
            # owned by the Environment so Deploy never branches on a runtime.
            # Empty for runtimes with nothing to pull (mamba/native).
            for _cmd, _display_cmd in container.ProvisionSteps(agent_home=resolved_agent_home, assertive=assertive):
                do_step(cmd=_cmd, display_cmd=_display_cmd)

            _remote_file(
                container.RenderMsmWrapper(
                    agent_home=resolved_agent_home,
                    run_command=container.MakeRunCommand(local=True, custom_bind_param="$BINDS"),
                    main_binds=container.MakeBindsParam(),
                    dev_binds=dev_mock.MakeBindsParam(),
                    dev_src=dev_src,
                ),
                dest="msm",
                executable=True,
            )

            # What the home actually resolved to on the host, kept on *this*
            # agent as well as on the copy that goes with it. The field's whole
            # description is "resolved upon deployment", and only the remote
            # copy was ever getting it -- so a caller that saved the agent after
            # deploying still had `None`, which reads as "never deployed".
            # `save_agent` clears it again the moment the home is re-pointed,
            # which is what keeps the two meanings from drifting.
            self.real_path = resolved_agent_home
            _remote_copy = Agent.Unpack(self.Pack())
            _remote_copy.home = Source.FromLocal(resolved_agent_home)
            _remote_copy.real_path = resolved_agent_home
            _remote_file(
                yaml.dump(_remote_copy.Pack()),
                dest=AgentPaths.to_definition(Path(".")),
            )

            bootstrap_container = Environment(
                image=self.container,
                runtime=self.runtime,
                native=self.native,
                container=ContainerDef(
                    cache=Path("$AGENT_HOME")/AgentPaths.CONTAINER_CACHE,
                    workdir=Path("/ws"),
                    binds=[
                        ("$(pwd -P)", Path("/ws")),
                        ("$AGENT_HOME", Path("/msm_home")),
                    ],
                ),
            )
            _remote_file(
                bootstrap_container.RenderBootstrap(
                    agent_home=resolved_agent_home,
                    run_command=bootstrap_container.MakeRunCommand(local=True, custom_bind_param="$BINDS"),
                    run_binds=bootstrap_container.MakeBindsParam(),
                    dev_src=dev_src,
                    dev_target=dev_target,
                    bind_file=BIND_FILE,
                ),
                dest=AgentPaths.to_bootstrap(Path(".")),
                executable=True,
            )

            _sync_remote_files()
            # Container extraction is the one truly expensive step left;
            # everything else above is either a no-op (rsync -au on unchanged
            # files) or self-gated ([ -e {sif} ] for the container pull).
            # Skip extraction only when its actual output already exists, so
            # a partial deploy (sif present, relay missing) self-heals on the
            # next call without needing assertive=True.
            relay_bin = AgentPaths.to_relay(self.home.GetPath())
            if not container.needs_relay:
                # The relay exists solely to bounce tool launches back across a
                # container boundary. mamba/native have no boundary, and the
                # extraction step reads from inside the metasmith container --
                # which is not running. Nothing to deploy.
                Log.Info(f"runtime [{self.runtime.name}] needs no relay, skipping container extraction")
            elif "relay-present" in shell.Exec(
                    f'[[ -e "{relay_bin}" ]] && echo "relay-present"', history=True).out and not assertive:
                Log.Info(f"relay binary present at [{relay_bin}], skipping container extraction")
            else:
                # Runs *inside* the metasmith container via the msm wrapper, so
                # the workspace is the container's own view of the agent home.
                do_step(f"{resolved_agent_home}/msm api deploy_from_container -a workspace={AgentPaths.CONTAINER_HOME_ROOT} architecture=$(uname -m) system=$(uname -s)")
                res = shell.Exec(f'[[ -e "{relay_bin}" ]] && echo "relay-deployed"', history=True)
                assert "relay-deployed" in res.out, f"deploy_from_container completed but relay binary missing at [{relay_bin}]"
            self._run_cleanup(shell)
            Log.Info(f"deployed to [{self.home.address}]")



