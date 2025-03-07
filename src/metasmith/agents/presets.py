import os
from pathlib import Path
from dataclasses import dataclass, field
from socket import timeout
import tempfile
import shutil
from typing import Iterable
import yaml
import time

from ..hashing import KeyGenerator
from ..coms.ipc import LiveShell, ShellResult, RemoveLeadingIndent
from ..logging import Log
from ..coms.containers import Container, CONTAINER_RUNTIME
from ..models.remote import Logistics, Source

AGENT_SETUP_COMPLETE = f"setup_complete.{KeyGenerator.FromInt(2**42)}"

@dataclass
class Agent:
    setup_commands: Iterable[str] # must echo AGENT_SETUP_COMPLETE to indicate sucess
    cleanup_commands: Iterable[str]
    home: Source
    container: str = "docker://quay.io/hallamlab/metasmith:latest"

    def Pack(self):
        return dict(
            setup_commands=list(self.setup_commands),
            cleanup_commands=list(self.cleanup_commands),
            home=self.home.Pack(),
            container=self.container,
        )
    
    def Save(self, file_path: Path):
        with open(file_path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Unpack(cls, data):
        data["home"] = Source.Unpack(data["home"])
        return cls(**data)
    
    @classmethod
    def Load(cls, file_path: Path):
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        return cls.Unpack(data)

    def RunSetup(self, shell: LiveShell, timeout: int = None):
        for cmd in self.setup_commands:
            res = shell.Exec(cmd, timeout=timeout, history=True)
            if any(AGENT_SETUP_COMPLETE in x for x in res.out): return
        assert False, f"Setup commands failed, since AGENT_SETUP_COMPLETE was not detected [{AGENT_SETUP_COMPLETE}]"
    
    def RunCleanup(self, shell: LiveShell):
        for cmd in self.cleanup_commands:
            shell.Exec(cmd)

    def Deploy(self):
        with LiveShell() as shell, tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            shell.RegisterOnOut(Log.Info)
            shell.RegisterOnErr(Log.Error)
            def do_step(cmd: str, timeout=15):
                str_cmd = RemoveLeadingIndent(cmd)
                for x in str_cmd.split("\n"):
                    Log.Info(f">>> {x}")
                return shell.Exec(cmd, timeout=timeout, history=True)

            def _remote_file(x: str|Path, dest: Path, executable=False):
                if isinstance(x, str):
                    x = RemoveLeadingIndent(x)
                    fpath = tmpdir/f"{dest.name}"
                    with open(fpath, "w") as f:
                        f.write(x)
                    if executable: os.chmod(fpath, 0o755)
                else:
                    fpath = x
                mover = Logistics()
                mover.QueueTransfer(
                    src=Source.FromLocal(fpath),
                    dest=self.home.WithPath(dest)
                )
                Log.Info(f">>> deploying file [{dest}]")
                res = mover.ExecuteTransfers()
                assert len(res.completed) == 1, "Failed to deploy file"

            self.RunSetup(shell)
            res = shell.Exec(f"""
                realpath {self.home.GetPath()}
                realpath ~
            """, history=True)
            resolved_msmhome, resolved_home = [Path(x.strip()) for x in res.out]

            dev_src = resolved_msmhome/"dev/metasmith"
            def make_dev_container(c: Container):
                return Container(
                    image=c.image,
                    binds=c.binds+[
                        (dev_src, Path("/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith")),
                    ],
                    workdir=c.workdir,
                    runtime=c.runtime,
                )
            container = Container(
                image=self.container,
                container_cache=resolved_msmhome, # just so the main container is saved here
                binds=[
                    (resolved_msmhome, Path("/msm_home")),
                    (Path(resolved_home)/".globus", Path(resolved_home)/".globus"),
                    (Path(resolved_home)/".globusonline", Path(resolved_home)/".globusonline"),
                ],
                runtime=CONTAINER_RUNTIME.APPTAINER
            )
            container_dev = make_dev_container(container)
            _cmds = [f"mkdir -p {p}" for p, _ in container.binds]
            do_step("\n".join(_cmds))
            do_step(f"[ -e {container._get_local_path()} ] || {container.MakePullCommand()}", timeout=None)

            _remote_file(
                f"""
                #!/bin/bash
                if [ -e "{dev_src}" ]; then
                    echo "including dev binds"
                    {container_dev.MakeRunCommand(local=f"{resolved_msmhome}/metasmith.sif")} $@
                else
                    {container.MakeRunCommand(local=f"{resolved_msmhome}/metasmith.sif")} $@
                fi
                """,
                dest=resolved_msmhome/"msm_stub",
                executable=True,
            )

            HERE='$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )'
            _remote_file(
                f"""
                #!/bin/bash
                HERE={HERE}
                $HERE/msm_stub metasmith $@
                """,
                dest=resolved_msmhome/"msm",
                executable=True,
            )

            do_step(f"cd {resolved_msmhome} && ./msm api deploy_from_container")
            do_step(f"exit")

            _remote_file(
                yaml.dump(self.Pack()),
                dest=resolved_msmhome/"lib/agent.yml",
            )

            bootstrap_container = Container(
                image=self.container,
                binds=[
                    (Path("./.msm"), Path("/msm_home")),
                    (Path("./"), Path("/ws")),
                    (resolved_msmhome, Path("/agent_home")),
                ],
                workdir=Path("/ws"),
                runtime=CONTAINER_RUNTIME.APPTAINER,
            )
            bootstrap_container_dev = make_dev_container(bootstrap_container)
            _remote_file(
                f"""
                #!/bin/bash
                function run_container {{
                    if [ -e "{dev_src}" ]; then
                        echo "including dev binds"
                        {bootstrap_container_dev.MakeRunCommand(local="./metasmith.sif")} $@
                    else
                        {bootstrap_container.MakeRunCommand(local="./metasmith.sif")} $@
                    fi
                }}

                echo "get container =================="
                cp {resolved_msmhome}/metasmith.sif ./
                mkdir -p ./.msm
                echo "deploy ========================="
                run_container metasmith api deploy_from_container -a workspace=./.msm
                echo "post deploy ===================="
                find .
                ls -lh .
                echo "relay =========================="
                ./.msm/relay/msm_relay start
                echo "execute ========================"
                run_container metasmith api execute_transform -a step_index=$1
                echo "post execute ==================="
                find .
                ls -lh .
                echo "exit ==========================="
                ./.msm/relay/msm_relay stop
                sleep 1
                """,
                dest=resolved_msmhome/"lib/msm_bootstrap",
                executable=True,
            )

            HERE = Path(__file__).parent
            _remote_file(HERE/"../nextflow_config", resolved_msmhome/"lib/nextflow_config")
            self.RunCleanup(shell)
