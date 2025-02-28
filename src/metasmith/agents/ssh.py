import os
from pathlib import Path
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
import yaml
import time

from ..coms.ipc import LiveShell, ShellResult, RemoveLeadingIndent
from ..logging import Log
from ..coms.containers import Container, CONTAINER_RUNTIME

@dataclass
class Agent:
    ssh_command: str
    ssh_address: str
    pre: str
    home: Path
    container: str = "docker://quay.io/hallamlab/metasmith:latest"
    globus_endpoint: str = None

    def __post_init__(self):
        self.home = Path(self.home)
        self.ssh_command = RemoveLeadingIndent(self.ssh_command).strip()
        self.pre = RemoveLeadingIndent(self.pre).strip()
    
    def Pack(self):
        return dict(
            ssh_command=self.ssh_command,
            ssh_address=self.ssh_address,
            pre=self.pre,
            home=str(self.home),
            container=self.container,
            globus_endpoint=self.globus_endpoint,
        )
    
    def Save(self, file_path: Path):
        with open(file_path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Unpack(cls, data):
        return cls(**data)
    
    @classmethod
    def Load(cls, file_path: Path):
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        return cls.Unpack(data)

    def Deploy(self, force=False):
        with LiveShell() as shell, LiveShell() as local_shell, TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            shell.RegisterOnOut(Log.Info)
            shell.RegisterOnErr(Log.Error)
            local_shell.RegisterOnOut(Log.Info)
            local_shell.RegisterOnErr(Log.Error)
            def _step(cmd: str, timeout=15, get_errs=None):
                str_cmd = RemoveLeadingIndent(cmd)
                for x in str_cmd.split("\n"):
                    Log.Info(f">>> {x}")
                res = shell.Exec(cmd, timeout=timeout, history=True)
                if get_errs is not None:
                    errs = get_errs(res)
                    assert errs is None, errs

            def _ssh_errs(res: ShellResult):
                WL = {
                    "Pseudo-terminal will not be allocated because stdin is not a terminal."
                }
                errs = [e for e in res.err if e not in WL]
                if len(errs) > 0:
                    return "\n".join(errs)
                

            def _remote_file(x: str|Path, dest: Path, executable=False):
                Log.Info(f">>> deploying file [{dest}]")
                if isinstance(x, str):
                    x = RemoveLeadingIndent(x)
                    fpath = tmpdir/f"{dest.name}"
                    with open(fpath, "w") as f:
                        f.write(x)
                    if executable: os.chmod(fpath, 0o755)
                local_shell.Exec(f"rsync -avcp {fpath} {self.ssh_address}:{dest}")

            _step(self.ssh_command, get_errs=_ssh_errs)
            _step(self.pre)
            res = shell.Exec(f"""
                realpath {self.home}
                realpath ~
            """, history=True)
            resolved_msmhome, resolved_home = [Path(x.strip()) for x in res.out]
            res = shell.Exec(f'[ -d {resolved_msmhome}/dev/metasmith ] && echo "dev exists"', history=True)
            dev_exists = "dev exists" in res.out
            dev_binds = []
            if dev_exists:
                dev_binds = [
                    (resolved_msmhome/"dev/metasmith", Path("/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith")),
                ]
            container = Container(
                image=self.container,
                container_cache=resolved_msmhome, # just so the main container is saved here
                binds=[
                    (resolved_msmhome, Path("/msm_home")),
                    (Path(resolved_home)/".globus", Path(resolved_home)/".globus"),
                    (Path(resolved_home)/".globusonline", Path(resolved_home)/".globusonline"),
                ] + dev_binds,
                runtime=CONTAINER_RUNTIME.APPTAINER
            )
            _cmds = [f"mkdir -p {p}" for p, _ in container.binds]
            _step("\n".join(_cmds))
            _step(f"[ -e {container._get_local_path()} ] || {container.MakePullCommand()}", timeout=None)

            _remote_file(
                f"""
                #!/bin/bash
                {container.MakeRunCommand('', local=f"{resolved_msmhome}/metasmith.sif")} $@
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

            _step(f"cd {resolved_msmhome} && ./msm api deploy_from_container")
            _step(f"exit")

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
                ]+dev_binds,
                workdir=Path("/ws"),
                runtime=CONTAINER_RUNTIME.APPTAINER,
            )
            _remote_file(
                f"""
                #!/bin/bash
                cp {resolved_msmhome}/metasmith.sif ./
                mkdir -p ./.msm && cd ./.msm
                {bootstrap_container.MakeRunCommand('metasmith api deploy_from_container', local="../metasmith.sif")}
                cd ..
                echo "===== post deploy ====="
                find .
                ls -lh .
                ./.msm/relay/msm_relay start
                {bootstrap_container.MakeRunCommand('metasmith api execute_transform -a context=$1', local="./metasmith.sif")}
                echo "===== post run ====="
                find .
                ls -lh .
                ./.msm/relay/msm_relay stop
                sleep 1
                """,
                dest=resolved_msmhome/"lib/msm_bootstrap",
                executable=True,
            )

            HERE = Path(__file__).parent
            local_shell.Exec(f"rsync -avcp {HERE/'../nextflow_config'} {self.ssh_address}:{resolved_msmhome/'lib/'}")

# # ========================================
# # old

# CONTAINER = "docker://quay.io/hallamlab/metasmith:latest"
# CONTAINER_FILE = "metasmith.sif"

# @dataclass
# class DeploymentConfig:
#     ssh_command: str
#     timeout: int
#     workspace: str
#     pre: str = ""
#     post: str = ""
#     container: str = CONTAINER
#     container_args: list[str] = field(default_factory=list)

#     @classmethod
#     def Parse(cls, file_path):
#         try:
#             with open(file_path, "r") as f:
#                 config = cls(**yaml.safe_load(f))
#             return config
#         except FileNotFoundError as e:
#             Log.Error(f"config file [{file_path}] not found")
#         except TypeError as e:
#             emsg = str(e)
#             emsg = emsg.replace("DeploymentConfig.__init__()", "").strip()
#             to_replace = {
#                 "missing 1 required positional argument:": "missing field",
#                 "got an unexpected keyword argument": "unknown field",
#                 "'": "[",
#                 "'": "]",
#             }
#             for k, v in to_replace.items():
#                 emsg = emsg.replace(k, v, 1)
#             Log.Error(f"error with config [{file_path}]")
#             Log.Error(emsg)

# def Deploy(config_path: Path):
#     config = DeploymentConfig.Parse(config_path)
#     if config is None: 
#         Log.Error("config failed to parse")
#         return
#     workspace = Path(config.workspace).absolute()

#     with LiveShell() as shell:
#         SILENT = False
#         internal = workspace/"internal"
#         containers = workspace/"containers"
#         shell.Exec(config.ssh_command, timeout=config.timeout, silent=SILENT)
#         shell.Exec(config.pre, timeout=None, silent=SILENT)
#         shell.Exec(
#             f"""\
#             {config.pre}
#             [ -d {internal} ] || mkdir -p {internal}
#             [ -d {containers} ] || mkdir -p {containers}
#             cd {containers}
#             [ -f {CONTAINER_FILE} ] || apptainer pull {CONTAINER_FILE} {config.container}
#             """,
#             timeout=None, silent=SILENT
#         )
#         shell.Exec(
#             f"""\
#             cd {workspace}
#             apptainer run {" ".join(config.container_args)} {containers/CONTAINER_FILE} metasmith api unpack_container
#             nohup {internal}/relay --io {internal}/connections &
#             """,
#             timeout=None, silent=SILENT
#         )
#         shell.Exec(config.post, timeout=None, silent=SILENT)
#         shell.Exec("exit")

# def UnpackContainer():
#     SILENT = False
#     Log.Warn("pretend to unpack relay")
#     # with LiveShell() as shell:
#     #     shell.Exec(
#     #         f"""\
#     #         pwd
#     #         ls
#     #         cd ..
#     #         ls
#     #         """,
#     #         timeout=None, silent=SILENT
#     #     )

# def RunWorkflow():
#     Log.Warn("pretend to run job")
#     # with LiveShell() as shell:
#     #     shell.Exec(
#     #         f"""\
#     #         pwd
#     #         ls
#     #         cd ..
#     #         ls
#     #         """,
#     #         timeout=None, silent=SILENT
#     #    )

