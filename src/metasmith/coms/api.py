from pathlib import Path

from ..logging import Log
from ..bootstrap import DeployFromContainer, StageAndRunTransform
from ..agents import RunWorkflow, StageWorkflow, CheckWorkflow
from ..env import Rootfs

class Api:
    def deploy_from_container(self, body: dict):
        deploy_path = Path(body.get("workspace", "/ws"))
        architecture = body.get("architecture")
        assert architecture, "[architecture] is required"
        system = body.get("system")
        assert system, "[system] is required"
        DeployFromContainer(deploy_path, architecture, system)

    def execute_transform(self, body: dict):
        workspace = body.get("workspace")
        assert workspace, "[workspace] is required"
        step_index = body.get("step_index")
        assert step_index, "[step_index] is required"
        host = body.get("host")
        assert host, "[host] is required"
        stage_root = body.get("stage_root") or None
        res = StageAndRunTransform(
            Path(workspace), int(step_index), host,
            stage_root=Path(stage_root) if stage_root else None,
        )
        exit(0 if res.success else 1)

    def stage_workflow(self, body: dict):
        task_key = body.get("task_key")
        assert task_key, "[task_key] is required"
        verify = body.get("verify", "False").strip().title()=="True"
        host = body.get("host")
        assert host, "[host] is required"
        rootfs = body.get("rootfs")
        rootfs = Rootfs.Parse(rootfs) if rootfs else None
        StageWorkflow(task_key, verify, host, rootfs=rootfs)

    def run_workflow(self, body: dict):
        key = body.get("key")
        assert key, "[key] is required"
        log_dir = body.get("log_dir")
        assert log_dir, "[log_dir] is required"
        host = body.get("host")
        assert host, "[host] is required"
        stub_delay = body.get("stub_delay", "0").strip()
        try:
            stub_delay = float(stub_delay)
        except:
            stub_delay = 0
        RunWorkflow(key, Path(log_dir), host, stub_delay)

    def check_workflow(self, body: dict):
        key = body.get("key")
        assert key, "[key] is required"
        index = body.get("index")
        if index is not None:
            try:
                index = int(index)
            except:
                Log.Error(f"invalid [index] value [{index}]")
                return
        CheckWorkflow(key, index)

_ENDPOINTS = {k:v for k, v in Api.__dict__.items() if k[0]!="_"}
def HandleRequest(endpoint: str, body: dict):
    endpoint = endpoint.lower()
    if endpoint not in _ENDPOINTS:
        Log.Error(f"endpoint [{endpoint}] does not exist")
        return
    api = Api()
    Log.Info(f"api call to [{endpoint}] with [{body}]")
    _ENDPOINTS[endpoint](api, body)
