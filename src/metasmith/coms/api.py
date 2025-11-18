from pathlib import Path

from ..logging import Log
from ..bootstrap import DeployFromContainer, StageAndRunTransform
from ..agents import RunWorkflow, StageWorkflow, CheckWorkflow

class Api:
    def deploy_from_container(self, body: dict):
        deploy_path = Path(body.get("workspace", "/ws"))
        DeployFromContainer(deploy_path)

    def execute_transform(self, body: dict):
        workspace = body.get("workspace")
        assert workspace, "[workspace] is required"
        step_index = body.get("step_index")
        assert step_index, "[step_index] is required"
        sample, step = [int(x) for x in step_index.split("/")]
        res = StageAndRunTransform(Path(workspace), sample, step)
        exit(res.success)

    def stage_workflow(self, body: dict):
        task_key = body.get("task_key")
        assert task_key, "[task_key] is required"
        verify = body.get("verify", "False").title()=="True"
        StageWorkflow(task_key, verify)

    def run_workflow(self, body: dict):
        key = body.get("key")
        assert key, "[key] is required"
        log_dir = body.get("log_dir")
        assert log_dir, "[log_dir] is required"
        RunWorkflow(key, Path(log_dir))

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
