from pathlib import Path

from ..logging import Log
from ..bootstrap import DeployFromContainer, StageAndRunTransform
from ..agents import ExecuteWorkflow, StageWorkflow

class Api:
    def deploy_from_container(self, body: dict):
        deploy_path = Path(body.get("workspace", "/ws"))
        if not deploy_path.exists():
            deploy_path = Path("./")
        DeployFromContainer(deploy_path)

    def execute_transform(self, body: dict):
        workspace = Path(body.get("workspace", "/ws")).resolve()
        step_index = body.get("step_index")
        assert step_index, "[step_index] is required"
        step_index = int(step_index)
        StageAndRunTransform(workspace, step_index)

    def stage_workflow(self, body: dict):
        task_key = body.get("task_key")
        assert task_key, "[task_key] is required"
        StageWorkflow(task_key)

    def execute_workflow(self, body: dict):
        key = body.get("key")
        assert key, "[key] is required"
        ExecuteWorkflow(key)

_ENDPOINTS = {k:v for k, v in Api.__dict__.items() if k[0]!="_"} 
def HandleRequest(endpoint: str, body: dict):
    endpoint = endpoint.lower()
    if endpoint not in _ENDPOINTS:
        Log.Error(f"endpoint [{endpoint}] does not exist")
        return
    api = Api()
    Log.Info(f"api call to [{endpoint}] with [{body}]")
    _ENDPOINTS[endpoint](api, body)
