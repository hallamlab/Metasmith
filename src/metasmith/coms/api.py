from pathlib import Path

from ..logging import Log
from ..agents.bootstrap import DeployFromContainer, StageAndRunTransform

class Api:
    def deploy_from_container(self, body: dict):
        deploy_path = Path(body.get("workspace", "/ws"))
        if not deploy_path.exists():
            deploy_path = Path("./")
        DeployFromContainer(deploy_path)

    def execute_transform(self, body: dict):
        workspace = Path(body.get("workspace", "/ws")).resolve()
        context = body.get("context")
        assert context, "context is required"
        StageAndRunTransform(workspace, Path(context))

_ENDPOINTS = {k:v for k, v in Api.__dict__.items() if k[0]!="_"} 
def HandleRequest(endpoint: str, body: dict):
    endpoint = endpoint.lower()
    if endpoint not in endpoint:
        Log.Error(f"endpoint [{endpoint}] does not exist")
        return
    api = Api()
    Log.Info(f"api call to [{endpoint}]")
    _ENDPOINTS[endpoint](api, body)
