from ..logging import Log
from ..agents.ssh import UnpackContainer 

class Api:
    def unpack_container(self, body: dict):
        UnpackContainer()

_ENDPOINTS = {k:v for k, v in Api.__dict__.items() if k[0]!="_"} 
def HandleRequest(endpoint: str, body: dict):
    endpoint = endpoint.lower()
    if endpoint not in endpoint:
        Log.Error(f"endpoint [{endpoint}] does not exist")
        return
    api = Api()
    Log.Info(f"api call to [{endpoint}]")
    _ENDPOINTS[endpoint](api, body)
