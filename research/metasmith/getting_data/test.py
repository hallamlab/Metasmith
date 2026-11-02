from metasmith.data import GetDefaultManager
from metasmith.models import ArtifactPointer

config = dict(
    
)
dataManager = GetDefaultManager(config)

dataManager.Prepare([
    ArtifactPointer("globus", "https://app.globus.org/file-manager?origin_id=2602486c-1e0f-47a0-be15-eec1b0ff0f96&origin_path=%2F")
])
