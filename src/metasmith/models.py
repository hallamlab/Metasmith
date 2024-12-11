from dataclasses import dataclass, field

class NotImplementedException(Exception):
    pass

@dataclass
class ArtifactPointer:
    platform: str
    url: str

# abstract
class DataManager:
    def __init__(self, config: dict) -> None:
        pass

    def Prepare(self, manifest: list[ArtifactPointer]):
        raise NotImplementedException()
