from .models import DataManager, DataManagerConfig
from .dataManagers.globus import GlobusDataManager

def GetDefaultManager(config: DataManagerConfig) -> DataManager:
    return GlobusDataManager(config)
