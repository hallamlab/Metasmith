import itertools
import re
from typing import Iterable
import yaml
from pathlib import Path
from dataclasses import dataclass, field
import numpy as np

from .solver import WorkflowSolver
from .logging import Log
from .models.libraries import DataType

class Workspace:

    def __init__(self) -> None:
        pass

    def Execute(self, targets: Iterable[DataType]):
        pass
