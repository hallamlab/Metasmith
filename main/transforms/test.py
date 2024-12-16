from pathlib import Path
from metasmith.models import ExecutionContext
from metasmith.transforms import LoadTransform

from local.constants import WORKSPACE_ROOT

ref = LoadTransform(WORKSPACE_ROOT/"lib/transforms/simple_1/fastal.py")
ref.protocol(ExecutionContext())
