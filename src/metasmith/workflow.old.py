import itertools
import re
import yaml
from pathlib import Path
from dataclasses import dataclass, field
import numpy as np

from metasmith.constants import Workspace

from .models import TransformReference
from .solver import Namespace, Transform, Endpoint, Solve
from .transforms import LoadTransform
from .logging import Log

class YamlData:
    @classmethod
    def Parse(cls, file_path):
        try:
            with open(file_path, "r") as f:
                config = cls(**yaml.safe_load(f))
            return config
        except FileNotFoundError as e:
            Log.Error(f"request not found [{file_path}]")
        except TypeError as e:
            emsg = str(e)
            cls_name = cls[len("<class '"):-len("'>")]
            emsg = emsg.replace(f"{cls_name}.__init__()", "").strip()
            to_replace = {
                "missing 1 required positional argument:": "missing field",
                "got an unexpected keyword argument": "unknown field",
                "'": "[",
                "'": "]",
            }
            for k, v in to_replace.items():
                emsg = emsg.replace(k, v, 1)
            Log.Error(f"error with request [{file_path}]")
            Log.Error(emsg)

@dataclass
class WorkflowRequest(YamlData):
    given: dict
    targets: dict
    namespaces: list[str] = field(default_factory=list)

@dataclass
class Prototypes(YamlData):
    schema: str
    ontology: dict
    prototypes: dict

class Workspace:
    def __init__(self, home: Path) -> None:
        self.TRANSFORMS = home/"metasmith_definitions/transforms"
        pass

def GeneratePlan(home: Path, request_file: Path):
    request = WorkflowRequest.Parse(request_file)

    prototypes_dir = home/"metasmith_definitions/prototypes"
    prototypes = {}
    for f in prototypes_dir.iterdir():
        if f.is_dir(): continue
        if f.suffix not in {".yaml", ".yml"}: continue
        prototype = Prototypes.Parse(f)
        path = ".".join(f"{f.relative_to(prototypes_dir)}".split(".")[:-1])
        prototypes[path] = prototype.prototypes
    if len(prototypes) == 0:
        Log.Error(f"no prototypes found in [{prototypes_dir}]")
        return    
    
    for ns in request.namespaces:
        if ns not in prototypes:
            Log.Error(f"unknown namespace [{ns}]")
            return
    requested_namespaces = {ns:prototypes[ns] for ns in request.namespaces}

    def get_dtype(name: str):
        if ":" in name:
            ns, name = [v.strip() for v in name.split(":")]
            if ns not in prototypes:
                Log.Error(f"unknown namespace [{ns}]")
                return
            if name not in prototypes[ns]:
                Log.Error(f"unknown prototype [{name}] in namespace [{ns}]")
                return
            raw = prototypes[ns][name]
        else:
            for ns, protos in requested_namespaces.items():
                if name not in protos: continue
                raw = protos[name]
                break
            else:
                Log.Error(f"unknown prototype [{name}]")
                return
        properties = {f"{k}={v}" for k, v in raw.items()}
        return properties        

    SEED = np.random.randint(0, 2**32)
    NS = Namespace(key_length=3, seed=SEED)

    loaded_dtypes = {}
    given: list[Endpoint] = []
    for tk, tv in itertools.chain(request.targets.items(), request.given.items()):
        base, additional_properties = [tv.get(k) for k in ["base", "properties"]]
        base_properties = get_dtype(base) if base is not None else set()
        if base_properties is None: return # error has ocurred

        if additional_properties is not None:
            properties = base_properties|{f"{k}={v}" for k, v in additional_properties.items()}
        else:
            properties = base_properties
        if len(properties) == 0:
            Log.Error(f"target had no properties [{tk}]")
            return
        loaded_dtypes[tk] = properties
        given.append(Endpoint(NS, properties))

    loaded_requirements = {}
    target = Transform(NS)
    for tk, properties in loaded_dtypes.items():
        r = target.AddRequirement(properties)
        loaded_requirements[tk] = r

    for tk, tv in request.targets.items():
        parents = tv.get("from", [])
        parent_requirements = []
        for p in parents:
            if p not in loaded_requirements:
                Log.Error(f"unknown parent [{p}]")
                return
            parent_requirements.append(loaded_requirements[p])
        properties = loaded_dtypes[tk]
        r = target.AddRequirement(properties, parent_requirements)

    transforms_definitions: list[str] = []
    for d in (home/"metasmith_definitions/transforms").rglob("*.py"):
        if d.stem.startswith("_"): continue
        transforms_definitions.append(d)

    transform_references = [LoadTransform(d) for d in transforms_definitions]
    transforms: list[Transform] = []
    for r in transform_references:
        tr = Transform(NS)
        for k, v in r.input_signature.items():
            tr.AddRequirement(v)
        for k, v in r.output_signature.items():
            tr.AddProduct(v)
        transforms.append(tr)

    solutions = Solve(given, target, transforms)
    if len(solutions) == 0:
        Log.Error("no solutions found")
        return
    return solutions[0]

def LoadTransforms(workspace: Workspace, namespace: Namespace):
    transforms_definitions: list[str] = []
    for d in (workspace.TRANSFORMS).rglob("*.py"):
        if d.stem.startswith("_"): continue
        transforms_definitions.append(d)

    transform_references = [LoadTransform(d) for d in transforms_definitions]
    transforms: list[Transform] = []
    for r in transform_references:
        tr = Transform(NS)
        for k, v in r.input_signature.items():
            tr.AddRequirement(v)
        for k, v in r.output_signature.items():
            tr.AddProduct(v)
        transforms.append(tr)


def Run(home: Path, request_file: Path):
    solutions = GeneratePlan(home, request_file)
    if solutions is None: return # error has ocurred

    for step in solutions.dependency_plan:
        Log.Info(f"{step}")
        Log.Info("")

