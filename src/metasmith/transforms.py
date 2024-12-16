import os, sys
import yaml
from pathlib import Path
from typing import Callable, Iterable
from importlib import import_module, __import__
from .models import ExecutionContext, ExecutionResult, TransformReference

def LoadTransform(definition: Path):
    global _last_transform
    _last_transform = None

    original_path = sys.path
    sys.path = [str(definition.parent)]+sys.path
    try:
        # importing the definition should let it call RegisterTransform
        # which sets _last_transform
        _ = __import__(f"{definition.stem}")
        # _ = import_module(f".{definition.stem}", f"{definition.parent.stem}")
        return _last_transform
    finally:
        sys.path = original_path

_last_transform: TransformReference = None
# this is called from within a transform definition
def RegisterTransform(
    container: str,
    protocol: Callable[[ExecutionContext], ExecutionResult],
    prototypes: Iterable[Path],
    input_signature: Iterable[str],
    output_signature: Iterable[str],
):
    prototypes_lib = {}
    for p in prototypes:
        with open(p, "r") as f:
            d = yaml.safe_load(f)
        k = d.get("namespace", "base")
        protos = d["prototypes"]
        ns = prototypes_lib.get(k, {})
        for pk, pv in protos.items():
            ns[pk] = set(pv)
        prototypes_lib[k] = ns

    def lookup(k):
        if ":" in k:
            namespace, prototype = k.split(":")
        else:
            namespace = None
            prototype = k

        if namespace is None:
            for ns, protos in prototypes_lib.items():
                if prototype in protos:
                    return protos[prototype]
            raise Exception(f"Prototype [{k}] not found")
        else:
            if namespace not in prototypes_lib:
                raise Exception(f"namespace [{namespace}] not found")
            if prototype not in prototypes_lib[namespace]:
                raise Exception(f"Prototype [{k}] not found in namespace [{namespace}]")
            return prototypes_lib[namespace][prototype]

    global _last_transform
    _last_transform = TransformReference(
        container=container,
        protocol=protocol,
        input_signature={k:lookup(k) for k in input_signature},
        output_signature={k:lookup(k) for k in output_signature},
    )
