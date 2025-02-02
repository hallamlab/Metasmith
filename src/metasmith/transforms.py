import os, sys
import yaml
from pathlib import Path
from typing import Callable, Iterable
from importlib import import_module, __import__
from .models.libraries import ExecutionContext, ExecutionResult, TransformInstance

_last_transform: TransformInstance = None
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

# this is called from within a transform definition
def RegisterTransform(
    protocol: Callable[[ExecutionContext], ExecutionResult],
    prototypes: Iterable[Path],
    input_signature: Iterable[str],
    output_signature: Iterable[str],
):
    prototypes_lib = {}
    for p in prototypes:
        with open(p, "r") as f:
            d = yaml.safe_load(f)
        k = p.stem
        protos = d["prototypes"]
        ns = prototypes_lib.get(k, {})
        for pk, pv in protos.items():
            ns[pk] = {f"{k}={v}" for k, v in pv.items()}
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
    _last_transform = TransformInstance(
        protocol=protocol,
        input_signature={k:lookup(k) for k in input_signature},
        output_signature={k:lookup(k) for k in output_signature},
    )
