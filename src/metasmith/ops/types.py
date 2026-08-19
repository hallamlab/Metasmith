from __future__ import annotations

from pathlib import Path

import yaml as _yaml

from ._common import collect_types, resolve_type, endpoint_to_dict


def list_types(
    type_paths: list[str] | None = None,
    transform_paths: list[str] | None = None,
    namespace: str | None = None,
) -> list[dict]:
    all_types = collect_types(type_paths, transform_paths)
    results = []
    for ns, types in all_types.items():
        if namespace and ns != namespace:
            continue
        for name, ep in types.items():
            results.append(endpoint_to_dict(name, ns, ep))
    return results


def get_type(
    type_name: str,
    type_paths: list[str] | None = None,
    transform_paths: list[str] | None = None,
) -> dict:
    ns, name, ep = resolve_type(type_name, type_paths, transform_paths)
    packed = ep.Pack(parents=True)
    return {
        "name": name,
        "namespace": ns,
        "full_name": f"{ns}::{name}",
        "properties": packed["properties"],
        "parents": packed.get("parents", []),
    }


def check_compatibility(
    source_type: str,
    target_type: str,
    type_paths: list[str] | None = None,
    transform_paths: list[str] | None = None,
) -> dict:
    _, _, src_ep = resolve_type(source_type, type_paths, transform_paths)
    _, _, tgt_ep = resolve_type(target_type, type_paths, transform_paths)
    compatible = src_ep.IsA(tgt_ep)
    if compatible:
        reason = f"{source_type} has all properties of {target_type}"
    else:
        missing = tgt_ep.properties - src_ep.properties
        reason = f"{source_type} is missing properties: {missing}"
    return {"compatible": compatible, "reason": reason}


def create_type_library(
    path: str,
    ontology: dict | None = None,
    types: dict | None = None,
) -> dict:
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    assert not p.exists(), f"file [{p}] already exists"
    ont = ontology or {
        "name": "EDAM",
        "version": "1.25",
        "doi": "https://doi.org/10.1093/bioinformatics/btt113",
        "strict": False,
    }
    doc = {"schema": "v1", "ontology": ont, "types": types or {}}
    with open(p, "w") as f:
        _yaml.safe_dump(doc, f)
    return {"path": str(p), "type_count": len(doc["types"])}


def add_type(
    library_path: str,
    name: str,
    properties: dict,
    extends: list[str] | None = None,
    overwrite: bool = False,
) -> dict:
    p = Path(library_path).resolve()
    assert p.exists(), f"library [{p}] does not exist"
    with open(p) as f:
        doc = _yaml.safe_load(f) or {}
    doc.setdefault("types", {})
    if name in doc["types"] and not overwrite:
        raise FileExistsError(f"type [{name}] already exists; pass overwrite=True")
    entry: dict = {"properties": properties}
    if extends:
        entry["extends"] = list(extends)
    doc["types"][name] = entry
    with open(p, "w") as f:
        _yaml.safe_dump(doc, f)
    return {"path": str(p), "name": name, "type_count": len(doc["types"])}
