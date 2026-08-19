from __future__ import annotations

import json
from pathlib import Path

from metasmith.models.dag_renderer import DagRenderer, Label, LabelMode, NodeKind

HERE = Path(__file__).resolve().parent


def load_dag(name: str = "stress_dag", **kwargs) -> DagRenderer:
    data = json.loads((HERE / f"{name}.json").read_text())
    r = DagRenderer(**kwargs)
    for n in data["nodes"]:
        L = n["label"]
        r.add_node(
            NodeKind[n["kind"]], n["id"],
            Label(name=L["name"], namespace=L["namespace"], full=L["full"]),
        )
    for src, dst in data["edges"]:
        r.add_edge(src, dst)
    return r


__all__ = ["load_dag", "LabelMode", "HERE"]
