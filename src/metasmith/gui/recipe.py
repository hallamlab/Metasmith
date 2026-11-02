from __future__ import annotations

from ..ops import inputs as op_inputs
from ..ops import samples as op_samples
from .store import Project


def rows_of(p: Project, name: str, wf=None) -> list[dict]:
    if wf is None:
        wf = p.read_workflow(name)
    rows = list(wf.request.get("input_drafts") or [])
    lib_path = p.input_library_path(name)
    if not lib_path.is_dir() or op_samples.read_record(str(lib_path)).get("adopted"):
        return rows
    out = op_inputs.adopt(str(lib_path), rows)
    if out is None:
        return rows
    p.write_request(name, {"input_drafts": out["rows"]})
    op_samples.write_record(str(lib_path), out["record"])
    wf.request["input_drafts"] = out["rows"]
    return out["rows"]
