"""A stored workflow's input rows, with the store's half of adoption.

`ops.inputs.adopt` is pure -- it is handed a library and rows and says what the
rows should become. Persisting both halves needs the project store, and two
callers need it (the routes, and export), so it lives here rather than in either
of them.

Called on the request thread only, never inside a solve job: it writes the
request, and the request is the browser's file.
"""
from __future__ import annotations

from ..ops import inputs as op_inputs
from ..ops import samples as op_samples
from .store import Project


def rows_of(p: Project, name: str, wf=None) -> list[dict]:
    """The recipe's input rows, adopting anything registered without one.

    One kind of row. A sample array is not a second list -- with a sheet
    attached every row is one, reading the columns its fields bind -- and
    neither is a registered library item: a workflow whose library predates
    this (an old project, a copy of a template, an import) gets one row per item
    the first time anything asks, and the record says so from then on. Without
    that mark, deleting a row could not be expressed at all -- the item outlives
    the row until the next solve, and every read in between would put it back.

    `wf` lets a caller that already holds this workflow's record hand it over,
    rather than pay a second `read_workflow` -- a full parse of both its YAML
    files -- to look at one field. When adoption writes, it also updates
    `wf.request["input_drafts"]` in place, so that caller's copy stays true to
    what was just persisted without a re-read.
    """
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
