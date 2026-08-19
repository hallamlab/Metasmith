from __future__ import annotations

import base64
import gzip
import hashlib
from pathlib import Path

import yaml

from ..models.paths import DEFERRED, is_deferred
from ..ops import agent as op_agent
from ..ops import data as op_data
from ..ops import samples as op_samples
from .names import slugify
from .recipe import rows_of
from .stdlib import available_types, discover
from .store import INPUT_LIBRARY_DIRNAME, Project, ProjectError
from .sshconfig import SshConfig, SshConfigError

MAGIC = "msm1"
KINDS = ("ssh_host", "agent", "workflow")

# The path spelling for "not known yet" on the wire. It is what `str(DEFERRED)`
# renders, so the constant round-trips as itself rather than as a second
# vocabulary that has to be kept in step with the first.
DEFERRED_WIRE = str(DEFERRED)


class ShareError(Exception):
    pass


def encode(kind: str, body: dict) -> str:
    assert kind in KINDS, f"unknown kind [{kind}]"
    doc = yaml.safe_dump({"kind": kind, "body": body}, sort_keys=False).encode()
    blob = gzip.compress(doc, mtime=0)
    digest = hashlib.sha256(blob).hexdigest()[:16]
    return f"{MAGIC}:{digest}:{base64.urlsafe_b64encode(blob).decode()}"


def decode(payload: str, expect: str | None = None) -> tuple[str, dict]:
    text = "".join((payload or "").split())
    parts = text.split(":", 2)
    if len(parts) != 3 or not parts[0]:
        raise ShareError("that is not a metasmith share string")
    magic, digest, b64 = parts
    if magic != MAGIC:
        raise ShareError(
            f"this was written by a different metasmith share format [{magic}]; "
            f"this one reads [{MAGIC}]"
        )
    try:
        blob = base64.urlsafe_b64decode(b64 + "=" * (-len(b64) % 4))
    except Exception:
        raise ShareError("that string is damaged -- it is not valid base64") from None
    if hashlib.sha256(blob).hexdigest()[:16] != digest:
        raise ShareError(
            "that string did not arrive whole -- it was truncated or edited in transit"
        )
    try:
        doc = yaml.safe_load(gzip.decompress(blob)) or {}
    except Exception:
        raise ShareError("that string is damaged and could not be unpacked") from None
    kind, body = doc.get("kind"), doc.get("body")
    if kind not in KINDS or not isinstance(body, dict):
        raise ShareError("that payload does not describe anything metasmith shares")
    if expect is not None and kind != expect:
        raise ShareError(f"that is a shared {kind.replace('_', ' ')}, not a {expect}")
    return kind, body


def _lib_name(path: str, lib_root: Path | None) -> str:
    p = Path(path)
    if lib_root is None:
        return str(p)
    try:
        return str(p.resolve().relative_to(lib_root))
    except ValueError:
        return str(p)


def _lib_path(name: str, lib_root: Path | None) -> str | None:
    p = Path(name)
    if not p.is_absolute():
        if lib_root is None:
            return None
        p = lib_root / p
    return str(p) if p.is_dir() else None


def _stdlib_root(p: Project) -> Path | None:
    found = discover(p.root)
    return Path(found["path"]) if found["present"] else None


def export_host(cfg: SshConfig, alias: str) -> dict:
    entry = cfg.find(alias)
    if entry is None:
        raise SshConfigError(f"no host named [{alias}]")
    d = entry.to_dict()
    return {
        k: d.get(k) for k in ("alias", "hostname", "user", "port", "proxy_jump")
    }


def export_agent(p: Project, name: str) -> dict:
    if not p.agent_exists(name):
        raise ProjectError(f"no agent named [{name}]")
    info = op_agent.info(str(p.agent_path(name)))
    return {
        "name": name,
        "home": info.get("home"),
        "container": info.get("container"),
        "runtime": info.get("runtime"),
        "native": bool(info.get("native")),
        "gpu_args": list(info.get("gpu_args") or []),
        "globus_uuid": info.get("globus_uuid"),
        "setup_commands": list(info.get("setup_commands") or []),
        "default_preset": info.get("default_preset"),
        "default_params": dict(info.get("default_params") or {}),
    }


def _has_no_path(path) -> bool:
    return not path or path == DEFERRED_WIRE or is_deferred(path)


def export_workflow(p: Project, name: str, bound: bool = False) -> dict:
    wf = p.read_workflow(name)
    root = _stdlib_root(p)
    req = wf.request

    rows = []
    for d in rows_of(p, name):
        d = dict(d)
        if not bound and d.get("mode") != "value":
            d["path"] = ""
        rows.append(d)

    return {
        "name": name,
        "bound": bool(bound),
        "spec": {
            "sample_type": req.get("sample_type"),
            "target_types": list(req.get("target_types") or []),
            "transform_libraries": [
                _lib_name(x, root) for x in req.get("transform_libraries") or []
            ],
            "resource_libraries": [
                _lib_name(x, root) for x in req.get("resource_libraries") or []
            ],
            "shared_input_paths": list(req.get("shared_input_paths") or []),
        },
        "inputs": [],
        "drafts": rows,
    }


def _rows_from(body: dict) -> tuple[list[dict], dict[str, str]]:
    rows = [dict(d) for d in body.get("drafts") or []]
    ids: dict[str, str] = {}
    made: list[dict] = []
    for i, r in enumerate(body.get("inputs") or []):
        rid = f"imported{i}"
        ids[str(r.get("id") or r.get("path"))] = rid
        value = r.get("value")
        path = "" if _has_no_path(r.get("path")) else str(r.get("path"))
        made.append({
            "id": rid,
            "mode": "value" if value is not None else "file",
            "path": "" if value is not None else path,
            "name": path if value is not None else "",
            "values": [{"key": "", "value": value if value is not None else ""}],
            "dtype": r.get("type") or "",
            "parents": list(r.get("parents") or []),
        })
    for row in rows + made:
        row["parents"] = [
            str(x) if str(x).startswith("#") else f"#{ids[str(x)]}"
            for x in (row.get("parents") or [])
            if str(x).startswith("#") or str(x) in ids
        ]
    return rows + made, ids


def export(p: Project, cfg: SshConfig, kind: str, name: str, bound: bool = False) -> dict:
    if kind == "ssh_host":
        body = export_host(cfg, name)
    elif kind == "agent":
        body = export_agent(p, name)
    elif kind == "workflow":
        body = export_workflow(p, name, bound=bound)
    else:
        raise ShareError(f"there is nothing of kind [{kind}] to share")
    return {"kind": kind, "name": name, "body": body, "payload": encode(kind, body)}


def _free_name(taken, wanted: str) -> str:
    if wanted not in taken:
        return wanted
    i = 2
    while f"{wanted}-{i}" in taken:
        i += 1
    return f"{wanted}-{i}"


def preview_host(cfg: SshConfig, body: dict) -> dict:
    alias = (body.get("alias") or "").strip()
    notes = []
    existing = cfg.find(alias) if alias else None
    if existing is not None:
        notes.append(
            f"[{alias}] is already in your ssh config, so this cannot be imported "
            f"under that name -- rename it or remove the existing entry"
        )
    jump = body.get("proxy_jump")
    if jump and cfg.find(jump) is None:
        notes.append(f"its jump host [{jump}] is not in your ssh config")
    notes.append("no identity file travels with a shared host; pick or generate a key after importing")
    return {
        "kind": "ssh_host", "name": alias, "blocked": existing is not None,
        "creates": {k: body.get(k) for k in ("alias", "hostname", "user", "port", "proxy_jump")},
        "notes": notes,
    }


def import_host(cfg: SshConfig, body: dict) -> dict:
    host = cfg.add_host(
        alias=(body.get("alias") or "").strip(),
        hostname=(body.get("hostname") or "").strip(),
        user=body.get("user"),
        port=body.get("port"),
        proxy_jump=body.get("proxy_jump"),
    )
    return {"kind": "ssh_host", "name": host["alias"], "host": host,
            "notes": preview_host(cfg, body)["notes"][-1:]}


def _agent_host(home: str | None) -> str | None:
    home = (home or "").strip()
    if not home.startswith("ssh://"):
        return None
    return home[len("ssh://"):].partition(":")[0].strip() or None


def preview_agent(p: Project, cfg: SshConfig, body: dict) -> dict:
    wanted = slugify(body.get("name") or "agent")
    name = _free_name(set(p.agent_names(include_archived=True)), wanted)
    notes = []
    if name != wanted:
        notes.append(f"[{wanted}] is taken here, so it will be called [{name}]")
    host = _agent_host(body.get("home"))
    if host and cfg.find(host) is None:
        notes.append(f"its host [{host}] is not in your ssh config, so it will need one before it can deploy")
    notes.append("an imported agent has not been deployed here; deploy it before running on it")
    return {
        "kind": "agent", "name": name, "blocked": False,
        "creates": {
            "name": name,
            "home": body.get("home"),
            "runtime": body.get("runtime"),
            "container": body.get("container"),
            "setup_commands": list(body.get("setup_commands") or []),
            "default_preset": body.get("default_preset"),
            "default_params": dict(body.get("default_params") or {}),
        },
        "notes": notes,
    }


def import_agent(p: Project, cfg: SshConfig, body: dict) -> dict:
    prev = preview_agent(p, cfg, body)
    name = prev["name"]
    p.initialize()
    op_agent.save_agent(
        path=str(p.agent_path(name)),
        home_uri=body.get("home") or "",
        container=body.get("container") or None,
        runtime=(body.get("runtime") or "APPTAINER").upper(),
        setup_commands=list(body.get("setup_commands") or []),
        globus_uuid=body.get("globus_uuid") or None,
        default_preset=body.get("default_preset") or None,
        default_params=dict(body.get("default_params") or {}),
        native=bool(body.get("native")),
        gpu_args=list(body.get("gpu_args") or []),
    )
    return {"kind": "agent", "name": name, "notes": prev["notes"]}


def _resolve_libs(names, root: Path | None) -> tuple[list[str], list[str]]:
    resolved, missing = [], []
    for n in names or []:
        path = _lib_path(n, root)
        (resolved.append(path) if path else missing.append(n))
    return resolved, missing


def preview_workflow(p: Project, body: dict) -> dict:
    root = _stdlib_root(p)
    spec = body.get("spec") or {}
    wanted = slugify(body.get("name") or "workflow")
    name = _free_name(set(p.workflow_names(include_archived=True)), wanted)
    _, missing_tr = _resolve_libs(spec.get("transform_libraries"), root)
    _, missing_res = _resolve_libs(spec.get("resource_libraries"), root)

    known = {t["full_name"] for t in available_types(p.root) if t.get("full_name")}
    rows, _ = _rows_from(body)
    unknown_types = sorted({
        r.get("dtype") for r in rows if r.get("dtype") and r.get("dtype") not in known
    })

    notes = []
    if name != wanted:
        notes.append(f"[{wanted}] is taken here, so it will be called [{name}]")
    for n in missing_tr + missing_res:
        notes.append(f"library [{n}] is not in your standard library, so it will not be enabled")
    for t in unknown_types:
        notes.append(f"type [{t}] is not in your standard library; rows of it arrive red until you retype them")
    blank = [
        r for r in rows
        if r.get("mode") != "value" and _has_no_path(r.get("path")) and r.get("dtype")
    ]
    if not body.get("bound") and blank:
        notes.append(f"{len(blank)} input row(s) arrive with no path -- fill them in before solving")
    return {
        "kind": "workflow", "name": name, "blocked": False,
        "creates": {
            "name": name,
            "sample_type": spec.get("sample_type"),
            "target_types": list(spec.get("target_types") or []),
            "transform_libraries": list(spec.get("transform_libraries") or []),
            "input_count": len(rows),
            "draft_count": len(rows),
            "bound": bool(body.get("bound")),
        },
        "notes": notes,
    }


def import_workflow(p: Project, body: dict) -> dict:
    prev = preview_workflow(p, body)
    name = prev["name"]
    root = _stdlib_root(p)
    spec = body.get("spec") or {}
    tr_libs, _ = _resolve_libs(spec.get("transform_libraries"), root)
    res_libs, _ = _resolve_libs(spec.get("resource_libraries"), root)
    rows, ids = _rows_from(body)

    wf = p.create_workflow(name=name, request={
        "sample_type": spec.get("sample_type"),
        "target_types": list(spec.get("target_types") or []),
        "transform_libraries": tr_libs,
        "resource_libraries": res_libs,
        "shared_input_paths": [
            str(x) if str(x).startswith("#") else f"#{ids[str(x)]}"
            for x in (spec.get("shared_input_paths") or [])
            if str(x).startswith("#") or str(x) in ids
        ],
        "input_drafts": rows,
    })
    lib_path = str(wf.path / INPUT_LIBRARY_DIRNAME)
    op_data.create_library(lib_path, type_library_paths=discover(p.root)["data_types"])
    op_samples.write_record(lib_path, {"adopted": True, "rows": {}})
    return {"kind": "workflow", "name": name, "notes": list(prev["notes"])}


def preview(p: Project, cfg: SshConfig, payload: str) -> dict:
    kind, body = decode(payload)
    if kind == "ssh_host":
        return preview_host(cfg, body)
    if kind == "agent":
        return preview_agent(p, cfg, body)
    return preview_workflow(p, body)


def commit(p: Project, cfg: SshConfig, payload: str) -> dict:
    kind, body = decode(payload)
    if kind == "ssh_host":
        return import_host(cfg, body)
    if kind == "agent":
        return import_agent(p, cfg, body)
    return import_workflow(p, body)
