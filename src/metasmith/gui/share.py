"""Handing an ssh host, an agent or a workflow to somebody else.

The unit of sharing is a string: `msm1:<checksum>:<base64 of gzipped yaml>`.
A string is the one thing that travels through every channel a colleague
actually uses -- chat, mail, a ticket -- without anyone having to agree on file
transport first.

Three properties are load-bearing.

**Versioned and checksummed.** The prefix says which format this is, so a later
one is *refused* rather than mis-read, and the digest says the string arrived
whole -- pasted payloads get truncated by line wrapping, and the failure of the
unchecked version is a half-built object rather than a message.

**Resolution is by name, best effort.** A shared object names libraries, hosts
and types rather than carrying them: the recipient has their own copies, and a
path from the sender's disk means nothing here. What does not resolve is
*reported*, not refused -- the object is created with the unresolved parts named
so they can be seen and fixed. That is the stance the rest of the GUI already
takes: an agent carries `problems`, a recipe row with an unknown type draws red,
and both are enforced at launch rather than at edit.

**Nothing secret travels.** A host's identity file is a path to a private key on
the sender's machine and is dropped; an agent's `real_path` is where it was
deployed on the sender's host and is dropped too. What remains is still worth
looking at before it is sent -- a home directory, setup commands, a cluster
account in the params -- which is why export hands back the decoded body beside
the payload, for the page to show before anything is copied.

This module knows about projects and ssh configs but nothing about HTTP: the
routes in `api.py` are three thin calls (export, preview, commit), one set for
all three kinds.
"""
from __future__ import annotations

import base64
import gzip
import hashlib
from pathlib import Path

import yaml

from ..models.paths import DEFERRED, is_deferred
from ..ops import agent as op_agent
from ..ops import data as op_data
# by name rather than `from . import stdlib`: importing the *package* from a
# module the package's own api imports would put this file inside the
# gui/api/app cycle, which `test_no_import_cycles` pins by exact membership
from .names import slugify
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
    """A payload that cannot be read, said in the user's words."""


# -- the envelope ------------------------------------------------------------


def encode(kind: str, body: dict) -> str:
    assert kind in KINDS, f"unknown kind [{kind}]"
    doc = yaml.safe_dump({"kind": kind, "body": body}, sort_keys=False).encode()
    blob = gzip.compress(doc, mtime=0)
    digest = hashlib.sha256(blob).hexdigest()[:16]
    return f"{MAGIC}:{digest}:{base64.urlsafe_b64encode(blob).decode()}"


def decode(payload: str, expect: str | None = None) -> tuple[str, dict]:
    """The kind and body a payload carries, or a refusal a person can act on."""
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


# -- library references ------------------------------------------------------
#
# A library travels as its name inside the standard library -- `transforms/x`
# -- because that is the only spelling both ends share. Anything outside the
# clone keeps its absolute path, which will usually not resolve on the other
# side; that is what the unresolved list is for.


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


# -- export ------------------------------------------------------------------


def export_host(cfg: SshConfig, alias: str) -> dict:
    """A host as its connection details. The identity file stays here."""
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
    # `real_path` is deliberately absent: it is where this agent was deployed on
    # the sender's host, and a copy that claimed it would skip its own deploy.
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


# A value row is something typed into a box -- a read-pair descriptor, a few
# lines of metadata. This bound is far above any of those and well below
# anything that would make a payload unpasteable; a file that big under a
# library-owned name is not a value and is left behind rather than shipped.
MAX_VALUE_BYTES = 64 * 1024


def _has_no_path(path) -> bool:
    """Whether a row arrives without one -- the constant, or one minted from it."""
    return not path or path == DEFERRED_WIRE or is_deferred(path)


def _read_value(lib_path: Path, item_path: str | None) -> str | None:
    """The contents of a library-owned row, or `None` if it is not one."""
    if not item_path or Path(item_path).is_absolute():
        return None
    f = lib_path / item_path
    if not f.is_file() or f.stat().st_size > MAX_VALUE_BYTES:
        return None
    try:
        return f.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def export_workflow(p: Project, name: str, bound: bool = False) -> dict:
    """A workflow as its spec and its recipe -- never the store's bookkeeping.

    `bound` keeps the input paths, which is "run exactly this" and only means
    anything to someone with the same files. Unbound is the recipe: every row
    that *points* at a file becomes DEFERRED, which is precisely what a template
    is, and the recipient fills the paths in. A row that *holds* its value
    travels whole either way -- see below.
    """
    wf = p.read_workflow(name)
    root = _stdlib_root(p)
    req = wf.request

    lib_path = p.input_library_path(name)
    inputs: list[dict] = []
    if lib_path.is_dir():
        info = op_data.inspect_library(str(lib_path))
        for item in info.get("items", []):
            row = op_data.show_item_lineage(str(lib_path), item["path"], render=False)
            entry = {
                # The row's name on the sender's side, and the *only* thing
                # lineage is stated in terms of. `path` is data that may be
                # thrown away by an unbound export -- and every unbound row
                # would then be called DEFERRED, so a child naming a deferred
                # parent would have no way to say which one it meant.
                "id": row.get("path"),
                "path": row.get("path"),
                "type": row.get("type_name"),
                "parents": [x.get("path") for x in (row.get("parents") or [])],
            }
            # A relative entry is library-owned: the file *is* the row, written
            # by `AddValue` and holding a value someone typed rather than
            # pointing at one of their files. It travels whole and under its own
            # name -- deferring it would throw the recipe away and leave the
            # recipient a nameless blank where a read-pair descriptor was.
            value = _read_value(lib_path, entry["path"])
            if value is not None:
                entry["value"] = value
            elif not bound:
                entry["path"] = DEFERRED_WIRE
            inputs.append(entry)

    drafts = []
    for d in req.get("input_drafts") or []:
        d = dict(d)
        if not bound:
            # A draft holding a `{column}` token is a rule rather than a path,
            # and it is the whole substance of a sample-array recipe -- so it
            # travels either way. A literal path is one machine's file.
            for k in ("path", "name", "value"):
                if isinstance(d.get(k), str) and "{" not in d[k]:
                    d[k] = ""
        drafts.append(d)

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
        "inputs": inputs,
        "drafts": drafts,
    }


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


# -- import ------------------------------------------------------------------
#
# Every import is two calls with the same body: a preview that reads what would
# happen, and a commit that does it. The preview exists because a payload
# carries paths -- a home directory, a cluster account, someone's absolute input
# files -- and pasting a string from a colleague should not be the moment you
# find out what was in it.


def _free_name(taken, wanted: str) -> str:
    """`wanted`, or the first `-2`, `-3` after it that nobody has."""
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
        # Passed as a mapping, which is exactly what `Agent.Pack` handles
        # outside its stringifying optional block -- a shared agent whose params
        # arrived as `"{'account': 'x'}"` would be truthy, wrong, and silent.
        default_params=dict(body.get("default_params") or {}),
        native=bool(body.get("native")),
        gpu_args=list(body.get("gpu_args") or []),
    )
    # named by hand from the sender's side: no naming record, so it is never
    # renamed out from under whoever imported it
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
    rows = body.get("inputs") or []
    unknown_types = sorted({
        r.get("type") for r in rows if r.get("type") and r.get("type") not in known
    })

    notes = []
    if name != wanted:
        notes.append(f"[{wanted}] is taken here, so it will be called [{name}]")
    for n in missing_tr + missing_res:
        notes.append(f"library [{n}] is not in your standard library, so it will not be enabled")
    for t in unknown_types:
        notes.append(f"type [{t}] is not in your standard library; rows of it arrive unregistered and red")
    # a value row carries its own contents, so it is not one of the blanks
    blank = [r for r in rows if r.get("value") is None and _has_no_path(r.get("path"))]
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
            "draft_count": len(body.get("drafts") or []),
            "bound": bool(body.get("bound")),
        },
        "notes": notes,
    }


def import_workflow(p: Project, body: dict) -> dict:
    """Create the workflow, then place as much of its recipe as resolves.

    A row whose type this project does not have cannot be registered -- the
    library would refuse it -- so it lands as a *draft* carrying that type,
    which is the form the recipe already draws in red and the user can retype in
    place. Everything else is registered, parents first, and the paths that
    changed on the way in (a deferred row is minted here, not there) are
    followed through parents and shared inputs.
    """
    prev = preview_workflow(p, body)
    name = prev["name"]
    root = _stdlib_root(p)
    spec = body.get("spec") or {}
    tr_libs, _ = _resolve_libs(spec.get("transform_libraries"), root)
    res_libs, _ = _resolve_libs(spec.get("resource_libraries"), root)

    wf = p.create_workflow(name=name, request={
        "sample_type": spec.get("sample_type"),
        "target_types": list(spec.get("target_types") or []),
        "transform_libraries": tr_libs,
        "resource_libraries": res_libs,
        "shared_input_paths": [],
        "input_drafts": [dict(d) for d in body.get("drafts") or []],
    })
    lib_path = str(wf.path / INPUT_LIBRARY_DIRNAME)
    op_data.create_library(lib_path, type_library_paths=discover(p.root)["data_types"])

    known = {t["full_name"] for t in available_types(p.root) if t.get("full_name")}
    rows = list(body.get("inputs") or [])
    placed: dict[str, str] = {}          # the row's id there -> its path here
    drafts = list(wf.request.get("input_drafts") or [])
    pending = [r for r in rows if r.get("type") in known]
    for i, r in enumerate(r for r in rows if r.get("type") not in known):
        drafts.append({
            "id": f"imported{i}", "mode": "file", "path": "", "name": "", "value": "",
            "dtype": r.get("type") or "", "parents": [],
        })

    # parents first: a row is registered once every parent it names has been,
    # and a parent that never arrives is dropped from the lineage rather than
    # blocking the row it belonged to
    progressed = True
    while pending and progressed:
        progressed, rest = False, []
        for r in pending:
            rid, src = r.get("id") or r.get("path"), r.get("path")
            parents = [x for x in (r.get("parents") or []) if x != rid]
            if any(x not in placed for x in parents):
                rest.append(r)
                continue
            kin = [placed[x] for x in parents]
            if r.get("value") is not None:
                out = op_data.add_value(lib_path, src, r["value"], r["type"], parents=kin)
            else:
                out = op_data.add_item(
                    lib_path, DEFERRED if _has_no_path(src) else src, r["type"], parents=kin,
                )
            placed[rid] = out["path"]
            progressed = True
        pending = rest
    notes = list(prev["notes"])
    if pending:
        notes.append(f"{len(pending)} input row(s) named a parent that did not arrive and were skipped")

    # a draft's parents point at rows that were just re-registered under this
    # project's paths; one that did not arrive is dropped, or the draft can
    # never be committed and never says why
    for d in drafts:
        d["parents"] = [
            x if str(x).startswith("#") else placed[x]
            for x in (d.get("parents") or []) if str(x).startswith("#") or x in placed
        ]
    shared = [placed[x] for x in (spec.get("shared_input_paths") or []) if x in placed]
    p.write_request(name, {"shared_input_paths": shared, "input_drafts": drafts})
    return {"kind": "workflow", "name": name, "notes": notes}


# -- the one door ------------------------------------------------------------


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
