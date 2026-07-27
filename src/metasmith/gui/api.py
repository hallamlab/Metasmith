"""The REST surface, one blueprint per nav section.

This is a second thin veneer over `metasmith.ops`, exactly as the CLI is: every
route calls those functions directly and none of them shells out to the command
line. Where a route needs something the ops layer does not expose -- a project
layout, a readable name, an SSH config -- that lives in a sibling module here,
never in a route body.
"""
from __future__ import annotations

import json
import shutil
import threading
from fnmatch import fnmatch
from pathlib import Path

from flask import Blueprint, Response, current_app, jsonify, request

from ..ops import agent as op_agent
from ..ops import data as op_data
from ..ops import runtime as op_runtime
from ..ops import workflow as op_workflow
from . import stdlib
from .jobs import LogCapture
from .names import assert_valid_name, generate_workflow_name, slugify
from .sshconfig import SshConfig, SshConfigError
from .store import INPUT_LIBRARY_DIRNAME, Project, ProjectError, utcnow

bp = Blueprint("api", __name__, url_prefix="/api")

# What a new agent's home is set to before the user touches it. `~` is the one
# path spelling that means the same thing whether the agent runs here or on a
# cluster, and it is expanded by whichever side ends up resolving it. The name
# is in the path because a host with three agents on it otherwise has three
# directories called the same thing, and which one you are looking at is then
# only knowable from this side.
DEFAULT_AGENT_HOME_PREFIX = "~/msm."


def default_agent_home(name: str) -> str:
    return f"{DEFAULT_AGENT_HOME_PREFIX}{name}"


def home_is_default(name: str, home: str | None) -> bool:
    """Is this home still simply the one the name makes?

    Answered here because only this side can answer it: `Source.Parse` expands
    `~` for a local home, so what comes back from a save is `<your home>/msm.x`
    where `~/msm.x` went in, and the browser does not know what `~` is. The page
    leaves the field empty when this is true -- so a wrong `True` would swap a
    path someone typed for a different one on the next save, and the comparison
    is exact on both spellings rather than a suffix test.
    """
    if not home:
        return False
    default = default_agent_home(name)
    remote = home.startswith("ssh://")
    if remote:
        # `ssh://host:path` -- split after the scheme, or the `:` found is the
        # one in `ssh:` and every remote home reads as having no path at all
        _, sep, path = home[len("ssh://"):].partition(":")
        if not sep:
            return False  # the older `ssh://host/path` spelling; never default
    else:
        path = home
    if path == default:
        return True
    # a local home, already expanded; a remote one is never expanded here
    return not remote and Path(path) == Path(default).expanduser()


# A setup block starts as a shebang and nothing else. It is a comment wherever
# it ends up -- the lines are run one at a time over a live shell, and pasted
# into the launcher script under their own marker -- so it costs nothing, and
# it says what the box is: bash, not a list of module names.
DEFAULT_SETUP_COMMANDS = ["#!/bin/bash"]

# Planning is not reentrant. TransformInstance.Load imports each transform by
# bare module name, mutates sys.path, calls importlib.reload, and hands the
# result back through a *class* attribute -- all process-global. Two generates
# running at once clobber each other and fail with a bare
# "spec not found for the module". The CLI never hit this because one process
# plans once; the GUI lets a user press generate on two workflows in a row, so
# it serialises them here. Planning is short and single-user, so the queueing
# costs nothing.
_plan_lock = threading.Lock()


# -- plumbing ----------------------------------------------------------------


def _project() -> Project:
    return current_app.config["MSM_PROJECT"]


def _jobs():
    return current_app.config["MSM_JOBS"]


def _ssh() -> SshConfig:
    return current_app.config["MSM_SSH"]


@bp.errorhandler(ProjectError)
@bp.errorhandler(SshConfigError)
def _handle_refusal(exc):
    # a refusal is a message for the user, not a stack trace
    return jsonify({"error": str(exc), "kind": "refused"}), 409


@bp.errorhandler(AssertionError)
def _handle_assertion(exc):
    return jsonify({"error": str(exc), "kind": "invalid"}), 400


@bp.errorhandler(Exception)
def _handle_error(exc):
    return jsonify({"error": str(exc) or exc.__class__.__name__, "kind": "error"}), 500


def _body() -> dict:
    return request.get_json(silent=True) or {}


def _wants_archived() -> bool:
    return request.args.get("archived", "").lower() in {"1", "true", "yes"}


# -- the update convention ---------------------------------------------------
#
# Every editable object -- an agent, a workflow, an ssh host -- is saved the
# same way: `PUT /<collection>/<id>` carrying the *whole* object. There is no
# partial form. These objects are a dozen short fields; a diff protocol would
# cost more to specify, implement and get wrong than the bytes it saves, and it
# leaves two ways to write every field.
#
# Identity travels inside the object like any other field, so an id in the body
# that differs from the one in the url is a rename, applied as part of the save.
# That is the whole reason for the convention: a name someone was given -- an
# agent's, a workflow's -- is a field they should be able to correct in the
# place they read it, not a thing needing a second route and a second gesture.
#
# The reply is the object as it now stands, at its new id if it moved, so a
# caller adopts one response rather than saving and then re-fetching.
#
# The `PATCH` routes that predate this are kept and delegate here; they take a
# subset and cannot rename.


def _renamed_to(body: dict, current: str, *, field: str = "name", slug: bool = True) -> str | None:
    """The id this update wants, or None when it is not moving.

    Absent means "not stated", which is not the same as unchanged-by-request:
    a client sending only the fields it edited still gets the rename skipped
    rather than a blank name asserted at it.
    """
    if field not in body:
        return None
    wanted = str(body[field] or "").strip()
    wanted = slugify(wanted) if slug else wanted
    assert wanted, f"a {field} is required"
    return None if wanted == current else wanted


# -- project -----------------------------------------------------------------


@bp.get("/health")
def health():
    """Is the server still there.

    The page polls this on a timer to colour the dot in the header, so it is
    deliberately the cheapest route in the file: no project, no disk, no
    stdlib walk. `/project` would answer the same question but re-walks the
    standard library every time, which is not something to do on a heartbeat.
    """
    return jsonify({"ok": True})


@bp.get("/project")
def get_project():
    from ..constants import CONDA_URL, CONTAINER_URL, DOCS_URL, GIT_URL, VERSION

    p = _project()
    return jsonify({
        "root": str(p.root),
        "version": VERSION,
        "stdlib": stdlib.discover(p.root),
        # the header's resource links; written down once, in constants.py
        "links": {
            "docs": DOCS_URL,
            "github": GIT_URL,
            "conda": CONDA_URL,
            "container": CONTAINER_URL,
        },
    })


@bp.get("/defaults/agent")
def agent_defaults():
    """What a new agent is made with.

    Generated here rather than in the browser because the name has to avoid the
    ones already taken, and only this side knows them. `runtimes` is read off
    the `env.Runtime` enum, so the dropdown gains a runtime when the enum does.
    """
    p = _project()
    name = generate_workflow_name(taken=p.agent_names(include_archived=True))
    return jsonify({
        "name": name,
        "home": default_agent_home(name),
        "home_prefix": DEFAULT_AGENT_HOME_PREFIX,
        "runtime": "APPTAINER",
        "runtimes": op_agent.runtimes(),
        "setup_commands": list(DEFAULT_SETUP_COMMANDS),
    })


@bp.get("/project/types")
def get_types():
    return jsonify(stdlib.available_types(_project().root))


@bp.get("/project/type-index")
def get_type_index():
    """Which transforms sit on either side of each type.

    Held behind the plan lock: building this imports every transform in every
    library, which is the same non-reentrant path a generate takes. Without the
    lock, opening a workflow while another one plans breaks both.
    """
    with _plan_lock:
        return jsonify(stdlib.type_index(
            _project().root, refresh=request.args.get("refresh", "") in {"1", "true", "yes"},
        ))


# -- ssh ---------------------------------------------------------------------


@bp.get("/ssh/hosts")
def ssh_hosts():
    cfg = _ssh()
    return jsonify({"path": str(cfg.path), "hosts": cfg.hosts()})


@bp.post("/ssh/hosts")
def ssh_add_host():
    b = _body()
    cfg = _ssh()
    host = cfg.add_host(
        alias=b.get("alias", ""),
        hostname=b.get("hostname", ""),
        user=b.get("user"),
        port=b.get("port"),
        proxy_jump=b.get("proxy_jump"),
        identity_file=b.get("identity_file"),
    )
    return jsonify({"host": host, "shadowed_by": cfg.shadowing_patterns(host["alias"])}), 201


@bp.get("/ssh/hosts/<alias>/identity")
def ssh_host_identity(alias):
    """The public half of whatever key this host authenticates with, if any."""
    cfg = _ssh()
    host = cfg.find(alias)
    if host is None:
        raise SshConfigError(f"no host named [{alias}]")
    return jsonify({"identity": cfg.read_identity(host.keywords.get("identityfile"))})


@bp.post("/ssh/keys")
def ssh_generate_key():
    """Mint an ed25519 keypair for an alias, or hand back the one already there."""
    b = _body()
    alias = (b.get("alias") or "").strip()
    assert alias, "an alias is required to name the key"
    return jsonify(_ssh().generate_identity(alias, comment=b.get("comment")))


@bp.delete("/ssh/keys/<alias>")
def ssh_delete_key(alias):
    """Only ever a key metasmith generated -- see `delete_identity`."""
    return jsonify(_ssh().delete_identity(alias))


@bp.put("/ssh/hosts/<alias>")
def ssh_put_host(alias):
    """Save a host whole -- see the update convention above.

    An `alias` that differs from the url renames it. An alias is not slugified:
    it is an ssh pattern the user chose, and `Host tony@big-iron.example` is a
    legal thing to be called.

    An agent's home names its host, so a rename would otherwise leave every
    agent on that host pointing at nothing. They are re-pointed here and named
    in the reply -- the alternative, refusing the rename while an agent uses
    the host, makes the one case where the name matters the one case you
    cannot fix.
    """
    b = _body()
    cfg = _ssh()
    renamed = _renamed_to(b, alias, field="alias", slug=False)
    host = cfg.update_host(alias, **b)
    repointed = _repoint_agents(alias, renamed) if renamed else []
    return jsonify({
        "host": host,
        "renamed_from": alias if renamed else None,
        "agents_repointed": repointed,
    })


@bp.patch("/ssh/hosts/<alias>")
def ssh_update_host(alias):
    """The partial form, kept for callers that predate the convention.

    `alias` is dropped rather than honoured: renaming through here would move
    the host without re-pointing the agents whose home names it, which is the
    half of the operation the PUT exists to carry.
    """
    fields = {k: v for k, v in _body().items() if k != "alias"}
    return jsonify({"host": _ssh().update_host(alias, **fields)})


def _agents_on_host(alias: str) -> list[str]:
    """Agents whose home is on this host, by name."""
    project = _project()
    out = []
    for name in project.agent_names(include_archived=True):
        try:
            info = op_agent.info(str(project.agent_path(name)))
        except Exception:
            continue
        if info.get("home_type") != "SSH":
            continue
        if info.get("home", "")[len("ssh://"):].partition(":")[0] == alias:
            out.append(name)
    return out


def _repoint_agents(old_alias: str, new_alias: str) -> list[str]:
    project = _project()
    moved = []
    for name in _agents_on_host(old_alias):
        info = op_agent.info(str(project.agent_path(name)))
        _, _, path = info["home"][len("ssh://"):].partition(":")
        op_agent.save_agent(
            path=str(project.agent_path(name)),
            home_uri=f"ssh://{new_alias}:{path}",
            container=info["container"],
            runtime=info["runtime"],
            setup_commands=info["setup_commands"],
            globus_uuid=info["globus_uuid"],
            # the machine and the directory are unchanged; only the alias moved
            renaming_host=True,
        )
        moved.append(name)
    return moved


@bp.delete("/ssh/hosts/<alias>")
def ssh_delete_host(alias):
    """Hosts are the one object that is never archived.

    Their storage is the user's own config file, so an unused entry costs
    nothing and a stale one is confusing. If an agent needs it, refuse.
    """
    dependents = _agents_on_host(alias)
    if dependents:
        raise SshConfigError(
            f"host [{alias}] is the home of agent(s) {', '.join(dependents)}; "
            f"remove or re-point them first"
        )
    return jsonify(_ssh().remove_host(alias))


def _config_payload(cfg) -> dict:
    before, managed, after = cfg.split()
    return {
        "path": str(cfg.path),
        "before": before,
        "managed": managed,
        "after": after,
        "native": cfg.native(),
        "exists": cfg.path.is_file(),
    }


@bp.get("/ssh/config")
def ssh_read_config():
    return jsonify(_config_payload(_ssh()))


@bp.put("/ssh/config")
def ssh_write_config():
    """Both halves are writable; only the layout is not.

    Sending just `managed` leaves the user's half untouched, which is what the
    per-host forms do. Sending `native` as well replaces it -- the editor shows
    the whole file, so it can save the whole file.
    """
    b = _body()
    cfg = _ssh()
    if "native" in b:
        cfg.write_all(b.get("managed", ""), b.get("native") or "")
    else:
        cfg.write_managed_block(b.get("managed", ""))
    return jsonify(_config_payload(cfg))


# -- agents ------------------------------------------------------------------


def _host_patterns() -> list[str]:
    """Every pattern the user's ssh config declares, wildcards included.

    `cfg.hosts()` is concrete destinations only -- what an agent may be pointed
    at -- but reachability is a wider question: someone with `Host *.cluster.edu`
    can ssh to a name that is nowhere in that list. Judging an agent against the
    concrete list alone would call a working host missing, and that verdict now
    stops a launch.
    """
    return [e.pattern for e in _ssh().resolved()]


def _agent_problems(info: dict, hosts: list[str]) -> list[str]:
    """What stops this agent from being run on, in the user's words.

    An agent is editable long before it is usable -- you make one, you know it
    is going on a cluster, and the host does not exist in your ssh config yet.
    Saving that is the normal way to work, so the incompleteness is *reported*
    rather than refused: it costs nothing here, and it is checked where it
    actually matters, which is the moment something is launched on it.
    """
    if info.get("error"):
        return [f"this agent's file could not be read: {info['error']}"]
    problems = []
    home = info.get("home") or ""
    if info.get("home_type") == "SSH":
        # the scheme carries a colon of its own, so the split is after it
        host, _, path = home[len("ssh://"):].partition(":")
        if not host:
            problems.append("no host chosen")
        elif not any(fnmatch(host, pattern) for pattern in hosts):
            problems.append(f"host [{host}] is not in your ssh config")
        if not path.strip():
            problems.append("no home directory")
    elif not home.strip():
        problems.append("no home directory")
    if info.get("runtime") not in set(op_agent.runtimes()):
        problems.append(f"unknown runtime [{info.get('runtime')}]")
    return problems


def _agent_payload(p: Project, name: str, hosts: list[str] | None = None) -> dict:
    """One agent, the shape every route that returns one returns.

    A save answers with this too, so a client adopts the reply rather than
    saving and then re-fetching what it just sent.
    """
    path = p.agent_path(name)
    try:
        info = op_agent.info(str(path))
    except Exception as exc:
        info = {"name": name, "error": str(exc)}
    info["name"] = name
    info["path"] = str(path)
    info["home_is_default"] = home_is_default(name, info.get("home"))
    info["archived_at"] = p.archived_at("agents", name)
    if hosts is None:
        hosts = _host_patterns()
    info["problems"] = _agent_problems(info, hosts)
    info["valid"] = not info["problems"]
    return info


@bp.get("/agents")
def list_agents():
    p = _project()
    hosts = _host_patterns()
    return jsonify([
        _agent_payload(p, name, hosts)
        for name in p.agent_names(include_archived=_wants_archived())
    ])


@bp.get("/agents/<name>")
def get_agent(name):
    p = _project()
    if not p.agent_exists(name):
        raise ProjectError(f"no agent named [{name}]")
    info = _agent_payload(p, name)
    info["runs"] = [
        {"name": r.name, "workflow": r.workflow, "state": r.state}
        for r in p.list_runs(include_archived=True) if r.record.get("agent") == name
    ]
    return jsonify(info)


@bp.post("/agents")
def create_agent():
    """Make an agent, immediately, from whatever was sent -- usually nothing.

    Same shape as a workflow: `+ agent` posts an empty body and lands you on
    the result. There is no form in front of it because there is nothing a
    blank agent needs that cannot be defaulted, and a name and a home you were
    given are easier to correct in place than to invent on an empty screen.
    """
    b = _body()
    p = _project()
    name = slugify(b["name"]) if b.get("name") else generate_workflow_name(
        taken=p.agent_names(include_archived=True)
    )
    assert_valid_name(name, "agent name")
    if p.agent_exists(name):
        raise ProjectError(f"agent [{name}] already exists")
    p.initialize()
    op_agent.save_agent(
        path=str(p.agent_path(name)),
        home_uri=b.get("home") or default_agent_home(name),
        container=b.get("container") or None,
        runtime=(b.get("runtime") or "APPTAINER").upper(),
        setup_commands=b.get("setup_commands", list(DEFAULT_SETUP_COMMANDS)),
        globus_uuid=b.get("globus_uuid") or None,
    )
    return jsonify(_agent_payload(p, name)), 201


@bp.put("/agents/<name>")
def update_agent(name):
    """Save an agent whole -- see the update convention above.

    A `name` that differs from the url renames it, which for an agent is a file
    move plus a rewrite of the run records that point at it. Everything else is
    written as sent; a field left out keeps what is on disk, which is what lets
    the two the page does not draw (the container image, the gpu flags) survive
    an edit made in the browser.
    """
    b = _body()
    p = _project()
    if not p.agent_exists(name):
        raise ProjectError(f"no agent named [{name}]")
    current = op_agent.info(str(p.agent_path(name)))
    home = b.get("home") or current["home"]
    runtime = (b.get("runtime") or current["runtime"]).upper()
    # asserted before the move, not after: a rename that lands and a save that
    # is then refused would leave the object under a name the caller does not
    # know it is at, and its next read would 404
    assert home and home.strip(), "a home directory is required"
    assert runtime in set(op_agent.runtimes()), (
        f"unknown runtime [{runtime}]; expected one of {', '.join(op_agent.runtimes())}"
    )
    renamed = _renamed_to(b, name)
    if renamed is not None:
        name = p.rename_agent(name, renamed)["name"]
    op_agent.save_agent(
        path=str(p.agent_path(name)),
        home_uri=home,
        container=b.get("container") or current["container"],
        runtime=runtime,
        setup_commands=b.get("setup_commands", current["setup_commands"]),
        globus_uuid=b.get("globus_uuid", current["globus_uuid"]),
    )
    return jsonify(_agent_payload(p, name))


@bp.delete("/agents/<name>")
def delete_agent(name):
    return jsonify(_project().delete_agent(name))


@bp.post("/agents/<name>/archive")
def archive_agent(name):
    archived = bool(_body().get("archived", True))
    return jsonify({"name": name, "archived_at": _project().set_archived("agents", name, archived)})


@bp.post("/agents/<name>/ping")
def ping_agent(name):
    p = _project()
    if not p.agent_exists(name):
        raise ProjectError(f"no agent named [{name}]")
    return jsonify(op_agent.ping(str(p.agent_path(name)), timeout_s=int(_body().get("timeout", 15))))


@bp.post("/agents/<name>/deploy")
def deploy_agent(name):
    p = _project()
    if not p.agent_exists(name):
        raise ProjectError(f"no agent named [{name}]")
    path = str(p.agent_path(name))
    assertive = bool(_body().get("assertive", False))

    def _work(job):
        with LogCapture(job):
            return op_agent.deploy(path, assertive)

    job = _jobs().submit("deploy", f"deploy {name}", _work, subject={"agent": name})
    return jsonify(job.summary()), 202


# -- workflows ---------------------------------------------------------------


def _workflow_summary(wf) -> dict:
    p = _project()
    runs = p.list_runs(workflow=wf.name, include_archived=True)
    return {
        "name": wf.name,
        "path": str(wf.path),
        "created_at": wf.request.get("created_at"),
        "archived_at": wf.archived_at,
        "forked_from": wf.request.get("forked_from"),
        "planned": wf.planned,
        "success": wf.ok,
        "task_key": wf.task_key,
        "step_count": wf.result.get("step_count"),
        "generated_at": wf.result.get("generated_at"),
        "run_count": len(runs),
        "live_runs": sum(1 for r in runs if r.live),
    }


@bp.get("/workflows")
def list_workflows():
    return jsonify([_workflow_summary(wf) for wf in _project().list_workflows(_wants_archived())])


@bp.get("/workflows/<name>")
def get_workflow(name):
    p = _project()
    wf = p.read_workflow(name)
    out = _workflow_summary(wf)
    out["request"] = wf.request
    # backfill for results written before the summary existed, and for anything
    # planned by the CLI directly into a workflow directory
    if wf.ok and not wf.result.get("step_display"):
        display = _step_display(wf.path)
        if display:
            wf = p.write_result(name, wf.result | {"step_display": display})
    out["result"] = wf.result
    out["runs"] = [_run_summary(r) for r in p.list_runs(workflow=name, include_archived=True)]
    lib_path = p.input_library_path(name)
    out["input_library"] = {"path": str(lib_path), "exists": lib_path.is_dir()}
    return jsonify(out)


@bp.post("/workflows")
def create_workflow():
    """Create a workflow and its live input library.

    The library is created up front, before anything is planned, because it is
    what the user edits -- the frozen copy inside the task bundle only appears
    once a plan succeeds, and a failed solve produces no bundle at all.
    """
    b = _body()
    p = _project()
    name = slugify(b["name"]) if b.get("name") else None
    wf = p.create_workflow(name=name, request={
        k: v for k, v in b.items()
        if k in {"sample_type", "target_types", "transform_libraries", "resource_libraries"}
    })

    types = b.get("type_libraries")
    if types is None:
        types = stdlib.discover(p.root)["data_types"]
    op_data.create_library(str(wf.path / INPUT_LIBRARY_DIRNAME), type_library_paths=types)
    return jsonify(_workflow_summary(p.read_workflow(wf.name))), 201


@bp.put("/workflows/<name>")
def put_workflow(name):
    """Save a workflow whole -- see the update convention above.

    The object here is its request: the recipe, the libraries, the name. A
    `name` that differs from the url renames it, under the conditions
    `store.rename_workflow` holds -- a workflow is created the moment it is
    asked for, under a made-up name, so this is where a user corrects it.
    """
    p = _project()
    b = _body()
    renamed = _renamed_to(b, name)
    if renamed is not None:
        name = p.rename_workflow(name, renamed).name
    request_fields = {k: v for k, v in b.items() if k != "name"}
    wf = p.write_request(name, request_fields) if request_fields else p.read_workflow(name)
    return jsonify(_workflow_summary(wf))


@bp.patch("/workflows/<name>")
def patch_workflow(name):
    """The partial form, kept for callers that predate the convention."""
    return jsonify(_workflow_summary(_project().write_request(name, _body())))


@bp.post("/workflows/<name>/rename")
def rename_workflow(name):
    """The rename-only form, kept for callers that predate the convention."""
    new_name = slugify(_body().get("name") or "")
    assert new_name, "a name is required"
    return jsonify(_workflow_summary(_project().rename_workflow(name, new_name)))


@bp.delete("/workflows/<name>")
def delete_workflow(name):
    return jsonify(_project().delete_workflow(name))


@bp.post("/workflows/<name>/archive")
def archive_workflow(name):
    archived = bool(_body().get("archived", True))
    return jsonify({"name": name, "archived_at": _project().set_archived("workflows", name, archived)})


@bp.post("/workflows/<name>/fork")
def fork_workflow(name):
    """Copy a workflow under a new input-library fork id.

    Identity is content-free by design, so a re-run against changed bytes at
    unchanged paths reuses the previous run's staged data. This is the explicit
    way out: the fork gets a different task key without anything reading the
    inputs. It also discards all cache reuse, which is the cost.
    """
    p = _project()
    source = p.read_workflow(name)
    new_name = slugify(_body().get("name") or "") or None
    forked = p.create_workflow(name=new_name, request=dict(source.request) | {
        "forked_from": name,
        "created_at": utcnow(),
    })
    op_data.fork_library(
        str(p.input_library_path(name)),
        str(p.input_library_path(forked.name)),
    )
    return jsonify(_workflow_summary(p.read_workflow(forked.name))), 201


@bp.post("/workflows/<name>/generate")
def generate_workflow(name):
    """Plan, and persist both halves of the outcome.

    On success the task bundle is written at the root of the workflow directory,
    which is what makes that directory a task reference the CLI can stage. On
    failure the request and the hints are still written, so the page can be
    reloaded back into the failure rather than losing it.
    """
    p = _project()
    wf = p.read_workflow(name)
    b = _body()
    request_body = wf.request | {
        k: v for k, v in b.items()
        if k in {"sample_type", "target_types", "transform_libraries", "resource_libraries"}
    }
    wf = p.write_request(name, request_body)

    # A sample type is optional: without one the inputs are planned as they
    # stand, as a single sample. The page does not offer one -- it is a way of
    # branching a plan into one run per item, not something a plan needs -- but
    # a request written by the CLI may carry one, and it is still honoured.
    sample_type = wf.request.get("sample_type")
    targets = wf.request.get("target_types") or []
    assert targets, "at least one target type is required"

    found = stdlib.discover(p.root)
    transforms = wf.request.get("transform_libraries") or found["transform_libraries"]
    resources = wf.request.get("resource_libraries") or found["resource_libraries"]
    lib_path = str(p.input_library_path(name))
    commit = found["commit"]

    def _work(job):
        with LogCapture(job):
            # a stale bundle from a previous generate must not outlive it: the
            # result the user sees and the bundle the CLI stages have to agree.
            for stale in ("task.yml", "data", "transforms"):
                target = wf.path / stale
                if target.is_dir():
                    shutil.rmtree(target)
                elif target.exists():
                    target.unlink()

            # Plan into a workspace private to this workflow. plan_workflow
            # always writes to <workspace>/<task_key>, and two workflows with
            # the same inputs deliberately produce the same key -- generating
            # both at once into a shared workspace would have them fighting
            # over one directory.
            staging = wf.path / ".staging"
            if staging.exists():
                shutil.rmtree(staging)
            with _plan_lock:
                result = op_workflow.plan_workflow(
                    data_library=lib_path,
                    sample_type=sample_type,
                    target_types=list(targets),
                    transform_libraries=list(transforms),
                    resource_libraries=list(resources) or None,
                    workspace=str(staging),
                )
            if result.get("success"):
                # promote the bundle to the workflow directory, so the readable
                # name is the address the CLI can stage
                staged = staging / result["task_key"]
                assert staged.is_dir(), f"planner wrote no bundle at [{staged}]"
                for item in staged.iterdir():
                    shutil.move(str(item), str(wf.path / item.name))
                result["step_display"] = _step_display(wf.path)
            if staging.exists():
                shutil.rmtree(staging)
            result["stdlib_commit"] = commit
            result["transform_libraries"] = list(transforms)
            result["resource_libraries"] = list(resources)
            p.write_result(name, result)
            return result

    job = _jobs().submit("generate", f"generate {name}", _work, subject={"workflow": name})
    return jsonify(job.summary()), 202


def _step_display(bundle: Path) -> list[dict]:
    """A readable summary of the plan's steps.

    The packed form of a step is a wire format -- instance ids and a dependency
    map -- with nothing a person would want to read. The step objects themselves
    carry `uses` and `produces`, so the summary is built once at generate time
    and stored beside the result.
    """
    from ..ops import workspace as op_workspace

    try:
        task = op_workspace.load_task(None, str(bundle))
    except Exception:
        return []
    out = []
    for step in task.plan.steps:
        out.append({
            "order": step.order,
            "transform": Path(str(step.transform._path)).name,
            "library": step.transform_library.GetKey(),
            "uses": sorted({inst.dtype_name for inst in step.uses}),
            "produces": sorted({
                inst.dtype_name for group in step.produces for inst in group
            }),
        })
    out.sort(key=lambda s: s["order"])
    return out


@bp.get("/workflows/<name>/dag")
def workflow_dag(name):
    """Render the plan's DAG. Cached beside the bundle; regenerated if missing."""
    p = _project()
    wf = p.read_workflow(name)
    if not wf.ok:
        raise ProjectError(f"workflow [{name}] has no successful plan to draw")
    svg = wf.path / "plan.dag.svg"
    if not svg.is_file():
        from ..ops import workspace as op_workspace
        task = op_workspace.load_task(None, str(wf.path))
        task.plan.RenderDAG(str(svg))
    return Response(svg.read_text(), mimetype="image/svg+xml")


# -- the input library -------------------------------------------------------


@bp.get("/workflows/<name>/inputs")
def get_inputs(name):
    p = _project()
    lib_path = p.input_library_path(name)
    if not lib_path.is_dir():
        raise ProjectError(f"workflow [{name}] has no input library")
    info = op_data.inspect_library(str(lib_path))
    info["items"] = [
        op_data.show_item_lineage(str(lib_path), item["path"]) for item in info["items"]
    ]
    return jsonify(info)


@bp.post("/workflows/<name>/inputs/items")
def add_input(name):
    b = _body()
    lib_path = str(_project().input_library_path(name))
    dtype = b.get("dtype")
    assert dtype, "a data type is required"
    parents = b.get("parents") or None
    if b.get("value") is not None:
        return jsonify(op_data.add_value(
            lib_path, b.get("name") or "", b["value"], dtype, parents,
        )), 201
    assert b.get("path"), "either a path or a value is required"
    return jsonify(op_data.add_item(lib_path, b["path"], dtype, parents)), 201


@bp.delete("/workflows/<name>/inputs/items")
def remove_input(name):
    item = request.args.get("path")
    assert item, "path is required"
    return jsonify(op_data.remove_item(str(_project().input_library_path(name)), item))


@bp.put("/workflows/<name>/inputs/items/parents")
def set_input_parents(name):
    """The item's lineage becomes exactly what is sent.

    A replacement, not an addition: the page edits lineage after the fact, and
    the additive `set_item_parents` the CLI uses cannot take a link back -- a
    parent unticked would have stayed on. PUT is already the right verb for it.
    """
    b = _body()
    return jsonify(op_data.replace_item_parents(
        str(_project().input_library_path(name)), b["path"], b.get("parents") or [],
    ))


@bp.put("/workflows/<name>/inputs/items/type")
def retype_input(name):
    """The row is that type instead. PUT for the same reason parents is: it
    replaces a property of a row that already exists."""
    b = _body()
    assert b.get("path"), "path is required"
    assert b.get("dtype"), "a data type is required"
    return jsonify(op_data.retype_item(
        str(_project().input_library_path(name)), b["path"], b["dtype"],
    ))


@bp.put("/workflows/<name>/inputs/items/path")
def repoint_input(name):
    """The row points somewhere else.

    Deliberately not `rename`: for an absolute entry -- a pointer to the user's
    own file -- nothing on disk moves. Only a library-owned (relative) entry is
    a real rename, and `repoint_item` is the one that knows the difference.
    """
    b = _body()
    assert b.get("path"), "path is required"
    assert b.get("new_path"), "a new path is required"
    return jsonify(op_data.repoint_item(
        str(_project().input_library_path(name)), b["path"], b["new_path"],
    ))


@bp.post("/workflows/<name>/inputs/types")
def attach_input_types(name):
    b = _body()
    return jsonify(op_data.attach_type_library(
        str(_project().input_library_path(name)),
        b["path"], b.get("namespace"), b.get("on_exist", "skip"),
    ))


# -- runs --------------------------------------------------------------------


def _run_summary(r) -> dict:
    return {
        "name": r.name,
        "workflow": r.workflow,
        "path": str(r.path),
        "state": r.state,
        "live": r.live,
        "archived_at": r.archived_at,
        **{k: r.record.get(k) for k in (
            "agent", "task_key", "created_at", "launched_at", "finished_at",
            "collected_at", "run_number", "preset", "error",
        )},
    }


@bp.get("/runs")
def list_runs():
    workflow = request.args.get("workflow")
    return jsonify([
        _run_summary(r) for r in _project().list_runs(workflow, _wants_archived())
    ])


@bp.get("/runs/<workflow>/<run>")
def get_run(workflow, run):
    p = _project()
    rec = p.read_run(workflow, run)
    out = _run_summary(rec)
    out["record"] = rec.record
    outputs = p.outputs_path(workflow, run)
    out["outputs"] = {"path": str(outputs), "collected": _has_results(outputs)}
    return jsonify(out)


def _has_results(outputs: Path) -> bool:
    return outputs.is_dir() and any(outputs.iterdir())


@bp.post("/runs")
def create_run():
    """Stage and launch. One button, because a staged-but-unlaunched run is not
    a state a user has any use for."""
    b = _body()
    p = _project()
    workflow = b.get("workflow")
    agent_name = b.get("agent")
    assert workflow, "workflow is required"
    assert agent_name, "agent is required"
    wf = p.read_workflow(workflow)
    if not wf.ok:
        raise ProjectError(f"workflow [{workflow}] has no successful plan to run")
    if not p.agent_exists(agent_name):
        raise ProjectError(f"no agent named [{agent_name}]")
    # an agent is saveable while it is still being filled in; this is the point
    # where the missing half stops being a work-in-progress and starts being a
    # staging that would fail on the host, several minutes from now
    problems = _agent_payload(p, agent_name)["problems"]
    if problems:
        raise ProjectError(
            f"agent [{agent_name}] is not ready to run on: {'; '.join(problems)}"
        )

    agent_path = str(p.agent_path(agent_name))
    rec = p.create_run(workflow, {
        "agent": agent_name,
        "preset": b.get("preset"),
        "params": b.get("params"),
        "resource_overrides": b.get("resource_overrides"),
        "on_exist": b.get("on_exist", "update"),
    })
    run_name = rec.name
    # bind before submitting: the job body runs on a worker thread, with no
    # request and no application context
    watcher = current_app.config["MSM_WATCHER"]

    def _work(job):
        with LogCapture(job):
            try:
                staged = op_runtime.stage(
                    agent_path, str(wf.path), rec.record.get("on_exist", "update"), None,
                )
                p.update_run(workflow, run_name, {
                    "state": "staged", "task_key": staged["task_key"],
                })
                p.update_run(workflow, run_name, {"state": "launching"})
                op_runtime.run(
                    agent_path,
                    staged["task_key"],
                    config_preset=rec.record.get("preset"),
                    params=rec.record.get("params"),
                    resource_overrides=rec.record.get("resource_overrides"),
                )
                runs = op_runtime.list_runs(agent_path, staged["task_key"])
                p.update_run(workflow, run_name, {
                    "state": "running",
                    "launched_at": utcnow(),
                    "run_number": runs[-1].get("run") if runs else None,
                })
            except Exception as exc:
                p.update_run(workflow, run_name, {
                    "state": "failed", "error": str(exc), "finished_at": utcnow(),
                })
                raise
            watcher.watch(workflow, run_name)
            return {"workflow": workflow, "run": run_name}

    job = _jobs().submit(
        "run", f"launch {run_name}", _work, subject={"workflow": workflow, "run": run_name},
    )
    return jsonify({"run": _run_summary(rec), "job": job.summary()}), 202


@bp.get("/runs/<workflow>/<run>/log")
def run_log(workflow, run):
    """Tail the agent-side log. Each call is an SSH round trip for a remote
    agent, so the page polls this rather than holding a stream open."""
    p = _project()
    rec = p.read_run(workflow, run)
    agent_name = rec.record.get("agent")
    if not agent_name or not p.agent_exists(agent_name):
        return jsonify({"lines": [], "error": f"agent [{agent_name}] is gone"}), 200
    key = rec.record.get("task_key")
    if not key:
        return jsonify({"lines": []})
    try:
        out = op_runtime.tail(
            str(p.agent_path(agent_name)),
            key,
            source=request.args.get("source", "agent"),
            lines=int(request.args.get("lines", 200)),
            run=rec.record.get("run_number"),
        )
    except Exception as exc:
        return jsonify({"lines": [], "error": str(exc)})
    return jsonify(out)


@bp.post("/runs/<workflow>/<run>/cancel")
def cancel_run(workflow, run):
    p = _project()
    rec = p.read_run(workflow, run)
    agent_name = rec.record.get("agent")
    if not agent_name or not p.agent_exists(agent_name):
        raise ProjectError(f"agent [{agent_name}] is gone; cannot cancel remotely")
    out = op_runtime.cancel(str(p.agent_path(agent_name)), rec.record["task_key"])
    p.update_run(workflow, run, {"state": "cancelled", "finished_at": utcnow()})
    return jsonify(out)


@bp.post("/runs/<workflow>/<run>/collect")
def collect_run(workflow, run):
    """Pull results back over SSH into this run's own outputs folder."""
    p = _project()
    rec = p.read_run(workflow, run)
    agent_name = rec.record.get("agent")
    if not agent_name or not p.agent_exists(agent_name):
        raise ProjectError(f"agent [{agent_name}] is gone; cannot collect")
    agent_path = str(p.agent_path(agent_name))
    key = rec.record["task_key"]
    dest = p.outputs_path(workflow, run)
    dest.mkdir(parents=True, exist_ok=True)

    def _work(job):
        with LogCapture(job):
            # a fast local transfer logs nothing at all, and a job log that stays
            # empty reads as "nothing happened" rather than "already done"
            job.emit(f"collecting results for [{key}] from agent [{agent_name}]")
            job.emit(f"into [{dest}]")
            # no globus: the whole remote path here is ssh
            out = op_runtime.collect(agent_path, key, str(dest), allow_globus=False)
            for src, dst in out.get("completed", []):
                job.emit(f"transferred [{src}] -> [{dst}]")
            for err in out.get("errors", []):
                job.emit(f"ERROR: {err}")
            job.emit(f"collected {len(out.get('completed', []))} item(s)")
            p.update_run(workflow, run, {"collected_at": utcnow()})
            return out

    job = _jobs().submit(
        "collect", f"collect {run}", _work, subject={"workflow": workflow, "run": run},
    )
    return jsonify(job.summary()), 202


@bp.get("/runs/<workflow>/<run>/results")
def run_results(workflow, run):
    """List the collected result library.

    Items with absolute paths are inputs the plan referenced, not things the run
    produced, so they are filtered out -- the same rule the tutorial applies.
    """
    p = _project()
    outputs = p.outputs_path(workflow, run)
    if not _has_results(outputs):
        return jsonify({"collected": False, "items": [], "path": str(outputs)})
    try:
        info = op_data.inspect_library(str(outputs))
    except Exception as exc:
        return jsonify({"collected": True, "items": [], "path": str(outputs), "error": str(exc)})
    items = [i for i in info["items"] if not Path(i["path"]).is_absolute()]
    return jsonify({"collected": True, "path": str(outputs), "items": items})


@bp.delete("/runs/<workflow>/<run>")
def delete_run(workflow, run):
    return jsonify(_project().delete_run(workflow, run))


@bp.post("/runs/<workflow>/<run>/archive")
def archive_run(workflow, run):
    archived = bool(_body().get("archived", True))
    return jsonify({
        "name": run,
        "archived_at": _project().set_archived("runs", f"{workflow}/{run}", archived),
    })


@bp.get("/agents/<name>/presets")
def agent_presets(name):
    p = _project()
    if not p.agent_exists(name):
        raise ProjectError(f"no agent named [{name}]")
    return jsonify(op_runtime.list_presets(str(p.agent_path(name))))


# -- jobs --------------------------------------------------------------------


@bp.get("/jobs")
def list_jobs():
    for key in ("workflow", "run", "agent"):
        if key in request.args:
            return jsonify(_jobs().list(key, request.args[key]))
    return jsonify(_jobs().list())


@bp.get("/jobs/<job_id>")
def get_job(job_id):
    job = _jobs().get(job_id)
    if job is None:
        raise ProjectError(f"no job [{job_id}]")
    return jsonify(job.summary() | {"lines": job.lines()})


@bp.get("/jobs/<job_id>/stream")
def stream_job(job_id):
    """Server-sent events. Requires a threaded server, or one open stream
    blocks every other request and the page looks hung."""
    job = _jobs().get(job_id)
    if job is None:
        raise ProjectError(f"no job [{job_id}]")

    def _events():
        for line in job.subscribe():
            yield f"data: {json.dumps({'line': line})}\n\n"
        yield f"event: end\ndata: {json.dumps(job.summary())}\n\n"

    return Response(
        _events(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
