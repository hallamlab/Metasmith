"""The REST surface, one blueprint per nav section.

This is a second thin veneer over `metasmith.ops`, exactly as the CLI is: every
route calls those functions directly and none of them shells out to the command
line. Where a route needs something the ops layer does not expose -- a project
layout, a readable name, an SSH config -- that lives in a sibling module here,
never in a route body.
"""
from __future__ import annotations

import json
import os
import shutil
import threading
from fnmatch import fnmatch
from pathlib import Path

from flask import Blueprint, Response, current_app, jsonify, request

from ..agents import Spec, Template
from ..models.dag_renderer import THEMES
from ..models.workflow import NextflowProcessName
from ..ops import agent as op_agent
from ..ops import data as op_data
from ..ops import runtime as op_runtime
from ..ops import samples as op_samples
from ..ops import workflow as op_workflow
from . import stdlib
from .jobs import LogCapture
from .names import (
    AGENT_LOCAL_HOST,
    agent_sort_name,
    assert_valid_name,
    compose_agent_name,
    generate_agent_name,
    slugify,
)
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


def agent_host_of(home: str | None) -> str | None:
    """Which machine a home is on, as an auto-composed name spells it.

    `None` means "remote, but no host chosen yet" -- an agent halfway through
    being pointed at a cluster. That is not a machine to name anything after, so
    the callers that recompose a name leave it alone until one is picked.
    """
    home = (home or "").strip()
    if not home.startswith("ssh://"):
        return AGENT_LOCAL_HOST
    return home[len("ssh://"):].partition(":")[0].strip() or None


def rehome(home: str, name: str) -> str:
    """The default home for `name`, keeping whatever machine `home` names."""
    path = default_agent_home(name)
    if not home.startswith("ssh://"):
        return path
    return f"ssh://{home[len('ssh://'):].partition(':')[0]}:{path}"


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

    A new agent is local until it is pointed somewhere, so the name offered here
    is the local one -- and it is only ever a preview: `POST /agents` with no
    name generates its own, together with the sort key that goes with it.
    """
    p = _project()
    _, name, _ = generate_agent_name(
        AGENT_LOCAL_HOST, taken=p.agent_names(include_archived=True)
    )
    return jsonify({
        "name": name,
        "home": default_agent_home(name),
        "home_prefix": DEFAULT_AGENT_HOME_PREFIX,
        "runtime": "APPTAINER",
        "runtimes": op_agent.runtimes(),
        # The image an agent gets when it names none. Reported so the page can
        # draw it: it is built from this metasmith's version *and build hash*,
        # so a copy running from a working tree names an image that was never
        # published, and the only cure is being able to see and change it.
        "container": op_agent.default_container(),
        "presets": op_agent.config_presets(),
        "setup_commands": list(DEFAULT_SETUP_COMMANDS),
    })


@bp.get("/defaults/agent/name")
def agent_name_suggestion():
    """A fresh auto-name for an agent on `host` (default: this machine).

    What the regenerate button beside an agent's name reads. It only *offers* a
    name -- nothing is renamed until the form is saved -- and it returns the
    naming record that goes with it, because a name adopted from here is an
    auto name: sending it back on the save is what re-arms the agent to follow
    its host again after someone typed over it once.
    """
    host = (request.args.get("host") or "").strip() or AGENT_LOCAL_HOST
    p = _project()
    prefix, name, sort_name = generate_agent_name(
        host, taken=p.agent_names(include_archived=True)
    )
    return jsonify({
        "name": name,
        "prefix": prefix,
        "sort_name": sort_name,
        "home": default_agent_home(name),
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
    """Follow an alias rename into every agent whose home names it.

    An auto-named agent carries the alias in its own name, so it is renamed too
    -- but its *home path* is left exactly as it was. The directory on that
    machine has not moved, and a deployed agent whose home silently followed its
    name would be pointing at somewhere nothing was ever installed.
    """
    project = _project()
    moved = []
    for name in _agents_on_host(old_alias):
        info = op_agent.info(str(project.agent_path(name)))
        _, _, path = info["home"][len("ssh://"):].partition(":")
        naming = project.agent_naming(name)
        label = name
        if naming:
            want = compose_agent_name(naming["prefix"], new_alias)
            if want == name:
                pass
            elif project.agent_exists(want):
                project.forget_agent_naming(name)
            else:
                project.rename_agent(name, want)
                project.set_agent_naming(
                    want, naming["prefix"], agent_sort_name(naming["prefix"], new_alias)
                )
                label = f"{name} → {want}"
                name = want
        op_agent.save_agent(
            path=str(project.agent_path(name)),
            home_uri=f"ssh://{new_alias}:{path}",
            container=info["container"],
            runtime=info["runtime"],
            setup_commands=info["setup_commands"],
            globus_uuid=info["globus_uuid"],
            default_preset=info["default_preset"],
            default_params=info["default_params"],
            # the machine and the directory are unchanged; only the alias moved
            renaming_host=True,
        )
        moved.append(label)
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


def _checked_preset(value: str | None) -> str | None:
    """The preset an agent declares, or nothing, which means the built-in local.

    Checked rather than reported, unlike the rest of an agent's incompleteness:
    the options are a fixed list the page picks from, so a value outside it is a
    bad request rather than a form still being filled in.
    """
    preset = (value or "").strip() or None
    if preset is not None:
        known = op_agent.config_presets()
        assert preset in known, (
            f"unknown nextflow preset [{preset}]; expected one of {', '.join(known)}"
        )
    return preset


def _param_value(v):
    """Text typed into a box, given the type it looks like.

    One rule, so it is the same everywhere: a value that reads as a JSON scalar
    becomes that scalar, anything else stays the string it was. `50` reaches
    nextflow as a number, `--partition=x` as a string, and `"50"` as a string on
    purpose -- which is the escape hatch for the one case the rule gets wrong.
    """
    if not isinstance(v, str): return v
    s = v.strip()
    if not s: return v
    try:
        parsed = json.loads(s)
    except ValueError:
        return v
    if isinstance(parsed, (dict, list)): return v
    return parsed


def _checked_params(raw, what: str = "params") -> dict | None:
    """Params as sent, with the unfilled rows dropped.

    A row with no name is a row someone has not finished typing, not a param
    named "" -- sending it would write a params file nextflow refuses.
    """
    if raw is None: return None
    assert isinstance(raw, dict), f"{what} must be a mapping of name to value"
    out = {}
    for k, v in raw.items():
        k = str(k).strip()
        if not k: continue
        out[k] = _param_value(v)
    return out


_OVERRIDE_FIELDS = {"cpus": int, "memory_gb": float, "duration_h": float}

# "no limit at all", as opposed to an empty box, which means "whatever the
# transform declared". A sentinel *string* rather than a JSON null so the two
# cannot collapse into each other on the wire: null and an absent key look the
# same to anything that drops empties, and this one has to survive that.
UNLIMITED = "unlimited"


def _checked_overrides(raw) -> dict | None:
    """Per-step resource overrides, with the untouched rows dropped.

    Keyed by step position, or by a transform name, or `all` -- the ops layer
    turns each into a nextflow selector. Rows whose boxes are all empty are
    dropped here so a page that draws one row per step can send them all.
    """
    if raw is None: return None
    assert isinstance(raw, dict), "resource_overrides must be a mapping keyed by step"
    out = {}
    for key, spec in raw.items():
        assert isinstance(spec, dict), f"resource override for [{key}] must be a mapping"
        kept = {}
        for field, cast in _OVERRIDE_FIELDS.items():
            v = spec.get(field)
            if v is None or (isinstance(v, str) and not v.strip()): continue
            if isinstance(v, str) and v.strip().lower() == UNLIMITED:
                assert field == "duration_h", f"[{field}] for step [{key}] cannot be unlimited"
                kept[field] = UNLIMITED
                continue
            try:
                kept[field] = cast(v)
            except (TypeError, ValueError):
                raise AssertionError(f"[{field}] for step [{key}] is not a number: [{v}]")
            assert kept[field] > 0, f"[{field}] for step [{key}] must be positive"
        if kept:
            out[str(key)] = kept
    return out or None


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
    # `sort_name` is set only for a name this side made up; an agent named by
    # hand is sorted as it was typed, and the absence *is* how the two are told
    # apart. The browser is told both so it can say which is which; it never has
    # to sort, because the list route is already in order.
    naming = p.agent_naming(name)
    info["sort_name"] = (naming or {}).get("sort_name")
    info["auto_named"] = naming is not None
    if hosts is None:
        hosts = _host_patterns()
    info["problems"] = _agent_problems(info, hosts)
    info["valid"] = not info["problems"]
    # Deliberately *not* one of `problems`, though it stops a run just as
    # surely. `problems` is "what is missing before this can be deployed", and
    # the deploy button reads it -- folding "has not been deployed" into that
    # list is a deadlock. `real_path` is what a deploy resolves on the host and
    # what re-pointing the home clears, so this answers for a remote agent as
    # cheaply as for a local one: no ssh, no stat, just the record.
    info["deployed"] = bool(info.get("real_path"))
    return info


def _agent_order(info: dict) -> str:
    return (info.get("sort_name") or info["name"]).casefold()


@bp.get("/agents")
def list_agents():
    """In sort-name order, so every agent on one machine sits with the others.

    Sorted here rather than in the browser: the sort key is stored, this is the
    side that holds it, and two clients ordering the same list two ways is the
    kind of difference nobody notices until it matters.
    """
    p = _project()
    hosts = _host_patterns()
    return jsonify(sorted(
        (
            _agent_payload(p, name, hosts)
            for name in p.agent_names(include_archived=_wants_archived())
        ),
        key=_agent_order,
    ))


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

    A made-up name says where the agent runs -- `blazing-local`, and
    `blazing-sockeye` once it is pointed somewhere -- and comes with a sort key
    that groups the list by machine. A name that was *sent* is the user's, and
    gets neither: it is taken as typed and sorted as typed, for good.
    """
    b = _body()
    p = _project()
    home = b.get("home") or None
    naming = None
    if b.get("name"):
        name = slugify(b["name"])
    else:
        host = agent_host_of(home) or AGENT_LOCAL_HOST
        prefix, name, sort_name = generate_agent_name(
            host, taken=p.agent_names(include_archived=True)
        )
        naming = (prefix, sort_name)
    assert_valid_name(name, "agent name")
    if p.agent_exists(name):
        raise ProjectError(f"agent [{name}] already exists")
    p.initialize()
    op_agent.save_agent(
        path=str(p.agent_path(name)),
        home_uri=home or default_agent_home(name),
        container=b.get("container") or None,
        runtime=(b.get("runtime") or "APPTAINER").upper(),
        setup_commands=b.get("setup_commands", list(DEFAULT_SETUP_COMMANDS)),
        globus_uuid=b.get("globus_uuid") or None,
        default_preset=_checked_preset(b.get("default_preset")),
        default_params=_checked_params(b.get("default_params"), "default_params"),
    )
    # after the save, not before: a naming record for an agent whose file failed
    # to write would outlive the thing it names
    if naming is not None:
        p.set_agent_naming(name, *naming)
    return jsonify(_agent_payload(p, name)), 201


@bp.put("/agents/<name>")
def update_agent(name):
    """Save an agent whole -- see the update convention above.

    A `name` that differs from the url renames it, which for an agent is a file
    move plus a rewrite of the run records that point at it. Everything else is
    written as sent; a field left out keeps what is on disk, which is what lets
    the two the page does not draw (the container image, the gpu flags) survive
    an edit made in the browser.

    Two things follow from the name being a field. Typing one is what makes an
    agent manually-named: its naming record is dropped and its name stops
    following its host, permanently. And an agent that still has that record and
    is pointed at a different machine is renamed *by this save* -- so the reply
    is the only thing that knows what it is now called, and the caller adopts it
    rather than the name it sent.
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
    preset = _checked_preset(b.get("default_preset", current["default_preset"]))
    params = _checked_params(
        b.get("default_params", current["default_params"]), "default_params",
    )
    assert home and home.strip(), "a home directory is required"
    assert runtime in set(op_agent.runtimes()), (
        f"unknown runtime [{runtime}]; expected one of {', '.join(op_agent.runtimes())}"
    )
    notes: list[str] = []
    renamed = _renamed_to(b, name)
    if renamed is not None:
        # the home is not moved to match: the caller knew the new name when it
        # sent this, so what it sent is what it wants. (The page does move it,
        # for a home it never wrote out -- an empty box is spelled as the
        # default one, and the default is made out of the name in the form.)
        p.forget_agent_naming(name)
        name = p.rename_agent(name, renamed)["name"]
        # ...unless the name came from `GET /defaults/agent/name` rather than
        # from a keyboard. Typing a name is what makes an agent manually-named;
        # regenerating one is the opposite gesture, and it has to be able to
        # undo that, or the button works once and then stops meaning anything.
        adopted = b.get("naming") or None
        if isinstance(adopted, dict) and adopted.get("prefix"):
            p.set_agent_naming(
                name,
                str(adopted["prefix"]),
                str(adopted.get("sort_name")
                    or agent_sort_name(adopted["prefix"], agent_host_of(home) or AGENT_LOCAL_HOST)),
            )
    else:
        naming = p.agent_naming(name)
        host = agent_host_of(home)
        # no host chosen yet is not a machine to be named after; the name waits
        if naming and host:
            want = compose_agent_name(naming["prefix"], host)
            following = True
            if want != name:
                # the home moves with the name only while it *is* the name --
                # an agent someone gave a path to keeps that path, and one that
                # never had one gets the default under its new machine
                was_default = home_is_default(name, home)
                try:
                    p.rename_agent(name, want)
                except ProjectError:
                    # something already holds the composed name. Keeping the
                    # current one loses nothing and overwrites nothing; the only
                    # thing missing is being told, so say it.
                    p.forget_agent_naming(name)
                    following = False
                    notes.append(
                        f"[{want}] is already taken, so this agent keeps the name "
                        f"[{name}] and is named by hand from now on"
                    )
                else:
                    notes.append(f"renamed [{name}] to [{want}], following its host")
                    name = want
                    if was_default:
                        home = rehome(home, name)
            if following:
                p.set_agent_naming(
                    name, naming["prefix"], agent_sort_name(naming["prefix"], host)
                )
    op_agent.save_agent(
        path=str(p.agent_path(name)),
        home_uri=home,
        container=b.get("container") or current["container"],
        runtime=runtime,
        setup_commands=b.get("setup_commands", current["setup_commands"]),
        globus_uuid=b.get("globus_uuid", current["globus_uuid"]),
        default_preset=preset,
        default_params=params,
    )
    return jsonify(_agent_payload(p, name) | {"notes": notes})


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


# -- templates ---------------------------------------------------------------
#
# A template is a workflow you start from: a spec whose input paths are
# DEFERRED, shipped in the standard library beside the transforms it names.
# There is no separate format to keep in step -- these routes read the same
# `Spec` a stored workflow record is, and creating from one is an ordinary
# create with that spec and its input rows.


def _target_names(targets) -> list[str]:
    """Target types as plain names, whichever of the two spellings was used."""
    return [t if isinstance(t, str) else str(t.get("type") or "") for t in targets or []]


def _templates(p) -> dict[str, Template]:
    found = stdlib.discover(p.root)
    if not found["present"]:
        return {}
    return {t.name: t for t in Template.Discover(found["path"])}


def _template(p, name: str) -> Template:
    tmpl = _templates(p).get(name)
    if tmpl is None:
        raise ProjectError(f"no template named [{name}]")
    return tmpl


def _template_dag_path(p, name: str, commit: str | None, theme: str) -> Path:
    """Where a template's drawing is cached.

    Keyed on the stdlib commit as much as on the name: a library pull can
    change what a template solves to, and a cache that ignored the commit would
    leave the modal drawing the previous graph with nothing to say it was
    stale. Files under an old commit simply stop being asked for.
    """
    stamp = (commit or "unversioned")[:12]
    return p.cache_dir / "template_dags" / stamp / f"{name}.{theme}.svg"


def _theme_arg() -> str:
    """An unknown theme falls back rather than raising -- it arrives from a url."""
    theme = request.args.get("theme", "light")
    return theme if theme in THEMES else "light"


@bp.get("/templates")
def list_templates():
    """The starting points on offer, cheaply: this reads yaml, never solves."""
    p = _project()
    commit = stdlib.discover(p.root)["commit"]
    theme = _theme_arg()
    return jsonify([
        {
            "name": t.name,
            "description": t.description,
            "sample_type": t.spec.sample_type,
            "target_types": _target_names(t.spec.target_types),
            # so the modal can show a cached drawing immediately and only
            # start a job for one it has never drawn
            "dag_ready": _template_dag_path(p, t.name, commit, theme).is_file(),
        }
        for t in _templates(p).values()
    ])


@bp.post("/templates/<name>/dag")
def render_template_dag(name):
    """Solve a template and draw it -- as a job, because a solve is seconds.

    It also holds `_plan_lock` for its whole duration, so a blocking route here
    would freeze every other page that plans. Cached on (template, stdlib
    commit, theme): paid once, and every later modal is served from disk.
    """
    p = _project()
    tmpl = _template(p, name)
    theme = _theme_arg()
    svg = _template_dag_path(p, name, stdlib.discover(p.root)["commit"], theme)
    if svg.is_file():
        return jsonify({"template": name, "theme": theme, "cached": True})

    def _work(job):
        with LogCapture(job):
            with _plan_lock:
                task = tmpl.spec.Solve()
            assert task.ok, (
                f"template [{name}] does not solve against this library: "
                f"dropped {sorted(task.plan.dropped_targets)}"
            )
            svg.parent.mkdir(parents=True, exist_ok=True)
            task.plan.RenderDAG(str(svg), theme=theme)
            return {
                "template": name, "theme": theme,
                "step_count": len(task.plan.steps),
            }

    job = _jobs().submit(
        "template_dag", f"draw {name}", _work, subject={"template": name},
    )
    return jsonify(job.summary()), 202


@bp.get("/templates/<name>/dag")
def template_dag(name):
    """The cached drawing. Absent until the job above has drawn it."""
    p = _project()
    _template(p, name)  # 4xx on an unknown name rather than a missing file
    theme = _theme_arg()
    svg = _template_dag_path(p, name, stdlib.discover(p.root)["commit"], theme)
    if not svg.is_file():
        raise ProjectError(f"template [{name}] has not been drawn for theme [{theme}] yet")
    return Response(svg.read_text(), mimetype="image/svg+xml")


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

    `template` names a starting point from the standard library. Because this
    is *new*, taking one is not a merge: the recipe is empty, so the template's
    spec is simply what the workflow starts as and its deferred rows are its
    input library.
    """
    b = _body()
    p = _project()
    name = slugify(b["name"]) if b.get("name") else None
    template = _template(p, b["template"]) if b.get("template") else None

    # A workflow record is a spec plus the store's bookkeeping, so what a create
    # may set is exactly the spec's fields -- named there rather than listed
    # again here.
    request_fields = {k: v for k, v in b.items() if k in set(Spec.FIELDS)}
    if template is not None:
        # resolved to this checkout's absolute paths on load; an explicit field
        # in the body still wins, so a caller can override what it starts from
        packed = template.spec.Pack()
        request_fields = {k: packed[k] for k in Spec.FIELDS} | request_fields
    wf = p.create_workflow(name=name, request=request_fields)

    types = b.get("type_libraries")
    if types is None:
        types = stdlib.discover(p.root)["data_types"]
    lib_path = str(wf.path / INPUT_LIBRARY_DIRNAME)
    if template is None:
        op_data.create_library(lib_path, type_library_paths=types)
    else:
        # Copied rather than re-added row by row: a deferred path is minted once
        # and identity follows it, so re-adding would give this workflow a task
        # key other than the one the template was validated at.
        op_data.copy_library(
            str(template.spec.input_library), lib_path, type_library_paths=types,
        )
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


def _given_summary(lib_path: str) -> list[dict]:
    """Every input the planner was handed, as type + identity + lineage.

    Deliberately read from the library at plan time rather than copied from the
    request: the request holds drafts and intentions, and what a plan succeeded
    or failed on is what was registered.
    """
    try:
        info = op_data.inspect_library(lib_path)
        items = [
            op_data.show_item_lineage(lib_path, item["path"], render=False)
            for item in info.get("items", [])
        ]
    except Exception:
        return []
    return [
        {
            "path": item.get("path"),
            "type": item.get("type_name"),
            # the closure, as the library reports it -- this is a readout, not
            # something anything writes back, so it is not collapsed
            "parents": [p.get("path") for p in (item.get("parents") or [])],
        }
        for item in items
    ]


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
    # A workflow record is a spec plus store bookkeeping, so what a generate may
    # change is exactly the spec's own fields -- named there rather than listed
    # again here, since a field added to one and not the other is silently
    # ignored on the way in.
    request_body = wf.request | {
        k: v for k, v in b.items() if k in set(Spec.FIELDS)
    }
    wf = p.write_request(name, request_body)

    # A sample type is optional: without one the inputs are planned as they
    # stand, as a single sample. It arrives here already decided -- from the
    # type of the row a sample table is indexed on, or from a request written by
    # the CLI -- and either way this route only honours it.
    sample_type = wf.request.get("sample_type")
    shared = wf.request.get("shared_input_paths") or None
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
            stale_names = ("task.yml", "data", "transforms")
            stale_names += tuple(_dag_cache_name(t) for t in THEMES)
            for stale in stale_names:
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
            # The record IS the spec, give or take the store's own bookkeeping
            # and the library defaults discovered above.
            spec = Spec.Unpack(
                wf.request | {
                    "transform_libraries": list(transforms),
                    "resource_libraries": list(resources),
                },
                input_library=lib_path,
            )
            with _plan_lock:
                result = op_workflow.plan_spec(spec, workspace=str(staging))
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
            # What the planner was actually given, read back off the library
            # rather than off the request. A failure is nearly always a wrong
            # *type* somewhere -- a row retyped by a stray click, a supertype
            # where a subtype was needed -- and the recipe above shows what the
            # browser believes rather than what the server planned from. So the
            # result says it in the server's own words, beside the hints.
            result["given"] = _given_summary(lib_path)
            result["targets"] = [
                {"type": t, "parents": []} if isinstance(t, str)
                else {"type": t.get("type", ""), "parents": list(t.get("parents") or [])}
                for t in targets
            ]
            result["sample_type"] = sample_type
            p.write_result(name, result)
            return result

    job = _jobs().submit("generate", f"generate {name}", _work, subject={"workflow": name})
    return jsonify(job.summary()), 202


def _dag_cache_name(theme: str) -> str:
    """Where a rendering is cached beside the bundle, one file per theme.

    The suffix has to stay `.svg`: `DagRenderer.render` derives the format from
    it. And the light name is the historical one, since the CLI stages that
    exact file into the bundle.
    """
    return "plan.dag.svg" if theme == "light" else f"plan.dag.{theme}.svg"


def _load_task(bundle: Path):
    """Read a staged bundle back, under the planner's lock.

    Loading a task imports every transform it names, through the same
    process-global machinery `plan_workflow` uses -- so it is subject to the
    same non-reentrancy and takes the same lock. It is not enough to hold it
    over the plan alone: a generate reading its own bundle back while a second
    generate was planning handed one of them a half-imported module, and the
    two report it differently -- a bare `spec not found`, or a transform that
    silently loaded as None.
    """
    from ..ops import workspace as op_workspace

    with _plan_lock:
        return op_workspace.load_task(None, str(bundle))


def _step_display(bundle: Path) -> list[dict]:
    """A readable summary of the plan's steps.

    The packed form of a step is a wire format -- instance ids and a dependency
    map -- with nothing a person would want to read. The step objects themselves
    carry `uses` and `produces`, so the summary is built once at generate time
    and stored beside the result.
    """
    try:
        task = _load_task(bundle)
    except Exception:
        return []
    out = []
    for step in task.plan.steps:
        # What the transform asks for, so an empty override box reads as "as
        # declared" rather than as "nothing". Rendered as plain numbers because
        # that is what an override is typed as.
        res = step.transform.resources
        declared = {}
        if res is not None:
            if res.cpus is not None: declared["cpus"] = res.cpus
            if res.memory is not None: declared["memory_gb"] = round(res.memory.value_gb, 3)
            if res.duration is not None:
                declared["duration_h"] = round(res.duration._delta.total_seconds() / 3600, 3)
        out.append({
            "order": step.order,
            "transform": Path(str(step.transform._path)).name,
            "declared_resources": declared,
            # The file name above is what a person recognises; this is what
            # nextflow calls the step, and the two need not be the same string.
            # A page building a resource selector must address this one.
            "process": NextflowProcessName(step.order, step.transform.name),
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
    # an unknown theme falls back rather than raising: the value arrives from a
    # url, and a malformed one should not turn the diagram into an error card
    theme = request.args.get("theme", "light")
    if theme not in THEMES:
        theme = "light"
    # the light drawing keeps the historical name, so a bundle staged by the
    # CLI and one drawn here are still one file; another theme is a sibling
    svg = wf.path / _dag_cache_name(theme)
    if not svg.is_file():
        task = _load_task(wf.path)
        task.plan.RenderDAG(str(svg), theme=theme)
    return Response(svg.read_text(), mimetype="image/svg+xml")


@bp.post("/dag/layout")
def dag_layout():
    """Place a graph the page built, so both drawings use the one engine.

    The info panel's nodes are buttons -- you click one to move the panel onto
    it -- so it cannot show the rendered SVG, and it used to run a layout of its
    own. Two engines meant the plan and the panel disagreed about the shape of
    the same graph. The page still *builds* its graph (what a node means, and
    which ones are plumbing, are content rules with another consumer); only the
    geometry comes from here.
    """
    b = _body()
    nodes = b.get("nodes") or []
    edges = b.get("edges") or []
    assert isinstance(nodes, list) and isinstance(edges, list), "nodes and edges must be lists"
    return jsonify(op_workflow.dag_geometry(
        nodes, edges,
        # COLUMN, so every label starts at one x, clear of the rails: the panel
        # draws a node as a row you click, and a row wants its text in a column.
        # BESIDE would put the label left of its own marker, which is the same
        # placement mirrored and not what the markup there is built for.
        label_mode=b.get("label_mode", "column"),
        font_size=float(b.get("font_size", 13.0)),
        max_label_chars=int(b.get("max_label_chars", 22)),
    ))


# -- the input library -------------------------------------------------------


@bp.get("/workflows/<name>/inputs")
def get_inputs(name):
    p = _project()
    lib_path = p.input_library_path(name)
    if not lib_path.is_dir():
        raise ProjectError(f"workflow [{name}] has no input library")
    info = op_data.inspect_library(str(lib_path))
    info["items"] = [
        # render=False: this maps over every item in the library, and the page
        # wants declared identity and manifest parents, not a trace walk each.
        op_data.show_item_lineage(str(lib_path), item["path"], render=False)
        for item in info["items"]
    ]
    # Which templated row registered each item, so the recipe can show a count
    # against that row instead of two hundred rows it did not ask for. The
    # attribution is the server's: the record of what an expansion put down is
    # the only place it is known.
    record = op_samples.read_record(str(lib_path))
    from_template = {
        path: tid for tid, paths in (record.get("generated") or {}).items() for path in paths
    }
    for item in info["items"]:
        item["template_id"] = from_template.get(item["path"])
    info["expansion"] = {
        "counts": {k: len(v) for k, v in (record.get("generated") or {}).items()},
        "row_count": record.get("row_count", 0),
        "sample_type": record.get("sample_type"),
        "index_id": record.get("index_id"),
    }
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


# -- the sample table --------------------------------------------------------


def _table_dir(name: str) -> Path:
    p = _project()
    p.read_workflow(name)  # refuses a name that is not a workflow
    return p.workflow_path(name)


def _drafts_of(name: str) -> list[dict]:
    """The recipe's input rows as the browser holds them.

    Templates are not a second list: a template *is* a draft whose path (or a
    value row's name or value) holds `{column}` tokens, so the two cannot get
    out of step with each other.
    """
    return list(_project().read_workflow(name).request.get("input_drafts") or [])


@bp.get("/workflows/<name>/table")
def get_table(name):
    table = op_samples.read_attached_table(_table_dir(name))
    if table is None:
        return jsonify({"attached": False})
    rows = _drafts_of(name)
    checked = op_samples.validate(str(_project().input_library_path(name)), table, rows)
    record = op_samples.read_record(str(_project().input_library_path(name)))
    return jsonify({
        "attached": True,
        "filename": table["filename"],
        "format": table["format"],
        "columns": table["columns"],
        "row_count": table["row_count"],
        # a peek, not the sheet: the page shows counts, never instances
        "preview": table["rows"][:5],
        "problems": checked["problems"],
        "index_id": checked["index_id"],
        "expansion": {
            "row_count": record.get("row_count", 0),
            "counts": {k: len(v) for k, v in (record.get("generated") or {}).items()},
            "sample_type": record.get("sample_type"),
            "stale": record.get("row_count", 0) != table["row_count"],
        },
    })


@bp.post("/workflows/<name>/table")
def attach_table(name):
    """Attach a sheet, uploaded as multipart or pasted as text.

    One route rather than two: which of the two the browser used is a transport
    detail, and the thing being created -- the attached table -- is the same.
    """
    where = _table_dir(name)
    upload = request.files.get("file") if request.files else None
    if upload is not None:
        data, filename = upload.read(), upload.filename
        fmt = request.form.get("format") or None
    else:
        b = _body()
        text = b.get("text")
        assert text, "paste a table, or upload one as a file"
        data, filename = str(text).encode(), b.get("filename")
        fmt = b.get("format") or None
    return jsonify(op_samples.attach_table(where, data, filename=filename, fmt=fmt)), 201


@bp.delete("/workflows/<name>/table")
def detach_table(name):
    """Take the sheet away. What it registered stays until it is cleared -- the
    two are separate gestures because one of them unregisters library items."""
    return jsonify(op_samples.detach_table(_table_dir(name)))


@bp.post("/workflows/<name>/table/expand")
def expand_table(name):
    """Register one item per (templated row x table row).

    Runs as a job: minting a leaf id blake3-hashes every input file that exists,
    so a two-hundred-sample sheet would otherwise be a click that hangs.
    """
    p = _project()
    table = op_samples.read_attached_table(_table_dir(name))
    assert table is not None, "no table is attached to this workflow"
    rows = _drafts_of(name)
    lib_path = str(p.input_library_path(name))

    def _work(job):
        with LogCapture(job):
            from ..logging import Log

            def _progress(done, total):
                Log.Info(f"registered {done}/{total} rows")

            return op_samples.expand(lib_path, table, rows, on_progress=_progress)

    job = _jobs().submit("expand", f"expand {name}", _work, subject={"workflow": name})
    return jsonify(job.summary()), 202


@bp.post("/workflows/<name>/table/clear")
def clear_table_expansion(name):
    return jsonify(op_samples.clear(str(_project().input_library_path(name))))


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
            # a run is reproducible only if it says what it was launched with
            "params", "resource_overrides",
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
    payload = _agent_payload(p, agent_name)
    problems = list(payload["problems"])
    # Deploy is the step that installs metasmith on that host, and it is
    # reachable only from its own button -- entirely skippable. Without this a
    # launch onto a fresh agent was accepted and then failed from inside
    # staging, minutes later, if it reported at all.
    if not payload["deployed"]:
        problems.append("has not been deployed yet")
    if problems:
        raise ProjectError(
            f"agent [{agent_name}] is not ready to run on: {'; '.join(problems)}"
        )

    agent_path = str(p.agent_path(agent_name))
    rec = p.create_run(workflow, {
        "agent": agent_name,
        "preset": b.get("preset"),
        # what is recorded is what was sent, coerced -- a run says what it was
        # launched with, and the agent's defaults are layered under it by
        # RunWorkflow rather than baked in here, so an agent edited later does
        # not rewrite the history of a run that already happened
        # `or None`: a mapping left empty once every unfilled row was dropped is
        # "no params", not "params, but blank" -- which would write an empty
        # params file where the agent used to write its placeholder one
        "params": _checked_params(b.get("params")) or None,
        "resource_overrides": _checked_overrides(b.get("resource_overrides")),
        "on_exist": b.get("on_exist", "update"),
        # `staging` and `launching` are owned by the thread below and by nothing
        # on disk, so the record has to say whose thread it was. Without this a
        # restart -- or a Ctrl-C, or a hung ssh that took the thread with it --
        # leaves the run claiming to be staging with nothing able to say
        # otherwise. The watcher reads it back.
        "launched_by": current_app.config.get("MSM_INSTANCE"),
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
                    # `index`, which is what ListWorkflowRuns returns -- this
                    # read `run`, a key it has never had, so every run recorded
                    # None and every probe fell back to `logs.latest`. On a
                    # re-run that symlink is the *other* run's directory.
                    "run_number": runs[-1].get("index") if runs else None,
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


def _collected_log_dir(outputs: Path) -> Path | None:
    """The timestamped log directory inside a collected result library.

    Never `logs.latest`: it is an alias for one of its own siblings, so taking
    it would name the same directory twice and, on a re-collect, could name a
    stale one. The newest real directory sorts last because the names are
    timestamps.
    """
    meta = outputs/"_metadata"
    if not meta.is_dir():
        return None
    dirs = sorted(
        p for p in meta.glob("logs.*") if p.is_dir() and p.name != "logs.latest"
    )
    return dirs[-1] if dirs else None


@bp.get("/runs/<workflow>/<run>/trace")
def run_trace(workflow, run):
    """Nextflow's per-task trace: which steps ran, and which of them died.

    A collected run answers this from its own copy of the log directory, so the
    common case costs no ssh. Only a run whose results are still on the agent
    goes over the wire.
    """
    p = _project()
    rec = p.read_run(workflow, run)
    local = _collected_log_dir(p.outputs_path(workflow, run))
    if local is not None:
        return jsonify(op_runtime.read_trace(local))
    agent_name = rec.record.get("agent")
    key = rec.record.get("task_key")
    if not key:
        return jsonify(op_runtime.read_trace("/nonexistent"))
    if not agent_name or not p.agent_exists(agent_name):
        out = op_runtime.read_trace("/nonexistent")
        out["error"] = f"agent [{agent_name}] is gone"
        return jsonify(out)
    try:
        return jsonify(op_runtime.trace(
            str(p.agent_path(agent_name)), key, rec.record.get("run_number"),
        ))
    except Exception as exc:
        out = op_runtime.read_trace("/nonexistent")
        out["error"] = str(exc)
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
            # An output whose link had no target on the agent: it is not at the
            # destination and never will be, so a folder that looks collected
            # holds nothing. Say so, and do not stamp the run collected.
            dangling = out.get("dangling", [])
            if dangling:
                already = set(out.get("errors", []))
                for d in dangling[:20]:
                    if d not in already:
                        job.emit(f"ERROR: no data behind [{d}]")
                raise ProjectError(
                    f"{len(dangling)} output(s) did not come across; the data they "
                    f"named is gone on the agent (first: {dangling[0]})"
                )
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
    return jsonify({
        "collected": True, "path": str(outputs), "items": items,
        "targets": _delivered_targets(p, workflow, items),
    })


def _delivered_targets(p, workflow: str, items: list[dict]) -> list[dict]:
    """What was asked for, against what actually came back.

    A run whose steps died under an ignoring error strategy finishes clean and
    is stamped completed, so the collected folder is the only place the loss is
    visible -- and only if something compares it to the request. Matching is by
    dtype *name* because that is what both sides record; the solver's property
    matching does not apply here, since these are the very types the plan was
    built to produce.
    """
    try:
        wanted = p.read_workflow(workflow).request.get("target_types") or []
    except Exception:
        return []
    have: dict[str, list[str]] = {}
    for i in items:
        have.setdefault(i["type_name"], []).append(i["path"])
    out = []
    for t in wanted:
        name = t.get("type") if isinstance(t, dict) else str(t)
        paths = have.get(name, [])
        out.append({"type": name, "count": len(paths), "paths": paths[:20]})
    return out


# -- browsing a collected result library -------------------------------------
#
# Everything here reads files the run produced, so every one of them goes
# through `_resolve_output` and nothing else ever joins a user string onto a
# path. The window sizes are the whole large-file policy: a preview is one
# window, and a file of any size costs one seek and one read.
PREVIEW_WINDOW = 256 * 1024   # one request's byte budget, and the "load more" step
PREVIEW_MAX_TOTAL = 8 * 1024 * 1024   # how much the page may accumulate
TREE_MAX_ENTRIES = 20000
TREE_MAX_DEPTH = 12
# What may be served with its own content type rather than as an attachment.
# SVG is in here because it is previewed through `<img>`, which does not run
# script; HTML deliberately is not, since nextflow's report carries its own.
_INLINE_TYPES = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml",
}


def _resolve_output(p, workflow: str, run: str) -> Path:
    """The run's outputs directory, resolved.

    Resolved, because the project may itself sit under a symlinked home and a
    containment test between a resolved candidate and an unresolved root
    answers no every time.
    """
    return p.outputs_path(workflow, run).resolve()


def _resolve_within(root: Path, rel: str) -> Path:
    """`rel` under `root`, or a refusal.

    Symlinks are followed on purpose -- `logs.latest/main.log` is a real thing
    to want -- so containment is asserted against the *target*, and with
    `relative_to` rather than a string prefix: `/x/outputs2` starts with
    `/x/outputs` and is not inside it.
    """
    if not rel or "\x00" in rel or Path(rel).is_absolute():
        raise ProjectError("that is not a path inside this run's results")
    try:
        cand = (root / rel).resolve(strict=True)
    except (OSError, RuntimeError):
        raise ProjectError(f"[{rel}] is not there")
    try:
        cand.relative_to(root)
    except ValueError:
        raise ProjectError("that is not a path inside this run's results")
    return cand


def _node(entry_path: Path, root: Path, name: str) -> dict:
    rel = entry_path.relative_to(root).as_posix()
    link = entry_path.is_symlink()
    try:
        st = entry_path.stat()   # follows the link: a copied output is a real file
        size, mtime, dangling = st.st_size, st.st_mtime, False
    except OSError:
        size, mtime, dangling = None, None, link
    # Anything under `_metadata` is bookkeeping rather than a product. The log
    # directory is called out separately because it is the audit trail collect
    # goes out of its way to bring across, and a reader looks for it by name.
    role = "output"
    if rel == "_metadata" or rel.startswith("_metadata/"):
        role = "log" if rel.startswith("_metadata/logs") else "metadata"
    return {
        "name": name, "path": rel,
        "type": "dir" if (entry_path.is_dir() and not dangling) else "file",
        "size": size, "mtime": mtime,
        "symlink": link, "dangling": dangling,
        "role": role, "type_name": None, "is_item": False,
        "children": [] if entry_path.is_dir() and not dangling else None,
    }


@bp.get("/runs/<workflow>/<run>/tree")
def run_tree(workflow, run):
    """The collected folder as it actually is on disk.

    Built by walking, not from the library manifest: `_metadata/` holds the
    per-step logs and is not a manifest entry, so a tree derived from the
    manifest could not show them at all.
    """
    p = _project()
    outputs = p.outputs_path(workflow, run)
    if not _has_results(outputs):
        return jsonify({"collected": False, "path": str(outputs), "root": None})
    root = outputs.resolve()
    budget = {"left": TREE_MAX_ENTRIES, "truncated": False}

    def walk(here: Path, depth: int) -> list[dict]:
        if depth >= TREE_MAX_DEPTH:
            budget["truncated"] = True
            return []
        try:
            entries = sorted(
                os.scandir(here), key=lambda e: (not e.is_dir(follow_symlinks=False), e.name),
            )
        except OSError:
            return []
        out = []
        for e in entries:
            if budget["left"] <= 0:
                budget["truncated"] = True
                break
            budget["left"] -= 1
            node = _node(Path(e.path), root, e.name)
            # A directory symlink is an alias for a sibling -- `logs.latest` is
            # one -- so descending it would carry the whole subtree twice under
            # two names. It is listed, and the sibling holds the contents.
            if node["type"] == "dir" and not node["symlink"]:
                node["children"] = walk(Path(e.path), depth + 1)
            out.append(node)
        return out

    tree = {
        "name": "", "path": "", "type": "dir", "role": "output",
        "size": None, "mtime": None, "symlink": False, "dangling": False,
        "type_name": None, "is_item": False, "children": walk(root, 0),
    }
    _tag_manifest_types(outputs, tree)
    # `_metadata` last: it is the bookkeeping, and a reader is looking for
    # products first.
    tree["children"].sort(key=lambda n: (n["role"] != "output", n["name"]))
    return jsonify({
        "collected": True, "path": str(outputs),
        "truncated": budget["truncated"], "root": tree,
    })


def _tag_manifest_types(outputs: Path, tree: dict) -> None:
    """Stamp the library's dtype names onto the nodes they name.

    Best effort: a folder that does not read as a library still gets a tree,
    the same way `/results` still answers with an `error` rather than a 500.
    A manifest key may name a directory, so this tags whatever node matches
    rather than assuming a leaf.
    """
    try:
        info = op_data.inspect_library(str(outputs))
    except Exception:
        return
    named = {
        i["path"]: i["type_name"] for i in info["items"]
        if not Path(i["path"]).is_absolute()
    }
    if not named:
        return
    # A library collected before the manifest recorded published locations
    # names its files by their cache-shard path, which is nowhere on disk. The
    # basename still matches, and it is unique within a run, so an old folder
    # keeps its type names instead of showing a column of blanks.
    by_name = {}
    for k, v in named.items():
        by_name.setdefault(Path(k).name, v)

    def visit(node):
        t = named.get(node["path"])
        if t is None and node["type"] == "file":
            t = by_name.get(node["name"])
        if t is not None:
            node["type_name"] = t
            node["is_item"] = True
        for c in node.get("children") or []:
            visit(c)

    visit(tree)


@bp.get("/runs/<workflow>/<run>/file")
def run_file(workflow, run):
    """One window of a file, addressed in bytes.

    Bytes rather than lines so that `load more` can resume from where it
    stopped: a line-addressed window has to re-read from zero to find line N,
    which is the one thing a 40 GB output cannot afford.
    """
    p = _project()
    root = _resolve_output(p, workflow, run)
    target = _resolve_within(root, request.args.get("path", ""))
    if not target.is_file():
        raise ProjectError("that is a directory, not a file")
    size = target.stat().st_size
    limit = max(1, min(int(request.args.get("limit", PREVIEW_WINDOW)), PREVIEW_WINDOW))
    mode = request.args.get("mode", "head")
    if mode == "tail":
        offset = max(0, size - limit)
    else:
        offset = max(0, min(int(request.args.get("offset", 0)), size))
    with open(target, "rb") as f:
        f.seek(offset)
        raw = f.read(limit)

    out = {
        "path": target.relative_to(root).as_posix(),
        "size": size, "mtime": target.stat().st_mtime,
        "offset": offset, "length": len(raw), "eof": offset + len(raw) >= size,
        "window": PREVIEW_WINDOW, "max_total": PREVIEW_MAX_TOTAL,
    }
    # A NUL in a text file is the cheap, reliable tell. Nothing is sniffed
    # beyond this window, because nothing beyond it is ever read.
    if b"\x00" in raw[:8192]:
        return jsonify(out | {"encoding": "binary", "text": None})
    text = raw.decode("utf-8", errors="replace")
    if text.count("�") > max(16, len(text) // 20):
        return jsonify(out | {"encoding": "binary", "text": None})
    # A window almost never lands on a line boundary. Trimming the partial ends
    # is cosmetic, so the byte counts are reported and the caller still pages by
    # `offset + length` -- never by counting the lines it was shown.
    dropped_head = dropped_tail = 0
    if offset > 0:
        cut = text.find("\n")
        if cut >= 0:
            dropped_head = len(text[: cut + 1].encode("utf-8"))
            text = text[cut + 1 :]
    if not out["eof"]:
        cut = text.rfind("\n")
        if cut >= 0:
            dropped_tail = len(text[cut + 1 :].encode("utf-8"))
            text = text[: cut + 1]
    return jsonify(out | {
        "encoding": "utf-8", "text": text,
        "dropped_head_bytes": dropped_head, "dropped_tail_bytes": dropped_tail,
    })


@bp.get("/runs/<workflow>/<run>/download")
def run_download(workflow, run):
    """The file itself, streamed.

    Everything outside a small media allowlist is served as an attachment with
    a generic type: these are files a pipeline wrote, and `nxf_report.html` in
    particular is same-origin HTML carrying its own script.
    """
    from flask import send_file

    p = _project()
    root = _resolve_output(p, workflow, run)
    target = _resolve_within(root, request.args.get("path", ""))
    if not target.is_file():
        raise ProjectError("that is a directory, not a file")
    mime = _INLINE_TYPES.get(target.suffix.lower())
    res = send_file(
        target, conditional=True,
        mimetype=mime or "application/octet-stream",
        as_attachment=mime is None, download_name=target.name,
    )
    res.headers["X-Content-Type-Options"] = "nosniff"
    return res


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
