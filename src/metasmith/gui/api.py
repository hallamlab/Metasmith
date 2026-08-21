from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import threading
from dataclasses import replace
from fnmatch import fnmatch
from pathlib import Path

from flask import Blueprint, Response, current_app, jsonify, request

from ..agents import Spec, Template
from ..hashing import KeyGenerator
from ..models.dag_renderer import THEMES
from ..models.paths import is_deferred
from ..models.workflow import NextflowProcessName
from ..ops import agent as op_agent
from ..ops import data as op_data
from ..ops import runtime as op_runtime
from ..ops import inputs as op_inputs
from ..ops import samples as op_samples
from ..ops import workflow as op_workflow
from . import recipe as op_recipe
from . import share as op_share
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

_LOG = logging.getLogger(__name__)

DEFAULT_AGENT_HOME_PREFIX = "~/msm."


def default_agent_home(agent_id: str) -> str:
    return f"{DEFAULT_AGENT_HOME_PREFIX}{agent_id}"


def agent_host_of(home: str | None) -> str | None:
    home = (home or "").strip()
    if not home.startswith("ssh://"):
        return AGENT_LOCAL_HOST
    return home[len("ssh://"):].partition(":")[0].strip() or None


def rehome(home: str, agent_id: str) -> str:
    path = default_agent_home(agent_id)
    if not home.startswith("ssh://"):
        return path
    return f"ssh://{home[len('ssh://'):].partition(':')[0]}:{path}"


def home_is_default(agent_id: str, home: str | None) -> bool:
    if not home:
        return False
    default = default_agent_home(agent_id)
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
    return not remote and Path(path) == Path(default).expanduser()


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


def _project() -> Project:
    return current_app.config["MSM_PROJECT"]


def _jobs():
    return current_app.config["MSM_JOBS"]


def _ssh() -> SshConfig:
    return current_app.config["MSM_SSH"]


@bp.errorhandler(ProjectError)
@bp.errorhandler(SshConfigError)
@bp.errorhandler(op_share.ShareError)
def _handle_refusal(exc):
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


def _renamed_to(body: dict, current: str, *, field: str = "name", slug: bool = True) -> str | None:
    if field not in body:
        return None
    wanted = str(body[field] or "").strip()
    wanted = slugify(wanted) if slug else wanted
    assert wanted, f"a {field} is required"
    return None if wanted == current else wanted


@bp.get("/health")
def health():
    return jsonify({"ok": True})


@bp.get("/project")
def get_project():
    from ..constants import CONDA_URL, CONTAINER_URL, DOCS_URL, GIT_URL, VERSION

    p = _project()
    return jsonify({
        "root": str(p.root),
        "version": VERSION,
        "stdlib": stdlib.discover(p.root),
        "links": {
            "docs": DOCS_URL,
            "github": GIT_URL,
            "conda": CONDA_URL,
            "container": CONTAINER_URL,
        },
    })


@bp.get("/defaults/agent")
def agent_defaults():
    p = _project()
    _, name, _ = generate_agent_name(
        AGENT_LOCAL_HOST, taken=p.agent_names(include_archived=True)
    )
    return jsonify({
        "name": name,
        "home": default_agent_home(KeyGenerator().GenerateUID(l=8)),
        "home_prefix": DEFAULT_AGENT_HOME_PREFIX,
        "runtime": "APPTAINER",
        "runtimes": op_agent.runtimes(),
        "container": op_agent.default_container(),
        "presets": op_agent.config_presets(),
        "setup_commands": list(DEFAULT_SETUP_COMMANDS),
    })


@bp.get("/defaults/agent/name")
def agent_name_suggestion():
    host = (request.args.get("host") or "").strip() or AGENT_LOCAL_HOST
    p = _project()
    prefix, name, sort_name = generate_agent_name(
        host, taken=p.agent_names(include_archived=True)
    )
    return jsonify({
        "name": name,
        "prefix": prefix,
        "sort_name": sort_name,
        "home": default_agent_home(KeyGenerator().GenerateUID(l=8)),
    })


@bp.get("/project/types")
def get_types():
    refresh = request.args.get("refresh", "") in {"1", "true", "yes"}
    return jsonify(stdlib.available_types(_project().root, refresh=refresh))


@bp.get("/project/type-index")
def get_type_index():
    with _plan_lock:
        return jsonify(stdlib.type_index(
            _project().root, refresh=request.args.get("refresh", "") in {"1", "true", "yes"},
        ))


@bp.post("/project/libraries/sync")
def sync_libraries():
    p = _project()

    def _work(job):
        with LogCapture(job):
            out = stdlib.update_stdlib(p.root)
            if out["updated"]:
                stdlib.resync_workflow_types(p)
            return out

    job = _jobs().submit("library-sync", "sync the standard library", _work)
    return jsonify(job.summary()), 202


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
    cfg = _ssh()
    host = cfg.find(alias)
    if host is None:
        raise SshConfigError(f"no host named [{alias}]")
    return jsonify({"identity": cfg.read_identity(host.keywords.get("identityfile"))})


@bp.post("/ssh/keys")
def ssh_generate_key():
    b = _body()
    alias = (b.get("alias") or "").strip()
    assert alias, "an alias is required to name the key"
    return jsonify(_ssh().generate_identity(alias, comment=b.get("comment")))


@bp.delete("/ssh/keys/<alias>")
def ssh_delete_key(alias):
    return jsonify(_ssh().delete_identity(alias))


@bp.put("/ssh/hosts/<alias>")
def ssh_put_host(alias):
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
    fields = {k: v for k, v in _body().items() if k != "alias"}
    return jsonify({"host": _ssh().update_host(alias, **fields)})


def _agents_on_host(alias: str) -> list[str]:
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
            renaming_host=True,
        )
        moved.append(label)
    return moved


@bp.delete("/ssh/hosts/<alias>")
def ssh_delete_host(alias):
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
    b = _body()
    cfg = _ssh()
    if "native" in b:
        cfg.write_all(b.get("managed", ""), b.get("native") or "")
    else:
        cfg.write_managed_block(b.get("managed", ""))
    return jsonify(_config_payload(cfg))


def _host_patterns() -> list[str]:
    return [e.pattern for e in _ssh().resolved()]


def _checked_preset(value: str | None) -> str | None:
    preset = (value or "").strip() or None
    if preset is not None:
        known = op_agent.config_presets()
        assert preset in known, (
            f"unknown nextflow preset [{preset}]; expected one of {', '.join(known)}"
        )
    return preset


def _param_value(v):
    return op_inputs.scalar(v)


def _checked_params(raw, what: str = "params") -> dict | None:
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
    if info.get("error"):
        return [f"this agent's file could not be read: {info['error']}"]
    problems = []
    home = info.get("home") or ""
    if info.get("home_type") == "SSH":
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
    path = p.agent_path(name)
    try:
        info = op_agent.info(str(path))
    except Exception as exc:
        info = {"name": name, "error": str(exc)}
    info["name"] = name
    info["path"] = str(path)
    info["home_is_default"] = home_is_default(info.get("id"), info.get("home"))
    info["archived_at"] = p.archived_at("agents", name)
    naming = p.agent_naming(name)
    info["sort_name"] = (naming or {}).get("sort_name")
    info["auto_named"] = naming is not None
    if hosts is None:
        hosts = _host_patterns()
    info["problems"] = _agent_problems(info, hosts)
    info["valid"] = not info["problems"]
    info["deployed"] = bool(info.get("real_path"))
    return info


def _agent_order(info: dict) -> str:
    return (info.get("sort_name") or info["name"]).casefold()


@bp.get("/agents")
def list_agents():
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
    agent_id = KeyGenerator().GenerateUID(l=8)
    op_agent.save_agent(
        path=str(p.agent_path(name)),
        home_uri=home or default_agent_home(agent_id),
        container=b.get("container") or None,
        runtime=(b.get("runtime") or "APPTAINER").upper(),
        setup_commands=b.get("setup_commands", list(DEFAULT_SETUP_COMMANDS)),
        globus_uuid=b.get("globus_uuid") or None,
        default_preset=_checked_preset(b.get("default_preset")),
        default_params=_checked_params(b.get("default_params"), "default_params"),
        id=agent_id,
    )
    if naming is not None:
        p.set_agent_naming(name, *naming)
    return jsonify(_agent_payload(p, name)), 201


@bp.put("/agents/<name>")
def update_agent(name):
    b = _body()
    p = _project()
    if not p.agent_exists(name):
        raise ProjectError(f"no agent named [{name}]")
    current = op_agent.info(str(p.agent_path(name)))
    home = b.get("home") or current["home"]
    runtime = (b.get("runtime") or current["runtime"]).upper()
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
        p.forget_agent_naming(name)
        name = p.rename_agent(name, renamed)["name"]
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
        if naming and host:
            want = compose_agent_name(naming["prefix"], host)
            following = True
            if want != name:
                was_default = home_is_default(current["id"], home)
                try:
                    p.rename_agent(name, want)
                except ProjectError:
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
                        home = rehome(home, current["id"])
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
            return op_agent.deploy(path, assertive, on_phase=lambda p: job.emit(f"PHASE:{p}"))

    job = _jobs().submit("deploy", f"deploy {name}", _work, subject={"agent": name})
    return jsonify(job.summary()), 202


USER_TEMPLATES_DIRNAME = "user_templates"


def _target_names(targets) -> list[str]:
    return [t if isinstance(t, str) else str(t.get("type") or "") for t in targets or []]


def _templates(p) -> dict[str, Template]:
    found = stdlib.discover(p.root)
    if not found["present"]:
        return {}
    return {t.name: t for t in Template.Discover(found["path"], libraries=found)}


def _user_templates(p) -> dict[str, Template]:
    if not (p.root / USER_TEMPLATES_DIRNAME).is_dir():
        return {}
    return {
        t.name: t for t in Template.Discover(
            p.root, dirname=USER_TEMPLATES_DIRNAME,
            libraries=stdlib.discover(p.root),
        )
    }


def _all_templates(p) -> dict[str, tuple[Template, str]]:
    out = {name: (t, "library") for name, t in _templates(p).items()}
    out.update({name: (t, "user") for name, t in _user_templates(p).items()})
    return out


def _template_version(p, name: str, source: str) -> str | None:
    if source == "library":
        return stdlib.discover(p.root)["commit"]
    path = Template.PathIn(p.root, name, dirname=USER_TEMPLATES_DIRNAME)
    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        mtime = 0
    return hashlib.sha1(f"user:{name}:{mtime}".encode()).hexdigest()


def _template_dag_path(p, name: str, version: str | None, theme: str) -> Path:
    stamp = (version or "unversioned")[:12]
    return p.cache_dir / "template_dags" / stamp / f"{name}.{theme}.svg"


def _theme_arg() -> str:
    theme = request.args.get("theme", "light")
    return theme if theme in THEMES else "light"


def _background_arg() -> bool:
    return request.args.get("background", "").lower() in {"1", "true", "yes"}


def _template_summary(p, name: str, tmpl: Template, source: str, theme: str) -> dict:
    return {
        "name": name,
        "description": tmpl.description,
        "sample_type": tmpl.spec.sample_type,
        "target_types": _target_names(tmpl.spec.target_types),
        "source": source,
        "problems": list(tmpl.unresolved),
        "dag_ready": _template_dag_path(p, name, _template_version(p, name, source), theme).is_file(),
    }


def _libraries_or_all(found: dict, transforms, resources) -> tuple[list, list]:
    return (
        list(transforms or found["transform_libraries"]),
        list(resources or found["resource_libraries"]),
    )


def _assert_resolved(name: str, tmpl: Template) -> None:
    assert not tmpl.unresolved, (
        f"template [{name}] names {', '.join(tmpl.unresolved)}, which this "
        f"project's standard library does not have"
    )


@bp.get("/templates")
def list_templates():
    p = _project()
    theme = _theme_arg()
    return jsonify([
        _template_summary(p, name, tmpl, source, theme)
        for name, (tmpl, source) in _all_templates(p).items()
    ])


def _render_template_dag(p, tmpl: Template, name: str, source: str, theme: str) -> tuple[Path, int]:
    svg = _template_dag_path(p, name, _template_version(p, name, source), theme)
    _assert_resolved(name, tmpl)
    transforms, resources = _libraries_or_all(
        stdlib.discover(p.root),
        tmpl.spec.transform_libraries, tmpl.spec.resource_libraries,
    )
    spec = replace(tmpl.spec, transform_libraries=transforms, resource_libraries=resources)
    with _plan_lock:
        task = spec.Solve()
    assert task.ok, (
        f"template [{name}] does not solve against this library: "
        f"dropped {sorted(task.plan.dropped_targets)}"
    )
    svg.parent.mkdir(parents=True, exist_ok=True)
    task.plan.RenderDAG(str(svg), theme=theme, background=False)
    return svg, len(task.plan.steps)


@bp.post("/templates/<name>/dag")
def render_template_dag(name):
    p = _project()
    tmpl, source = _all_templates(p).get(name, (None, None))
    if tmpl is None:
        raise ProjectError(f"no template named [{name}]")
    theme = _theme_arg()
    svg = _template_dag_path(p, name, _template_version(p, name, source), theme)
    if svg.is_file():
        return jsonify({"template": name, "theme": theme, "cached": True})

    def _work(job):
        with LogCapture(job):
            _, step_count = _render_template_dag(p, tmpl, name, source, theme)
            return {"template": name, "theme": theme, "step_count": step_count}

    job = _jobs().submit(
        "template_dag", f"draw {name}", _work, subject={"template": name},
    )
    return jsonify(job.summary()), 202


@bp.get("/templates/<name>/dag")
def template_dag(name):
    p = _project()
    tmpl, source = _all_templates(p).get(name, (None, None))
    if tmpl is None:
        raise ProjectError(f"no template named [{name}]")
    theme = _theme_arg()
    svg = _template_dag_path(p, name, _template_version(p, name, source), theme)
    if not svg.is_file():
        raise ProjectError(f"template [{name}] has not been drawn for theme [{theme}] yet")
    return Response(svg.read_text(), mimetype="image/svg+xml")


@bp.delete("/templates/<name>")
def delete_template(name):
    p = _project()
    if name not in _user_templates(p):
        if name in _templates(p):
            raise ProjectError(
                f"[{name}] ships with the standard library and cannot be deleted here"
            )
        raise ProjectError(f"no saved template named [{name}]")
    shutil.rmtree(Template.PathIn(p.root, name, dirname=USER_TEMPLATES_DIRNAME).parent)
    return jsonify({"name": name, "action": "deleted"})


def _workflow_summary(wf, runs=None) -> dict:
    if runs is None:
        runs = _project().list_runs(workflow=wf.name, include_archived=True)
    return {
        "name": wf.name,
        "display_name": wf.request.get("display_name"),
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
    p = _project()
    by_workflow: dict[str, list] = {}
    for r in p.list_runs(include_archived=True):
        by_workflow.setdefault(r.workflow, []).append(r)
    return jsonify([
        _workflow_summary(wf, runs=by_workflow.get(wf.name, []))
        for wf in p.list_workflows(_wants_archived())
    ])


@bp.get("/workflows/<name>")
def get_workflow(name):
    p = _project()
    wf = p.read_workflow(name)
    rows = _rows_of(name, wf=wf)
    runs = p.list_runs(workflow=name, include_archived=True)
    out = _workflow_summary(wf, runs=runs)
    out["request"] = wf.request
    if wf.ok and (
        not wf.result.get("step_display")
        or (wf.result.get("plan_graph") or {}).get("v") != op_workflow.GEOMETRY_VERSION
    ):
        display, plan_graph = _step_display(wf.path, p.root)
        if display:
            wf = p.write_result(name, wf.result | {
                "step_display": display, "plan_graph": plan_graph,
            })
    out["result"] = wf.result
    out["overrides"] = wf.overrides
    out["runs"] = [_run_summary(r) for r in runs]
    lib_path = wf.path / wf.request.get("input_library", INPUT_LIBRARY_DIRNAME)
    out["input_library"] = {"path": str(lib_path), "exists": lib_path.is_dir()}

    include = {s.strip() for s in request.args.get("include", "").split(",") if s.strip()}
    if include & {"inputs", "table"} and lib_path.is_dir():
        record = op_samples.read_record(str(lib_path))
        if "inputs" in include:
            out["inputs"] = _inputs_payload(lib_path, record=record)
        if "table" in include:
            out["table"] = _table_payload(name, lib_path, wf.path, rows=rows, record=record)
    return jsonify(out)


@bp.post("/workflows")
def create_workflow():
    b = _body()
    p = _project()
    name = slugify(b["name"]) if b.get("name") else None
    template = None
    if b.get("template"):
        found = _all_templates(p).get(b["template"])
        if found is None:
            raise ProjectError(f"no template named [{b['template']}]")
        template, _ = found
        _assert_resolved(b["template"], template)

    request_fields = {k: v for k, v in b.items() if k in set(Spec.FIELDS)}
    if template is not None:
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
        op_data.materialize_template(
            template.spec.input_library, lib_path, type_library_paths=types,
        )
    default_preset = op_agent.config_presets()
    if default_preset:
        source = "local" if "local" in default_preset else default_preset[0]
        p.write_preset(wf.name, op_agent.preset_content(source))
        p.write_request(wf.name, {"preset_source": source})
    _rows_of(wf.name)
    return jsonify(_workflow_summary(p.read_workflow(wf.name))), 201


@bp.put("/workflows/<name>")
def put_workflow(name):
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
    return jsonify(_workflow_summary(_project().write_request(name, _body())))


@bp.put("/workflows/<name>/overrides")
def put_workflow_overrides(name):
    p = _project()
    overrides = _checked_overrides(_body().get("resource_overrides")) or {}
    wf = p.write_overrides(name, overrides)
    return jsonify({"name": name, "overrides": wf.overrides})


@bp.get("/workflows/<name>/preset")
def get_workflow_preset(name):
    p = _project()
    wf = p.read_workflow(name)
    return jsonify({
        "name": name,
        "content": p.read_preset(name) or "",
        "preset_source": wf.request.get("preset_source"),
    })


@bp.put("/workflows/<name>/preset")
def put_workflow_preset(name):
    p = _project()
    b = _body()
    p.write_preset(name, b.get("content") or "")
    return jsonify({"name": name, "content": p.read_preset(name) or ""})


@bp.post("/workflows/<name>/preset/adopt")
def adopt_workflow_preset(name):
    p = _project()
    source = _checked_preset(_body().get("source"))
    assert source, "a preset name is required"
    p.write_preset(name, op_agent.preset_content(source))
    wf = p.write_request(name, {"preset_source": source})
    return jsonify({"name": name, "content": p.read_preset(name) or "", "preset_source": source})


@bp.get("/presets")
def list_all_presets():
    return jsonify(op_agent.config_presets())


@bp.post("/workflows/<name>/rename")
def rename_workflow(name):
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
    src_record = op_samples.record_path(p.input_library_path(name))
    if src_record.is_file():
        shutil.copy2(src_record, op_samples.record_path(p.input_library_path(forked.name)))
    src_preset = p.preset_path(name)
    if src_preset.is_file():
        p.write_preset(forked.name, src_preset.read_text())
    return jsonify(_workflow_summary(p.read_workflow(forked.name))), 201


@bp.post("/workflows/<name>/save_as_template")
def save_as_template(name):
    p = _project()
    wf = p.read_workflow(name)
    b = _body()
    tmpl_name = slugify(b.get("name") or "")
    assert tmpl_name, "a template name is required"
    if tmpl_name in _all_templates(p):
        raise ProjectError(f"a template named [{tmpl_name}] already exists")
    assert wf.ok, (
        f"workflow [{name}] has no successful plan: a template is a recipe "
        f"known to solve, so generate one first"
    )

    derived = op_data.derive_template_library(
        str(p.input_library_path(name)),
        type_library_paths=stdlib.discover(p.root)["data_types"],
    )
    spec = Spec(
        input_library=derived,
        target_types=wf.request.get("target_types") or [],
        transform_libraries=list(wf.request.get("transform_libraries") or []),
        resource_libraries=list(wf.request.get("resource_libraries") or []),
        sample_type=wf.request.get("sample_type"),
        shared_input_paths=list(wf.request.get("shared_input_paths") or []),
    )
    tmpl = Template(name=tmpl_name, spec=spec, description=b.get("description") or "")
    tmpl.Save(p.root, dirname=USER_TEMPLATES_DIRNAME)
    return jsonify(_template_summary(p, tmpl_name, tmpl, "user", _theme_arg())), 201


def _given_summary(lib_path: str) -> list[dict]:
    try:
        lib = op_data.load_data_lib(lib_path)
        info = op_data.inspect_library(lib_path)
        items = [
            op_data.show_item_lineage(lib_path, item["path"], render=False, lib=lib)
            for item in info.get("items", [])
        ]
    except Exception:
        return []
    return [
        {
            "path": item.get("path"),
            "type": item.get("type_name"),
            "parents": [p.get("path") for p in (item.get("parents") or [])],
        }
        for item in items
    ]


@bp.post("/workflows/<name>/generate")
def generate_workflow(name):
    p = _project()
    wf = p.read_workflow(name)
    b = _body()
    request_body = wf.request | {
        k: v for k, v in b.items() if k in set(Spec.FIELDS)
    }
    wf = p.write_request(name, request_body)

    recipe_fingerprint = b.get("recipe_fingerprint")

    sample_type = wf.request.get("sample_type")
    shared_refs = list(wf.request.get("shared_input_paths") or [])
    targets = wf.request.get("target_types") or []
    assert targets, "at least one target type is required"

    found = stdlib.discover(p.root)
    transforms, resources = _libraries_or_all(
        found,
        wf.request.get("transform_libraries"), wf.request.get("resource_libraries"),
    )
    lib_path = str(p.input_library_path(name))
    commit = found["commit"]
    table = op_samples.read_attached_table(wf.path)
    rows = _rows_of(name)

    def _work(job):
        with LogCapture(job):
            job.emit("PHASE:syncing")

            synced = op_inputs.sync(lib_path, rows, table)

            registered = synced["rows"]
            generated = synced["generated"]
            shared: list[str] = []
            for s in shared_refs:
                if not s.startswith("#"):
                    shared.append(s)
                elif s[1:] in registered:
                    shared.append(registered[s[1:]])
                else:
                    shared += list(dict.fromkeys(generated.get(s[1:], [])))
            shared = shared or None

            stale_names = ("task.yml", "data", "transforms")
            stale_names += _dag_cache_names()
            for stale in stale_names:
                target = wf.path / stale
                if target.is_dir():
                    shutil.rmtree(target)
                elif target.exists():
                    target.unlink()

            staging = wf.path / ".staging"
            if staging.exists():
                shutil.rmtree(staging)
            spec = Spec.Unpack(
                wf.request | {
                    "transform_libraries": list(transforms),
                    "resource_libraries": list(resources),
                    "shared_input_paths": shared or [],
                },
                input_library=lib_path,
            )
            job.emit("PHASE:solving")
            with _plan_lock:
                result, task = op_workflow.plan_spec(spec, workspace=str(staging), return_task=True)
                if result.get("success"):
                    result["step_display"], result["plan_graph"] = _step_display_from_task(task, p.root)
            job.emit("PHASE:finishing")
            if result.get("success"):
                staged = staging / result["task_key"]
                assert staged.is_dir(), f"planner wrote no bundle at [{staged}]"
                for item in staged.iterdir():
                    shutil.move(str(item), str(wf.path / item.name))
            if staging.exists():
                shutil.rmtree(staging)
            result["stdlib_commit"] = commit
            result["recipe_fingerprint"] = recipe_fingerprint
            result["transform_libraries"] = list(transforms)
            result["resource_libraries"] = list(resources)
            result["given"] = _given_summary(lib_path)
            result["targets"] = [
                {"type": t, "parents": []} if isinstance(t, str)
                else {"type": t.get("type", ""), "parents": list(t.get("parents") or [])}
                for t in targets
            ]
            result["sample_type"] = sample_type
            result["recipe_problems"] = op_inputs.problems(rows, table)
            p.write_result(name, result)
            return result

    job = _jobs().submit("generate", f"generate {name}", _work, subject={"workflow": name})
    return jsonify(job.summary()), 202


def _dag_cache_name(theme: str, background: bool = False) -> str:
    parts = ["plan", "dag"]
    if theme != "light": parts.append(theme)
    if background: parts.append("filled")
    return ".".join(parts) + ".svg"


def _dag_cache_names() -> tuple[str, ...]:
    return tuple(
        _dag_cache_name(theme, bg) for theme in THEMES for bg in (False, True)
    )


def _load_task(bundle: Path):
    from ..ops import workspace as op_workspace

    with _plan_lock:
        return op_workspace.load_task(None, str(bundle))


def _stdlib_transforms(root: Path) -> tuple[list[dict], dict[tuple[str, str], int]]:
    try:
        transforms = stdlib.type_index(root).get("transforms") or []
    except Exception:
        return [], {}
    seen: dict[tuple[str, str], int | None] = {}
    for i, tr in enumerate(transforms):
        key = (Path(str(tr.get("path") or "")).name, str(tr.get("name") or ""))
        seen[key] = None if key in seen else i
    return transforms, {k: v for k, v in seen.items() if v is not None}


def _step_display(bundle: Path, root: Path) -> tuple[list[dict], dict | None]:
    try:
        task = _load_task(bundle)
    except Exception:
        return [], None
    return _step_display_from_task(task, root, bundle=bundle)


def _step_display_from_task(task, root: Path, bundle: Path | None = None) -> tuple[list[dict], dict | None]:
    catalogue, by_index = _stdlib_transforms(root)
    node_extra: dict[str, dict] = {}
    out = []
    for step in task.plan.steps:
        res = step.transform.resources
        declared = {}
        if res is not None:
            if res.cpus is not None: declared["cpus"] = res.cpus
            if res.memory is not None: declared["memory_gb"] = round(res.memory.value_gb, 3)
            if res.duration is not None:
                declared["duration_h"] = round(res.duration._delta.total_seconds() / 3600, 3)
        file_name = Path(str(step.transform._path)).name
        out.append({
            "order": step.order,
            "transform": file_name,
            "declared_resources": declared,
            "process": NextflowProcessName(step.order, step.transform.name),
            "library": step.transform_library.GetKey(),
            "uses": sorted({inst.dtype_name for inst in step.uses}),
            "produces": sorted({
                inst.dtype_name for group in step.produces for inst in group
            }),
        })
        node_extra[f"{step.order} {step.transform.name}"] = {
            "step": step.order,
            "transform_index": by_index.get((file_name, step.transform.name)),
        }
    out.sort(key=lambda s: s["order"])

    plan_graph = None
    try:
        plan_graph = op_workflow.serialize_geometry(task.plan.BuildDAG())
        for n in plan_graph["nodes"]:
            x = node_extra.get(n["id"])
            if x is None:
                if n["kind"] != "transform":
                    n["type"] = n["id"]
                continue
            n.update(x)
            i = x["transform_index"]
            tr = catalogue[i] if i is not None and i < len(catalogue) else None
            if tr and tr.get("library_name"):
                n["namespace"] = tr["library_name"]
    except Exception:
        _LOG.warning(
            "no dag geometry for [%s]; the diagram will not draw",
            bundle if bundle is not None else "in-memory task", exc_info=True,
        )

    return out, plan_graph


@bp.get("/workflows/<name>/dag")
def workflow_dag(name):
    p = _project()
    wf = p.read_workflow(name)
    if not wf.ok:
        raise ProjectError(f"workflow [{name}] has no successful plan to draw")
    theme, background = _theme_arg(), _background_arg()
    svg = wf.path / _dag_cache_name(theme, background)
    if not svg.is_file():
        task = _load_task(wf.path)
        task.plan.RenderDAG(str(svg), theme=theme, background=background)
    return Response(svg.read_text(), mimetype="image/svg+xml")


@bp.post("/dag/layout")
def dag_layout():
    b = _body()
    nodes = b.get("nodes") or []
    edges = b.get("edges") or []
    assert isinstance(nodes, list) and isinstance(edges, list), "nodes and edges must be lists"
    order = b.get("order")
    row_y = b.get("row_y")
    assert order is None or isinstance(order, list), "order must be a list of node ids"
    assert row_y is None or isinstance(row_y, dict), "row_y must be a node id -> y map"
    return jsonify(op_workflow.dag_geometry(
        nodes, edges,
        label_mode=b.get("label_mode", "column"),
        font_size=float(b.get("font_size", 13.0)),
        max_label_chars=int(b.get("max_label_chars", 22)),
        order=[str(x) for x in order] if order else None,
        row_y={str(k): float(v) for k, v in row_y.items()} if row_y else None,
        min_lanes=int(b.get("min_lanes", 0)),
    ))


@bp.get("/dag/theme")
def dag_theme():
    return jsonify({
        name: {
            "plate": {
                "background": theme.plate.background,
                "edge": theme.plate.edge,
            },
            "styles": {
                kind.name.lower(): {
                    "fill": st.fill, "stroke": st.stroke,
                    "text": st.text, "muted": st.muted,
                    "shape": st.svg_shape, "marker_scale": st.marker_scale,
                    "stroke_width": st.stroke_width, "rx": st.rx,
                    "solid": st.solid,
                }
                for kind, st in theme.styles.items()
            },
        }
        for name, theme in THEMES.items()
    })


def _inputs_payload(lib_path: Path, *, lib=None, record=None) -> dict:
    if lib is None:
        lib = op_data.load_data_lib(str(lib_path))
    info = op_data.inspect_library(str(lib_path))
    info["items"] = [
        op_data.show_item_lineage(str(lib_path), item["path"], render=False, lib=lib)
        for item in info["items"]
    ]
    if record is None:
        record = op_samples.read_record(str(lib_path))
    from_array = {
        path: tid for tid, paths in (record.get("generated") or {}).items() for path in paths
    }
    from_row = {str(v): str(k) for k, v in (record.get("rows") or {}).items()}
    for item in info["items"]:
        item["array_id"] = from_array.get(item["path"])
        item["row_id"] = from_row.get(item["path"])
        item["deferred"] = is_deferred(item["path"])
    info["expansion"] = {
        "counts": {k: len(v) for k, v in (record.get("generated") or {}).items()},
        "row_count": record.get("row_count", 0),
    }
    return info


@bp.get("/workflows/<name>/inputs")
def get_inputs(name):
    p = _project()
    lib_path = p.input_library_path(name)
    if not lib_path.is_dir():
        raise ProjectError(f"workflow [{name}] has no input library")
    return jsonify(_inputs_payload(lib_path))


def _table_dir(name: str) -> Path:
    p = _project()
    p.read_workflow(name)
    return p.workflow_path(name)


def _rows_of(name: str, wf=None) -> list[dict]:
    return op_recipe.rows_of(_project(), name, wf=wf)


def _table_payload(name: str, lib_path: Path, table_dir: Path, *, rows=None, record=None) -> dict:
    table = op_samples.read_attached_table(table_dir)
    if table is None:
        return {"attached": False}
    if rows is None:
        rows = _rows_of(name)
    checked = op_samples.validate(str(lib_path), table, rows)
    if record is None:
        record = op_samples.read_record(str(lib_path))
    return {
        "attached": True,
        "filename": table["filename"],
        "format": table["format"],
        "columns": table["columns"],
        "row_count": table["row_count"],
        "preview": table["rows"][:5],
        "problems": checked["problems"],
        "row_uniques": op_samples.row_uniques(table, rows),
        "expansion": {
            "row_count": record.get("row_count", 0),
            "counts": {k: len(v) for k, v in (record.get("generated") or {}).items()},
            "stale": record.get("row_count", 0) != table["row_count"],
        },
    }


@bp.get("/workflows/<name>/table")
def get_table(name):
    p = _project()
    return jsonify(_table_payload(name, p.input_library_path(name), _table_dir(name)))


@bp.get("/workflows/<name>/table/raw")
def get_table_raw(name):
    table = op_samples.read_attached_table(_table_dir(name))
    assert table is not None, "no table attached"
    return jsonify({"text": op_samples.table_to_text(table)})


@bp.post("/workflows/<name>/table")
def attach_table(name):
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
    return jsonify(op_samples.detach_table(_table_dir(name)))


@bp.post("/workflows/<name>/inputs/types")
def attach_input_types(name):
    b = _body()
    return jsonify(op_data.attach_type_library(
        str(_project().input_library_path(name)),
        b["path"], b.get("namespace"), b.get("on_exist", "skip"),
    ))


def _run_summary(r) -> dict:
    return {
        "name": r.name,
        "workflow": r.workflow,
        "path": str(r.path),
        "state": r.state,
        "live": r.live,
        "archived_at": r.archived_at,
        **{k: r.record.get(k) for k in (
            "agent", "task_key", "staged_path", "created_at", "launched_at", "finished_at",
            "collected_at", "run_number", "preset_source", "error",
            "probe_error", "probe_error_at",
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


def _runnable(p, workflow: str, agent_name: str):
    # The pair of checks anything that reaches an agent with a workflow makes:
    # a plan worth sending, and an agent able to receive it.
    assert workflow, "workflow is required"
    assert agent_name, "agent is required"
    wf = p.read_workflow(workflow)
    if not wf.ok:
        raise ProjectError(f"workflow [{workflow}] has no successful plan to run")
    recipe_problems = list(wf.result.get("recipe_problems") or [])
    if recipe_problems:
        raise ProjectError(
            f"workflow [{workflow}] was planned from an unfinished recipe: "
            f"{'; '.join(recipe_problems)} -- fill them in and solve again"
        )
    if not p.agent_exists(agent_name):
        raise ProjectError(f"no agent named [{agent_name}]")
    payload = _agent_payload(p, agent_name)
    problems = list(payload["problems"])
    if not payload["deployed"]:
        problems.append("has not been deployed yet")
    if problems:
        raise ProjectError(
            f"agent [{agent_name}] is not ready to run on: {'; '.join(problems)}"
        )
    return wf, str(p.agent_path(agent_name))


@bp.post("/workflows/<name>/environment")
def setup_environment(name):
    # Prepare the chosen agent to run this workflow, without running it.
    #
    # Staging first is not a side effect to hide: the manifest that says which
    # images and envs the workflow needs is written by staging, so there is
    # nothing to read before it. `update` re-stages in place, which is what the
    # run path does too, so pressing this and then run does not stage twice.
    b = _body()
    p = _project()
    agent_name = b.get("agent")
    force = bool(b.get("force", False))
    wf, agent_path = _runnable(p, name, agent_name)
    found = stdlib.discover(p.root)
    library = found["path"] if found["present"] else None

    def _work(job):
        with LogCapture(job):
            staged = op_runtime.stage(agent_path, str(wf.path), "update", None)
            return op_runtime.setup_environment(
                agent_path, staged["task_key"], force=force, library=library,
            )

    job = _jobs().submit(
        "environment", f"setup environment for {name}", _work,
        subject={"workflow": name, "agent": agent_name},
    )
    return jsonify(job.summary()), 202


def _ensure_preset(p, workflow: str) -> str | None:
    # A workflow created before per-workflow presets existed has no
    # `preset.nf` of its own -- adopt one now, the same content eager
    # adoption would have written at creation time, rather than launching
    # against a config file that was never written.
    wf = p.read_workflow(workflow)
    source = wf.request.get("preset_source")
    if p.preset_path(workflow).is_file():
        return source
    available = op_agent.config_presets()
    if not available:
        return source
    source = "local" if "local" in available else available[0]
    p.write_preset(workflow, op_agent.preset_content(source))
    p.write_request(workflow, {"preset_source": source})
    return source


@bp.post("/runs")
def create_run():
    b = _body()
    p = _project()
    workflow = b.get("workflow")
    agent_name = b.get("agent")
    wf, agent_path = _runnable(p, workflow, agent_name)
    preset_source = _ensure_preset(p, workflow)
    rec = p.create_run(workflow, {
        "agent": agent_name,
        "preset_source": preset_source,
        "params": _checked_params(b.get("params")) or None,
        "resource_overrides": _checked_overrides(b.get("resource_overrides")),
        "on_exist": b.get("on_exist", "update"),
        "launched_by": current_app.config.get("MSM_INSTANCE"),
    })
    run_name = rec.name
    watcher = current_app.config["MSM_WATCHER"]

    def _work(job):
        with LogCapture(job):
            try:
                staged = op_runtime.stage(
                    agent_path, str(wf.path), rec.record.get("on_exist", "update"), None,
                )
                p.update_run(workflow, run_name, {
                    "state": "staged", "task_key": staged["task_key"],
                    "staged_path": op_runtime.staged_path(agent_path, staged["task_key"]),
                })
                p.update_run(workflow, run_name, {"state": "launching"})
                op_runtime.run(
                    agent_path,
                    staged["task_key"],
                    config_file=str(p.preset_path(workflow)),
                    is_local_preset=rec.record.get("preset_source") == "local",
                    params=rec.record.get("params"),
                    resource_overrides=rec.record.get("resource_overrides"),
                )
                runs = op_runtime.list_runs(agent_path, staged["task_key"])
                p.update_run(workflow, run_name, {
                    "state": "running",
                    "launched_at": utcnow(),
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
    p = _project()
    rec = p.read_run(workflow, run)
    agent_name = rec.record.get("agent")
    if not agent_name or not p.agent_exists(agent_name):
        return jsonify({"lines": [], "error": f"agent [{agent_name}] is gone"}), 200
    key = rec.record.get("task_key")
    if not key:
        return jsonify({"lines": []})
    run_number = rec.record.get("run_number")
    if run_number is None:
        # Pre-launch (staging/staged/launching) this task_key has no run-specific
        # log dir yet -- only `logs.latest`, a symlink shared across every run of
        # this task_key that still points at whichever run came before this one
        # until the launcher relinks it. Resolving through it here would hand
        # back the PREVIOUS run's log instead of "nothing yet".
        return jsonify({"lines": []})
    try:
        out = op_runtime.tail(
            str(p.agent_path(agent_name)),
            key,
            source=request.args.get("source", "agent"),
            lines=int(request.args.get("lines", 200)),
            run=run_number,
        )
    except Exception as exc:
        return jsonify({"lines": [], "error": str(exc)})
    return jsonify(out)


def _collected_log_dir(outputs: Path) -> Path | None:
    meta = outputs/"_metadata"
    if not meta.is_dir():
        return None
    dirs = sorted(
        p for p in meta.glob("logs.*") if p.is_dir() and p.name != "logs.latest"
    )
    return dirs[-1] if dirs else None


@bp.get("/runs/<workflow>/<run>/trace")
def run_trace(workflow, run):
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
    run_number = rec.record.get("run_number")
    if run_number is None:
        # Same hazard as run_log above: pre-launch there is no run-specific trace
        # file yet, only the task's shared `logs.latest`, which can still point
        # at whichever run came before this one -- every per-step chip in the
        # GUI is driven straight off this response, so leaking it here is what
        # paints steps as already done/failed the instant a new run is created.
        return jsonify(op_runtime.read_trace("/nonexistent"))
    try:
        return jsonify(op_runtime.trace(
            str(p.agent_path(agent_name)), key, run_number,
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
    # Only call it cancelled when nothing is left running. A run with survivors
    # stays `cancelling` and carries them, so the record cannot claim a stop it
    # did not achieve.
    survived = out.get("survived") or []
    if survived:
        p.update_run(workflow, run, {"state": "cancelling", "survivors": survived})
    else:
        p.update_run(workflow, run, {"state": "cancelled", "finished_at": utcnow(), "survivors": []})
    return jsonify(out)


@bp.post("/runs/<workflow>/<run>/collect")
def collect_run(workflow, run):
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
            job.emit(f"collecting results for [{key}] from agent [{agent_name}]")
            job.emit(f"into [{dest}]")
            out = op_runtime.collect(agent_path, key, str(dest), allow_globus=False)
            for src, dst in out.get("completed", []):
                job.emit(f"transferred [{src}] -> [{dst}]")
            for err in out.get("errors", []):
                job.emit(f"ERROR: {err}")
            job.emit(f"collected {len(out.get('completed', []))} item(s)")
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


PREVIEW_WINDOW = 256 * 1024
PREVIEW_MAX_TOTAL = 8 * 1024 * 1024
TREE_MAX_ENTRIES = 20000
TREE_MAX_DEPTH = 12
_INLINE_TYPES = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml",
}


def _resolve_output(p, workflow: str, run: str) -> Path:
    return p.outputs_path(workflow, run).resolve()


def _resolve_within(root: Path, rel: str) -> Path:
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
        st = entry_path.stat()
        size, mtime, dangling = st.st_size, st.st_mtime, False
    except OSError:
        size, mtime, dangling = None, None, link
    role = "output"
    if rel == "_metadata" or rel.startswith("_metadata/"):
        role = "log" if rel.startswith("_metadata/logs") else "metadata"
    return {
        "name": name, "path": rel,
        "type": "dir" if (entry_path.is_dir() and not dangling) else "file",
        "size": size, "mtime": mtime,
        "symlink": link, "dangling": dangling,
        "role": role, "type_name": None, "is_item": False, "parents": [],
        "children": [] if entry_path.is_dir() and not dangling else None,
    }


@bp.get("/runs/<workflow>/<run>/tree")
def run_tree(workflow, run):
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
            if node["type"] == "dir" and not node["symlink"]:
                node["children"] = walk(Path(e.path), depth + 1)
            out.append(node)
        return out

    tree = {
        "name": "", "path": "", "type": "dir", "role": "output",
        "size": None, "mtime": None, "symlink": False, "dangling": False,
        "type_name": None, "is_item": False, "parents": [],
        "children": walk(root, 0),
    }
    _tag_manifest_types(outputs, tree)
    tree["children"].sort(key=lambda n: (n["role"] != "output", n["name"]))
    return jsonify({
        "collected": True, "path": str(outputs),
        "truncated": budget["truncated"], "root": tree,
    })


def _tag_manifest_types(outputs: Path, tree: dict) -> None:
    try:
        lib = op_data.load_data_lib(str(outputs))
    except Exception:
        return
    named = {
        str(p): dtype_name for p, dtype_name, _ep in lib.Iterate()
        if not p.is_absolute()
    }
    if not named:
        return
    by_name = {}
    key_by_name = {}
    for k, v in named.items():
        by_name.setdefault(Path(k).name, v)
        key_by_name.setdefault(Path(k).name, k)
    ancestry = _ancestry_index(lib, outputs, named)

    def visit(node):
        key = node["path"] if node["path"] in named else None
        if key is None and node["type"] == "file":
            key = key_by_name.get(node["name"])
        t = named.get(key) if key else None
        if t is not None:
            node["type_name"] = t
            node["is_item"] = True
            node["parents"] = ancestry.get(key, [])
        for c in node.get("children") or []:
            visit(c)

    visit(tree)


# Every ancestor, not just the file's direct parents: `DataInstanceLibrary.Unpack`
# re-expands the transitively reduced graph on disk into the full closure, so the
# panel can answer "where did this come from" without walking anything.
def _ancestry_index(lib, outputs: Path, named: dict[str, str]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for path, parents in lib.parents.items():
        key = str(path)
        if key not in named:
            continue
        rows = []
        for pm in parents:
            p = Path(pm.path)
            node = None if p.is_absolute() else str(p)
            rows.append({
                "path": str(p),
                "type_name": pm.name,
                # An unpublished intermediate is in the manifest with no file
                # behind it; the panel says so rather than offering it as a link.
                "node": node if node and (outputs/p).exists() else None,
            })
        out[key] = rows
    return out


@bp.get("/runs/<workflow>/<run>/file")
def run_file(workflow, run):
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
    if b"\x00" in raw[:8192]:
        return jsonify(out | {"encoding": "binary", "text": None})
    text = raw.decode("utf-8", errors="replace")
    if text.count("�") > max(16, len(text) // 20):
        return jsonify(out | {"encoding": "binary", "text": None})
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


@bp.post("/share/export")
def share_export():
    b = _body()
    kind = b.get("kind")
    name = b.get("name")
    assert kind and name, "a kind and a name are required"
    return jsonify(op_share.export(
        _project(), _ssh(), kind, name, bound=bool(b.get("bound")),
    ))


@bp.post("/share/preview")
def share_preview():
    return jsonify(op_share.preview(_project(), _ssh(), _body().get("payload") or ""))


@bp.post("/share/import")
def share_import():
    return jsonify(op_share.commit(_project(), _ssh(), _body().get("payload") or "")), 201


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
