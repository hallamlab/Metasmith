"""Agent loading, saving, info, ping, deploy."""
from __future__ import annotations

from pathlib import Path

from ..agents import Agent, GetNxfConfigPresets
from ..env import Rootfs, Runtime
from ..models.remote import Source


def _agent_info(name: str, agent: Agent) -> dict:
    presets = {}
    try:
        presets = {k: str(v) for k, v in agent.GetNxfConfigPresets().items()}
    except Exception:
        pass
    return {
        "name": name,
        "id": agent.id,
        "home": agent.home.address,
        "home_type": agent.home.type.name,
        "container": agent.container,
        "runtime": agent.runtime.name,
        "native": agent.native,
        "gpu_args": list(agent.gpu_args),
        "globus_uuid": agent.globus_uuid,
        "real_path": str(agent.real_path) if agent.real_path else None,
        "setup_commands": list(agent.setup_commands),
        "config_presets": presets,
        # which of `config_presets` a run uses when the caller names none;
        # `None` is the built-in `local`
        "default_preset": agent.default_preset,
        # params every run on this agent starts from -- where a scheduler preset
        # gets the account it needs
        "default_params": dict(agent.default_params),
    }


def runtimes() -> list[str]:
    """The runtimes an agent may be given, as the wire names `save_agent` takes.

    Read off the enum rather than written out again, so a runtime added to
    `env.Runtime` reaches every caller -- the CLI, the GUI's dropdown -- with
    nothing else to update.
    """
    return [r.name for r in Runtime]


def default_container() -> str:
    """The image an agent runs metasmith from when it names none.

    Read off the dataclass rather than rebuilt from the version, so it cannot
    drift from what an agent created without one actually gets.
    """
    return Agent.__dataclass_fields__["container"].default


def config_presets() -> list[str]:
    """The nextflow config presets an agent may declare, by name.

    Same shape and same reason as `runtimes()`: a fixed list read off what
    metasmith ships, so a preset added to the package folder reaches the CLI's
    `--preset` and the page's dropdown with nothing else to update.
    """
    try:
        return sorted(GetNxfConfigPresets())
    except Exception:
        return []


def load_agent(agent_path: str) -> Agent:
    return Agent.Load(Path(agent_path))


def list_agents(agent_paths: list[str]) -> list[dict]:
    """List the given agent YAMLs with summary info."""
    results = []
    for p in agent_paths:
        try:
            a = load_agent(p)
            results.append({
                "name": Path(p).stem,
                "path": str(p),
                "home": a.home.address,
                "container": a.container,
                "runtime": a.runtime.name,
            })
        except Exception as exc:
            results.append({"name": Path(p).stem, "path": str(p), "error": str(exc)})
    return results


def info(agent_path: str) -> dict:
    return _agent_info(Path(agent_path).stem, load_agent(agent_path))


def save_agent(
    path: str,
    home_uri: str,
    container: str | None = None,
    runtime: str = "DOCKER",
    setup_commands: list[str] | None = None,
    globus_uuid: str | None = None,
    renaming_host: bool = False,
    default_preset: str | None = None,
    default_params: dict | None = None,
    native: bool | None = None,
    gpu_args: list[str] | None = None,
    id: str | None = None,
    rootfs: str | None = None,
) -> dict:
    """Write an agent YAML to disk.

    An agent that is already there is *edited*, not rebuilt: the fields below
    are set and everything else the file carries is kept. That matters for
    `real_path`, resolved at deploy time: building a fresh Agent here silently
    reverted it on the next save.

    `native`, `gpu_args` and `rootfs` are host facts no editor draws. They are kept when
    not given, which is what an editor saving a form wants, and set when they
    are, which is what an importer wants: an agent arriving from a colleague has
    no file on this side to preserve them from.

    `renaming_host` says the home changed only in how its host is *spelled* --
    the caller renamed an ssh alias and is bringing the agents on it along. The
    machine and the directory are the same, so the resolution the agent already
    has still holds; clearing it would make a cosmetic rename cost a redeploy.

    `id` is only ever honoured the one time it matters: when there is no file
    here yet. An agent that already exists keeps whatever id `Agent.Load` gave
    it -- the whole point of the id is that nothing after creation can move it.

    The named fields *are* set, including to nothing: omitting `globus_uuid` or
    `default_preset` clears it. That is the contract a save-the-whole-object
    caller wants, and it is why the two lists above are worth reading -- what is
    preserved is what is not named here.
    """
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    assert home_uri and home_uri.strip(), "a home directory is required"
    assert runtime in {r.name for r in Runtime}, (
        f"unknown runtime [{runtime}]; expected one of {', '.join(r.name for r in Runtime)}"
    )
    home = Source.Parse(home_uri)
    if p.is_file():
        agent = Agent.Load(p)
    else:
        agent = Agent(home=home, id=id) if id else Agent(home=home)
    # `real_path` is what the *old* home resolved to on the host; carrying it
    # across a re-point would have the agent claim a directory it no longer
    # names. The next deploy resolves it again.
    if agent.home.address != home.address and not renaming_host:
        agent.real_path = None
    agent.home = home
    agent.setup_commands = list(setup_commands or [])
    agent.runtime = Runtime[runtime]
    agent.globus_uuid = globus_uuid
    agent.default_preset = default_preset or None
    agent.default_params = dict(default_params or {})
    if native is not None:
        agent.native = bool(native)
    if gpu_args is not None:
        agent.gpu_args = list(gpu_args)
    if rootfs is not None:
        agent.rootfs = Rootfs.Parse(rootfs)
    if container:
        agent.container = container
    agent.Save(p)
    return {"name": p.stem, "path": str(p), "home": home.address, "runtime": agent.runtime.name}


def ping(agent_path: str, timeout_s: int = 15) -> dict:
    agent = load_agent(agent_path)
    res = agent._remote_oneshot("echo ok && hostname", timeout_s)
    return {
        "agent": Path(agent_path).stem,
        "out": res.out,
        "err": res.err,
        "ok": any("ok" in ln for ln in res.out),
    }


def deploy(agent_path: str, assertive: bool = False) -> dict:
    agent = load_agent(agent_path)
    agent.Deploy(assertive)
    # Saved back, because the deploy resolved something the file did not know:
    # `real_path`. Without this the record is indistinguishable from one that
    # was never deployed, and that distinction is what the launch route needs
    # in order to refuse a run rather than fail inside staging minutes later.
    agent.Save(Path(agent_path))
    return {
        "status": "deployed",
        "agent": Path(agent_path).stem,
        "home": agent.home.address,
        "real_path": str(agent.real_path) if agent.real_path else None,
    }
