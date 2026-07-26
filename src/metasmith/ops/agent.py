"""Agent loading, saving, info, ping, deploy."""
from __future__ import annotations

from pathlib import Path

from ..agents import Agent
from ..env import Runtime
from ..models.remote import Source


def _agent_info(name: str, agent: Agent) -> dict:
    presets = {}
    try:
        presets = {k: str(v) for k, v in agent.GetNxfConfigPresets().items()}
    except Exception:
        pass
    return {
        "name": name,
        "home": agent.home.address,
        "home_type": agent.home.type.name,
        "container": agent.container,
        "runtime": agent.runtime.name,
        "native": agent.native,
        "globus_uuid": agent.globus_uuid,
        "real_path": str(agent.real_path) if agent.real_path else None,
        "setup_commands": list(agent.setup_commands),
        "config_presets": presets,
    }


def runtimes() -> list[str]:
    """The runtimes an agent may be given, as the wire names `save_agent` takes.

    Read off the enum rather than written out again, so a runtime added to
    `env.Runtime` reaches every caller -- the CLI, the GUI's dropdown -- with
    nothing else to update.
    """
    return [r.name for r in Runtime]


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
) -> dict:
    """Write an agent YAML to disk.

    An agent that is already there is *edited*, not rebuilt: the fields below
    are set and everything else the file carries is kept. That matters for the
    two an editor never shows -- `real_path`, resolved at deploy time, and
    `gpu_args`/`native`, which are host facts someone set deliberately. Building
    a fresh Agent here silently reverted all three on the next save.

    `renaming_host` says the home changed only in how its host is *spelled* --
    the caller renamed an ssh alias and is bringing the agents on it along. The
    machine and the directory are the same, so the resolution the agent already
    has still holds; clearing it would make a cosmetic rename cost a redeploy.
    """
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    assert home_uri and home_uri.strip(), "a home directory is required"
    assert runtime in {r.name for r in Runtime}, (
        f"unknown runtime [{runtime}]; expected one of {', '.join(r.name for r in Runtime)}"
    )
    home = Source.Parse(home_uri)
    agent = Agent.Load(p) if p.is_file() else Agent(home=home)
    # `real_path` is what the *old* home resolved to on the host; carrying it
    # across a re-point would have the agent claim a directory it no longer
    # names. The next deploy resolves it again.
    if agent.home.address != home.address and not renaming_host:
        agent.real_path = None
    agent.home = home
    agent.setup_commands = list(setup_commands or [])
    agent.runtime = Runtime[runtime]
    agent.globus_uuid = globus_uuid
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
    return {"status": "deployed", "agent": Path(agent_path).stem, "home": agent.home.address}
