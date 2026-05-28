"""Agent loading, saving, info, ping, deploy."""
from __future__ import annotations

from pathlib import Path

from ..agents import Agent
from ..coms.containers import ContainerRuntime
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
        "globus_uuid": agent.globus_uuid,
        "real_path": str(agent.real_path) if agent.real_path else None,
        "setup_commands": list(agent.setup_commands),
        "config_presets": presets,
    }


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
) -> dict:
    """Write an agent YAML to disk."""
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    home = Source.Parse(home_uri)
    agent = Agent(
        home=home,
        setup_commands=setup_commands or [],
        runtime=ContainerRuntime[runtime],
        globus_uuid=globus_uuid,
    )
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
