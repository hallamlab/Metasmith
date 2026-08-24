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
        "default_preset": agent.default_preset,
        "default_params": dict(agent.default_params),
    }


def runtimes() -> list[str]:
    return [r.name for r in Runtime]


def default_container() -> str:
    return Agent.__dataclass_fields__["container"].default


def config_presets() -> list[str]:
    try:
        return sorted(GetNxfConfigPresets())
    except Exception:
        return []


def preset_content(name: str) -> str:
    presets = GetNxfConfigPresets()
    assert name in presets, f"unknown nextflow preset [{name}]; expected one of {sorted(presets)}"
    return Path(presets[name]).read_text()


def load_agent(agent_path: str) -> Agent:
    return Agent.Load(Path(agent_path))


def list_agents(agent_paths: list[str]) -> list[dict]:
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


def deploy(agent_path: str, assertive: bool = False, on_phase=None) -> dict:
    agent = load_agent(agent_path)
    agent.Deploy(assertive, on_phase=on_phase)
    agent.Save(Path(agent_path))
    return {
        "status": "deployed",
        "agent": Path(agent_path).stem,
        "home": agent.home.address,
        "real_path": str(agent.real_path) if agent.real_path else None,
    }
