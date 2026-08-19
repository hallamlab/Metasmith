from __future__ import annotations

import json
from pathlib import Path

from ..coms.terminals import LiveShell
from ..constants import AgentPaths
from ..env import Environment
from ..models.remote import Source

class EnvPortabilityError(Exception):
    pass
def _read_env_manifest(shell: LiveShell, workspace: Path) -> dict[str, dict]:
    path = workspace/AgentPaths.ENV_MANIFEST
    res = shell.Exec(f'[ -e "{path}" ] && cat "{path}"', history=True, quiet=True)
    text = "\n".join(res.out)
    if "{" not in text: return {}
    try:
        text = text[text.index("{"):text.rindex("}")+1]
        return json.loads(text).get("steps", {})
    except (json.JSONDecodeError, ValueError) as e:
        raise EnvPortabilityError(
            f"env manifest at [{path}] exists but could not be parsed ({e});"
            f" cannot verify tool-environment portability. Re-stage the workflow."
        ) from e

def _check_env_portability(manifest: dict[str, dict], env: Environment) -> None:
    if not manifest or env.needs_relay: return
    ARM, FIELD = "ifVirtualEnvDo", "conda"
    offenders: list[str] = []
    for _, v in sorted(manifest.items()):
        who = f"{v.get('transform')} (step {v.get('step')})"
        arms = v.get("arms")
        if arms is None:
            continue
        if not arms:
            continue
        if ARM not in arms:
            offenders.append(f"{who}: declares no {ARM} arm (has {arms or ['no arms']})")
            continue
        for name, fields in sorted((v.get("envs") or {}).items()):
            if fields is None: continue
            if FIELD not in fields:
                offenders.append(f"{who}: env resource [{name}] has no '{FIELD}:' entry (has {fields or ['nothing']})")
    if offenders:
        raise EnvPortabilityError(
            f"agent runtime [{env.runtime.name}] runs tools without a container, but"
            f" [{len(offenders)}] step(s) have no form it can execute:\n  "
            + "\n  ".join(offenders)
        )
