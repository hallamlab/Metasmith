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
    return _read_env_manifest_doc(shell, workspace).get("steps", {})

def _read_env_manifest_doc(shell: LiveShell, workspace: Path) -> dict:
    # The whole manifest, not just the steps: the workspace's staged rootfs override
    # rides at the top level and decides which artifact a pre-fetch must produce.
    # An absent manifest means a workspace staged by an older metasmith, which is
    # indistinguishable from "nothing to check" and treated as such -- otherwise
    # every already-staged workspace starts failing at run.
    path = workspace/AgentPaths.ENV_MANIFEST
    res = shell.Exec(f'[ -e "{path}" ] && cat "{path}"', history=True, quiet=True)
    text = "\n".join(res.out)
    if "{" not in text: return {}
    try:
        text = text[text.index("{"):text.rindex("}")+1]
        return json.loads(text)
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
            if fields is None: continue  # resource unreadable at stage time
            # A list of keys (schema 1) or a mapping of key to what it resolves to
            # (schema 2). Membership is all this asks, and reads the same on both.
            if FIELD not in fields:
                offenders.append(f"{who}: env resource [{name}] has no '{FIELD}:' entry (has {sorted(fields) or ['nothing']})")
    if offenders:
        raise EnvPortabilityError(
            f"agent runtime [{env.runtime.name}] runs tools without a container, but"
            f" [{len(offenders)}] step(s) have no form it can execute:\n  "
            + "\n  ".join(offenders)
        )
