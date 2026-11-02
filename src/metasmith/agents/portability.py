from __future__ import annotations

import json
from pathlib import Path

from ..coms.terminals import LiveShell
from ..constants import AgentPaths
from ..env import Environment, Runtime
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
    # The question is about the environment resource, not about the transform body:
    # a body says what to run and in what environment, and the runtime decides how.
    # So the only way a step cannot run here is that its environment declares
    # nothing for this runtime -- no `conda:` where there is no container, no
    # `container:` where the tool is launched into one.
    if not manifest: return
    FIELD = "conda" if env.runtime == Runtime.MAMBA else "container"
    offenders: list[str] = []
    for _, v in sorted(manifest.items()):
        who = f"{v.get('transform')} (step {v.get('step')})"
        # `runs` absent means a workspace staged by an older metasmith, which is
        # indistinguishable from "nothing to check"; zero runs means a body that
        # launches no tool at all.
        runs = v.get("runs")
        if not runs:
            continue
        for name, fields in sorted((v.get("envs") or {}).items()):
            # An unreadable resource is UNKNOWN, not absent.
            if fields is None: continue
            if FIELD not in fields:
                offenders.append(
                    f"{who}: env resource [{name}] has no '{FIELD}:' entry"
                    f" (has {sorted(fields) or ['nothing']})"
                )
    if offenders:
        where = (
            "runs tools without a container" if env.runtime == Runtime.MAMBA
            else "launches tools into a container"
        )
        raise EnvPortabilityError(
            f"agent runtime [{env.runtime.name}] {where}, but"
            f" [{len(offenders)}] step(s) have no form it can execute:\n  "
            + "\n  ".join(offenders)
        )
