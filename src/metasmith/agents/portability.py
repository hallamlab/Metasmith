"""Refusing a run the agent cannot execute, before it is launched.

Not every transform has a runnable form under every runtime -- roughly a third
of the standard library has no conda recipe -- so a mamba agent handed a
container-only workflow would fail per-task, on the far side, after staging. The
manifest the stage writes lets the client check first and name every step that
has no form the agent can run.
"""

from __future__ import annotations

import json
from pathlib import Path

from ..coms.terminals import LiveShell
from ..constants import AgentPaths
from ..env import Environment
from ..models.remote import Source

class EnvPortabilityError(Exception):
    """A staged workflow names a tool this agent's runtime cannot provide."""

def _read_env_manifest(shell: LiveShell, workspace: Path) -> dict[str, dict]:
    # Per-step tool-environment declarations recorded at stage time. Absent for
    # workspaces staged by an older metasmith, which is indistinguishable from
    # "nothing to check" and is treated as such -- otherwise every already-staged
    # workspace would start failing at run.
    path = workspace/AgentPaths.ENV_MANIFEST
    res = shell.Exec(f'[ -e "{path}" ] && cat "{path}"', history=True, quiet=True)
    text = "\n".join(res.out)
    if "{" not in text: return {}
    try:
        text = text[text.index("{"):text.rindex("}")+1]
        return json.loads(text).get("steps", {})
    except (json.JSONDecodeError, ValueError) as e:
        # Same reasoning as the GPU manifest: a file that exists but does not
        # parse means the check cannot be performed, and skipping it silently is
        # the failure mode this exists to prevent.
        raise EnvPortabilityError(
            f"env manifest at [{path}] exists but could not be parsed ({e});"
            f" cannot verify tool-environment portability. Re-stage the workflow."
        ) from e

def _check_env_portability(manifest: dict[str, dict], env: Environment) -> None:
    """Refuse a run whose steps have no tool form this agent can execute.

    Under a container runtime every declaration is satisfiable by construction
    (a `container:` entry is the legacy default and the arms only gate *which*
    command runs), so the check is about the relay-free runtimes, where a third
    of a typical tool library simply has no conda form.
    """
    if not manifest or env.needs_relay: return
    ARM, FIELD = "ifVirtualEnvDo", "conda"
    offenders: list[str] = []
    for _, v in sorted(manifest.items()):
        who = f"{v.get('transform')} (step {v.get('step')})"
        arms = v.get("arms")
        if arms is None:
            # Source could not be scanned at stage time -- unknown, not absent.
            continue
        if not arms:
            # Scanned fine and declares NO arms: the step invokes no external tool at
            # all (a pure-python declaration, a verify, a refusal). There is nothing
            # for a runtime to fail to provide, so it is not an offender -- the empty
            # list is a different fact from None above, which is "we could not look".
            continue
        if ARM not in arms:
            offenders.append(f"{who}: declares no {ARM} arm (has {arms or ['no arms']})")
            continue
        for name, fields in sorted((v.get("envs") or {}).items()):
            if fields is None: continue  # resource unreadable at stage time
            if FIELD not in fields:
                offenders.append(f"{who}: env resource [{name}] has no '{FIELD}:' entry (has {fields or ['nothing']})")
    if offenders:
        raise EnvPortabilityError(
            f"agent runtime [{env.runtime.name}] runs tools without a container, but"
            f" [{len(offenders)}] step(s) have no form it can execute:\n  "
            + "\n  ".join(offenders)
        )
