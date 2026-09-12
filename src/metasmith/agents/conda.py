# Creating the tool environments a container-less agent needs, before a run.
#
# The sibling of `images.py`, for the other half of the same question: an agent
# whose runtime is mamba (or that is already native to its environment) runs each
# tool through `mamba run -n <env>`, and nothing in metasmith has ever created
# those envs -- the library ships one recipe per tool and a user was expected to
# build them by hand. The same verb that fills an image store for a container
# agent builds these for a mamba one.
#
# Two things the caller is told rather than shielded from:
#
#   * an env whose recipe the library does not ship is named, because inferring a
#     package spec from a container tag would produce a plausible env that is not
#     the one the transform was written against;
#   * a step whose env resource carries no `conda:` at all is named together with
#     the container it does have. That is the same fact `_check_env_portability`
#     raises on at launch, produced early and as a list -- the point is to see it
#     before staging a workflow that cannot run here.
#
# Free functions taking a shell, like `images.py`: the agent method is the only
# thing that opens a connection.

from __future__ import annotations

import base64
import importlib.util
from pathlib import Path

from ..constants import AgentPaths
from ..logging import Log


class CondaEnvError(Exception):
    pass


_READY = "env-ready"
# Why an env is not there, as a code rather than a sentence: the caller splits the
# two -- one is the library's gap and is reported, the other is a failure and raises.
NO_RECIPE = "no-recipe"
FAILED = "create-failed"
_FRONTEND = "MSM_FRONTEND"
RECIPES_IN_LIBRARY = Path("envs")/"tools"


def _manifest_envs(doc: dict) -> tuple[list[str], list[dict], list[str]]:
    # The distinct conda envs a staged workspace needs, what cannot be built, and
    # what could not be read.
    #
    # Distinct by env name: tools that share an env share the build.
    #
    # The second list is the report G4 exists for -- one entry per env resource
    # with no `conda:` entry, carrying the container it does have so the reader
    # can see the workflow is runnable, just not here. The third is the same
    # unreadable-requirement case `_manifest_images` reports, and means the same
    # thing: re-stage.
    envs: list[str] = []
    without: list[dict] = []
    unknown: set[str] = set()
    for _, step in sorted((doc.get("steps") or {}).items()):
        for name, fields in sorted((step.get("envs") or {}).items()):
            if not isinstance(fields, dict):
                unknown.add(str(step.get("transform")))
                continue
            env = fields.get("conda")
            if env:
                if env not in envs: envs.append(env)
                continue
            without.append({
                "transform": step.get("transform"),
                "step": step.get("step"),
                "resource": name,
                "container": fields.get("container"),
            })
    return sorted(envs), without, sorted(unknown)


def _installed_library_root() -> Path|None:
    from .templates import standard_library_root
    return standard_library_root()


def _recipe_roots(library: "Path|str|None" = None) -> list[Path]:
    # Where a tool env's recipe is looked for, nearest first: the library this
    # workflow was planned against, then the one shipped inside this package.
    # A source checkout's `envs/` lives outside the library root and so is not
    # reachable from here, which is why "no recipe" is a reportable answer
    # rather than an assertion; a vendored install carries `envs/` and resolves.
    roots = []
    if library: roots.append(Path(library)/RECIPES_IN_LIBRARY)
    installed = _installed_library_root()
    if installed: roots.append(installed/RECIPES_IN_LIBRARY)
    return [r for r in roots if r.is_dir()]


def _find_recipes(names: list[str], roots: list[Path]) -> dict[str, str|None]:
    found: dict[str, str|None] = {}
    for name in names:
        found[name] = None
        for root in roots:
            recipe = root/f"{name}.yml"
            if not recipe.is_file(): continue
            found[name] = recipe.read_text(encoding="utf-8")
            break
    return found


def _conda_frontend(shell) -> str|None:
    res = shell.Exec(
        'for c in mamba conda; do command -v $c >/dev/null 2>&1 '
        f'&& {{ echo "{_FRONTEND}=$c"; break; }}; done',
        history=True, quiet=True,
    )
    for line in res.out:
        if f"{_FRONTEND}=" in line:
            return line.split(f"{_FRONTEND}=", 1)[1].strip()
    return None


def _is_created(shell, frontend: str, name: str) -> bool:
    # The test the runtime itself applies: `mamba run -n <env>` is the wrapper
    # every task is launched under, so an env that answers it is an env that
    # works, and one that does not is worth rebuilding whatever `env list` says.
    res = shell.Exec(
        f'{frontend} run -n {name} true >/dev/null 2>&1 && echo "{_READY}"',
        history=True, quiet=True,
    )
    return _READY in res.out


def _push_recipe(shell, text: str, path: Path) -> None:
    blob = base64.b64encode(text.encode("utf-8")).decode("ascii")
    shell.Exec(f'mkdir -p "{path.parent}" && echo "{blob}" | base64 -d > "{path}"', history=True, quiet=True)


def _create_conda_envs(
    shell, recipes: dict[str, str|None], frontend: str, agent_home: Path,
    *, force: bool = False,
) -> list[dict]:
    # Build every missing env on the agent's own host.
    #
    # Idempotent the same way `_materialise_images` is: an env that already
    # answers the wrapper is skipped, so a second run does nothing. `force`
    # removes and rebuilds, which is what to reach for when an env is suspect
    # rather than absent -- `env create` refuses an existing name outright.
    #
    # One env failing does not stop the rest: solving a large recipe is minutes,
    # and a caller who has to re-run from the top for each failure will not.
    report: list[dict] = []
    for name, text in recipes.items():
        if not force and _is_created(shell, frontend, name):
            report.append({"env": name, "ok": True, "skipped": True, "reason": None})
            continue
        if text is None:
            report.append({"env": name, "ok": False, "skipped": False, "reason": NO_RECIPE})
            Log.Warn(f"no recipe for env [{name}]")
            continue
        recipe = agent_home/AgentPaths.CONDA_RECIPES/f"{name}.yml"
        _push_recipe(shell, text, recipe)
        if force:
            shell.Exec(f'{frontend} env remove -n {name} -y', history=True)
        shell.Exec(f'{frontend} env create -n {name} -f "{recipe}"', history=True)
        ok = _is_created(shell, frontend, name)
        report.append({"env": name, "ok": ok, "skipped": False, "reason": None if ok else FAILED})
        Log.Info(f"{'created' if ok else 'FAILED to create'} env [{name}]")
    failed = [r["env"] for r in report if r["reason"] == FAILED]
    if failed:
        raise CondaEnvError(
            f"[{len(failed)}] of [{len(report)}] environment(s) could not be created "
            f"on this agent:\n  " + "\n  ".join(failed)
        )
    return report
