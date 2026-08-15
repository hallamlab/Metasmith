#!/usr/bin/env python3
"""Shared machinery for the template authors in this directory.

A template is a `metasmith.Spec` whose input paths are `DEFERRED`, saved under
`templates/<name>/` next to the deferred input library it points at. Authoring
one is three steps, and this module owns all three so a driver is only the part
that differs: what the inputs are, and what to build from them.

    build the deferred input library  ->  build the spec  ->  solve it

The solve *is* the test. A template that no longer solves against the transforms
beside it is a broken template, and `dev/libraries.sh -b` fails naming it.

Two things a driver must not do. It must not name an agent -- a template says
what to build, never where; whoever loads it supplies the host. And it must not
ship a rendered DAG: `--dag` writes one under `results/` (git-ignored) because
seeing the graph is how you tell whether the spec you just wrote is the one you
meant, but the repository ships the spec and the build asserts the spec.

The input library is built **once** and then reused from the repository --
inline in the committed `spec.yml` (see `Template.Save`/`Spec.Pack`), not as a
sibling directory. Deferred paths are minted on `AddItem` and persisted, and
identity follows the path, so rebuilding on every run would give the template
a different task key every time it was authored -- and the deferred rows
would pile up, since each mint is a new manifest entry rather than a
replacement. `--rebuild` is the deliberate way to start over after changing
what the inputs *are*.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Callable

# metasmith must be importable; set MSM_SRC to a source checkout if not installed.
if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])

from metasmith.python_api import DataInstanceLibrary, Spec, Template

MLIB = Path(__file__).resolve().parent
TYPES = MLIB / "data_types"


def transforms(*names: str) -> list[Path]:
    return [MLIB / "transforms" / n for n in names]


def envs() -> Path:
    return MLIB / "resources" / "env"


def deferred_inputs(
    name: str,
    build: Callable[[DataInstanceLibrary], None],
    *,
    rebuild: bool = False,
) -> DataInstanceLibrary | dict:
    """The template's input library: typed rows with lineage and no paths yet.

    A previous build committed this inline in `templates/<name>/spec.yml` --
    load it back unchanged, so the deferred rows it minted keep their paths
    and the template keeps its task key. Only build fresh (into a throwaway
    directory; nothing here is meant to be committed as a directory) when
    there is nothing to load yet, or `--rebuild` asks to start over.
    """
    spec_path = Template.PathIn(MLIB, name)
    if spec_path.exists() and not rebuild:
        return Template.Load(spec_path, root=MLIB).spec.input_library
    library = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix=f"msm-template-{name}-")))
    build(library)
    return library


def author(module, *, rebuild: bool = False, dag: bool = False) -> Spec:
    """Build, solve and save one driver's template. Raises if it does not solve."""
    name, description = module.NAME, module.DESCRIPTION.strip()
    spec = module.build_spec(rebuild=rebuild)

    task = spec.Solve()
    if not task.ok:
        raise AssertionError(
            f"template [{name}] no longer solves: "
            f"{len(task.plan.steps)} steps, dropped {sorted(task.plan.dropped_targets)}"
        )

    Template(name=name, description=description, spec=spec).Save(MLIB)
    print(f"  {name}: {len(task.plan.steps)} steps")

    if dag:
        out = MLIB / "results" / "template_dags" / name
        out.parent.mkdir(parents=True, exist_ok=True)
        print(f"  dag: {task.plan.RenderDAG(str(out), format='svg')}")
    return spec


def cli(module) -> None:
    p = argparse.ArgumentParser(description=module.DESCRIPTION)
    p.add_argument("--rebuild", action="store_true",
                   help="discard and re-mint the deferred input library")
    p.add_argument("--dag", action="store_true",
                   help="also render the solved DAG under results/ (authoring aid)")
    args = p.parse_args()
    author(module, rebuild=args.rebuild, dag=args.dag)
