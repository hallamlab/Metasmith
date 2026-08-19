#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Callable

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
    spec_path = Template.PathIn(MLIB, name)
    if spec_path.exists() and not rebuild:
        return Template.Load(spec_path, root=MLIB).spec.input_library
    library = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix=f"msm-template-{name}-")))
    build(library)
    return library


def author(module, *, rebuild: bool = False, dag: bool = False) -> Spec:
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
