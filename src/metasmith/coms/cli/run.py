from __future__ import annotations

import sys
from pathlib import Path

from ...models.direct_run import RunTransform


def register(subs):
    p = subs.add_parser(
        "run",
        help="run a single transform against concrete inputs (no Nextflow)",
        description="Run a transform's protocol directly against user-supplied "
                    "input files, bypassing the workflow solver and Nextflow.",
    )
    p.add_argument("transform", type=Path,
                   help="path to the transform .py; its library is the nearest "
                        "parent holding compiled _metadata/")
    p.add_argument("-i", "--input", required=True, action="append", default=[],
                   metavar="NAME=PATH",
                   help="input binding, where NAME is the variable the "
                        "requirement was assigned to in the transform (e.g. "
                        "reads=./sample.fq); repeat for multiple inputs, or "
                        "repeat one NAME to give that input several files")
    p.add_argument("-w", "--work-dir", type=Path, default=None,
                   help="output directory (defaults to cwd)")
    p.add_argument("--agent-home", type=Path, default=None,
                   help="deployed agent home (containing lib/agent.yml); "
                        "defaults to $AGENT_HOME")
    p.set_defaults(func=_cmd_run)


def _cmd_run(args) -> None:
    inputs: list[tuple[str, Path]] = []
    for item in args.input:
        if "=" not in item:
            print(f"invalid --input [{item}], expected NAME=PATH", file=sys.stderr)
            sys.exit(2)
        k, v = item.split("=", 1)
        inputs.append((k, Path(v)))

    try:
        result = RunTransform(
            transform=args.transform,
            inputs=inputs,
            work_dir=args.work_dir,
            agent_home=args.agent_home,
        )
    except ValueError as e:
        print(e, file=sys.stderr)
        sys.exit(2)
    sys.exit(0 if result.success else 1)
