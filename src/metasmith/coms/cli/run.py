"""`metasmith run ...` — direct-run a single transform without Nextflow."""
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
    p.add_argument("-l", "--transform-lib", required=True, type=Path,
                   help="path to the transform library (a directory)")
    p.add_argument("-t", "--transform", required=True,
                   help="relative path of the transform .py within the library")
    p.add_argument("-i", "--input", required=True, action="append", default=[],
                   metavar="TYPE=PATH",
                   help="input binding (e.g. ncbi::assembly_accession=./acc.txt); "
                        "repeat for multiple inputs")
    p.add_argument("-w", "--work-dir", type=Path, default=None,
                   help="output directory (defaults to cwd)")
    p.add_argument("--agent-home", type=Path, default=None,
                   help="deployed agent home (containing lib/agent.yml); "
                        "defaults to $AGENT_HOME or a host-local stub")
    p.add_argument("--host", default=None,
                   help="relay host name (reserved for relay-bounce wiring)")
    p.set_defaults(func=_cmd_run)


def _cmd_run(args) -> None:
    inputs: list[tuple[str, Path]] = []
    for item in args.input:
        if "=" not in item:
            print(f"invalid --input [{item}], expected TYPE=PATH", file=sys.stderr)
            sys.exit(2)
        k, v = item.split("=", 1)
        inputs.append((k, Path(v)))

    result = RunTransform(
        transform_lib=args.transform_lib,
        transform=args.transform,
        inputs=inputs,
        work_dir=args.work_dir,
        host=args.host,
        agent_home=args.agent_home,
    )
    sys.exit(0 if result.success else 1)
