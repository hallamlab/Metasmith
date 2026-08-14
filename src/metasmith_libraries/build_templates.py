#!/usr/bin/env python3
"""Author every template in this directory, and fail naming the ones that broke.

`./dev.sh -b` runs this after rebuilding the `_metadata/`, which is what makes a
template a tested artifact rather than a stale example: the solve is the
assertion, and a transform whose products changed shape takes the templates that
depend on it down with it, by name, at build time.

A template author is a module here defining `NAME`, `DESCRIPTION` and
`build_spec`; they are listed below rather than discovered, because the rest of
this directory is run drivers that talk to real clusters on import and must not
be swept up by a build.

    python main/build_templates.py [--rebuild] [--dag] [name ...]
"""

from __future__ import annotations

import argparse
import importlib
import sys
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import _authoring as A

AUTHORS = (
    "pangenome_heatmap_from_assembly",
    "metagenomics_from_paired_reads",
    "isolate_assembly_from_long_reads",
    "annotation_palette_from_assembly",
)

# Authors that exist but cannot ship, and why. Kept visible rather than deleted:
# each is one line away from building again, and a build that silently omitted
# them would read as "these are all the templates there are".
BLOCKED = {
    "dl_embeddings_from_orfs":
        "every embedding transform consumes sequences::orfs_shard, whose only "
        "producer (transforms/logistics/shardFasta.py) is disabled -- re-enable "
        "it and move this name into AUTHORS",
}


def authors() -> list:
    found = []
    for name in AUTHORS:
        module = importlib.import_module(name)
        missing = [a for a in ("NAME", "DESCRIPTION", "build_spec") if not hasattr(module, a)]
        assert not missing, f"[{name}] is listed as a template author but defines no {missing}"
        found.append(module)
    return found


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("names", nargs="*", help="only these templates (default: all)")
    p.add_argument("--rebuild", action="store_true",
                   help="discard and re-mint each deferred input library")
    p.add_argument("--dag", action="store_true",
                   help="also render each solved DAG under results/ (authoring aid)")
    args = p.parse_args()

    modules = [m for m in authors() if not args.names or m.NAME in args.names]
    unknown = set(args.names) - {m.NAME for m in modules}
    if unknown:
        print(f"no such template(s): {', '.join(sorted(unknown))}", file=sys.stderr)
        return 2

    failed = []
    for module in modules:
        print(f"[{module.NAME}]")
        try:
            A.author(module, rebuild=args.rebuild, dag=args.dag)
        except Exception:
            traceback.print_exc()
            failed.append(module.NAME)

    for name, why in sorted(BLOCKED.items()):
        print(f"[{name}] SKIPPED: {why}")

    print(f"\n{len(modules) - len(failed)}/{len(modules)} templates built"
          f"{f', {len(BLOCKED)} blocked' if BLOCKED else ''}")
    if failed:
        print(f"FAILED: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
