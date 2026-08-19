#!/usr/bin/env python3
"""Which requirements have more than one transform that could fill them.

This is the map that predicts a solve blowing its budget, and the compiled type
count is not. The solver branches at a requirement with several reachable
producers, and every independent target downstream of that fork multiplies the
branch -- so a library set with one such fork sitting above two thirds of a
template's targets is exponential in the number of targets, whatever the type
surface looks like.

`x.IsA(y)` is `y.properties <= x.properties`, so a product fills a requirement
when the product's property set is a superset of the requirement's. Names never
enter it: two differently-named types with identical properties are one node.

    python research/metasmith_libraries/probe_ambiguity.py [library ...]

Reachability is not modelled here -- a producer gated behind an input nobody has
(long reads, an NCBI accession) still counts. Read the output as the *upper
bound* on where a fork can appear, and confirm with `probe_solve_budget.py
--exclude` which of a pair is actually reachable for a given template.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))

from metasmith.python_api import TransformInstanceLibrary  # noqa: E402

DEFAULT = ["logistics", "assembly", "metagenomics", "functionalAnnotation"]


def main() -> int:
    names = sys.argv[1:] or DEFAULT
    paths = [Path(n) if "/" in n else MLIB / "transforms" / n for n in names]

    products: list[tuple[str, frozenset]] = []
    requirements: list[tuple[str, frozenset]] = []
    type_index: dict[frozenset, str] = {}
    for p in paths:
        lib = TransformInstanceLibrary.Load(p.resolve())
        for ns, tlib in lib.types.items():
            for tname, t in tlib.types.items():
                type_index.setdefault(frozenset(t.properties), f"{ns}::{tname}")
        for key, inst in lib.IterateTransforms():
            label = f"{p.name}/{key}"
            for dep in inst.model.requires:
                requirements.append((label, frozenset(dep.properties)))
            for group in inst.model.produces:
                for dep in group:
                    products.append((label, frozenset(dep.properties)))

    fills: dict[frozenset, set[str]] = defaultdict(set)
    askers: dict[frozenset, set[str]] = defaultdict(set)
    for label, req in requirements:
        askers[req].add(label)
        for plabel, prod in products:
            if req <= prod:
                fills[req].add(plabel)

    rows = sorted(((len(fills[r]), r) for r in askers if len(fills[r]) > 1),
                  key=lambda x: -x[0])
    print(f"# libraries: {', '.join(names)}")
    print(f"# {len(set(l for l, _ in products))} transforms,"
          f" {len(askers)} distinct requirements, {len(rows)} ambiguous\n")
    for n, req in rows:
        name = type_index.get(req) or "{" + ", ".join(sorted(str(p) for p in req)[:2]) + "...}"
        print(f"{n} producers  <-  {name}   ({len(askers[req])} demanders)")
        for f in sorted(fills[req]):
            print(f"      produced by  {f}")
        for a in sorted(askers[req]):
            print(f"      demanded by  {a}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
