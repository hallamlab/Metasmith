#!/usr/bin/env python3
import sys

import _authoring as A
import engcyc_metagenomics as M

NAME = "engcyc_prep_references"
DESCRIPTION = """
Fetches and builds every reference database and tool image that
engcyc_metagenomics needs, and takes no samples. Run it once on an agent before
the real run; engcyc_metagenomics then finds these products in the cache.

Its targets are the reference products of engcyc_metagenomics' plan, so the two
templates name the same steps. Licensed packages are left out, as in
engcyc_metagenomics. The downloads total ~680 GB, ~350 GB of it iPHoP's; see
engcyc_metagenomics' description for each lane's size.
"""


def reference_targets(plan) -> list[str]:
    # A step is reference-side when nothing it consumes descends from a sample.
    # Its products that a sample-side step consumes are the targets.
    sample_made = {str(i.path) for i in plan.given if i.dtype_name.startswith("sequences::")}
    ref_made: dict[str, str] = {}
    consumed_by_samples: set[str] = set()
    for step in sorted(plan.steps, key=lambda s: s.order):
        ins = [i for d in step.transform.model.requires for i in step.dependency_map.get(d, [])]
        outs = [i for g in step.transform.model.produces for d in g
                for i in step.dependency_map.get(d, [])]
        if any(str(i.path) in sample_made for i in ins):
            sample_made |= {str(i.path) for i in outs}
            consumed_by_samples |= {str(i.path) for i in ins}
        else:
            ref_made |= {str(i.path): i.dtype_name for i in outs}
    return sorted({t for i, t in ref_made.items() if i in consumed_by_samples})


def build_spec(rebuild: bool = False):
    main = M.build_spec(rebuild=rebuild).Solve()
    assert main.ok, f"engcyc_metagenomics does not solve: {sorted(main.plan.dropped_targets)}"
    return M.spec_for(NAME, rebuild=rebuild, targets=reference_targets(main.plan))


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
