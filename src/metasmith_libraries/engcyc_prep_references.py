#!/usr/bin/env python3
import sys

import _authoring as A
import engcyc_metagenomics as M

NAME = "engcyc_prep_references"
DESCRIPTION = """
Fetches and builds every reference database and tool image that
engcyc_metagenomics needs, and takes no samples. Run it once on an agent before
the real run; engcyc_metagenomics then finds these products in the cache.

Its targets are engcyc_metagenomics' references case, so the two templates name
the same reference steps: the long-read lanes' references and the Metabuli database
build included. Licensed packages are left out, as in engcyc_metagenomics. The plan
summary shows the download total; the largest are iPHoP's 278 GB and the Metabuli
build's 180 GB of sources. Each archive is deleted once its database is built. See
engcyc_metagenomics' description for each lane's size.
"""


def reference_side(plan) -> tuple[set, list[str]]:
    # A step is reference-side when nothing it consumes descends from a sample. Returns
    # those steps, as (transform, product ids), and the products a sample-side step
    # consumes, which are the targets.
    sample_made = {str(i.path) for i in plan.given if i.dtype_name.startswith("sequences::")}
    steps, ref_made, consumed_by_samples = set(), {}, set()
    for step in sorted(plan.steps, key=lambda s: s.order):
        ins = [str(i.path) for i in step.uses]
        outs = [i for g in step.produces for i in g]
        if any(i in sample_made for i in ins):
            sample_made |= {str(i.path) for i in outs}
            consumed_by_samples |= set(ins)
        else:
            ref_made |= {str(i.path): i.dtype_name for i in outs}
            steps.add((step.transform.name, frozenset(i.instance_id for i in outs)))
    return steps, sorted({t for i, t in ref_made.items() if i in consumed_by_samples})


def build_template(rebuild: bool = False):
    main = M.build_template(rebuild=rebuild).Plan()
    assert main.ok, f"engcyc_metagenomics does not plan: {sorted(main.plan.dropped_targets)}"
    steps, targets = reference_side(main.plan)
    assert set(targets) == set(M.REFERENCES), (
        f"engcyc_metagenomics' samples read references its references case does not name: "
        f"missing {sorted(set(targets) - set(M.REFERENCES))}, unread {sorted(set(M.REFERENCES) - set(targets))}"
    )
    template = M.template_for(NAME, rebuild=rebuild, targets=M.REFERENCES, description=DESCRIPTION)
    prep = template.Plan()
    assert prep.ok, f"{NAME} does not plan: {sorted(prep.plan.dropped_targets)}"
    mine, _ = reference_side(prep.plan)
    assert mine == steps, (
        f"{NAME} and engcyc_metagenomics plan different reference steps: "
        f"only here {sorted(t for t, _ in mine - steps)}, only there {sorted(t for t, _ in steps - mine)}"
    )
    return template


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
