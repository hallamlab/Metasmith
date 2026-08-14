"""``fabfos.pipelines.DOMAINS`` must be the union of what the drivers load.

That list is a component of the method id, and it is a literal rather than a
derived value so that `--describe-method` never has to import metasmith (see
``fabfos/pipelines/__init__.py``). A literal that nothing checks is a literal
that drifts, and the drift is invisible: adding a domain to a driver would widen
the planner's candidate space -- a different method -- without moving the id.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/test_domains.py -v
"""
from __future__ import annotations

from fabfos.pipelines import DOMAINS
from fabfos.pipelines import annotation, assembly, ecspr


def test_domains_is_the_union_the_drivers_load():
    union = set(assembly.DOMAINS) | set(annotation.DOMAINS) | set(ecspr.DOMAINS)
    assert set(DOMAINS) == union, (
        f"fabfos.pipelines.DOMAINS is {sorted(DOMAINS)} but the drivers load "
        f"{sorted(union)}; update the literal (it is hashed into the method id)"
    )


def test_domains_is_sorted_and_unique():
    # The method id hashes `sorted(DOMAINS)`, so order here is cosmetic -- but a
    # duplicate is not: it would read as two domains in the document.
    assert DOMAINS == sorted(set(DOMAINS))


def test_every_domain_exists_in_the_library():
    from fabfos.pipelines.common import resolve_library_root

    transforms = resolve_library_root() / "transforms"
    missing = [d for d in DOMAINS if not (transforms / d).is_dir()]
    assert not missing, (
        f"no transforms/ directory for: {missing}. A name with nothing behind it "
        f"does not fail here, it fails inside TransformInstanceLibrary.Load "
        f"with a path instead of a reason"
    )
