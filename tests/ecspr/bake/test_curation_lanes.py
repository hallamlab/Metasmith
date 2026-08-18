"""`lane_fragment`, and the one name it must not read as chemistry.

The lane infers an atom budget by resolving a name's tokens to metabolites. That reading
is defeated by a redox carrier, because MNXref carries a ModelSEED fragment stub named
`Oxidized-` -- a trailing hyphen `norm` erases, so the bare adjective `oxidized` resolves
to a real C12N4 body. `oxidized [NADPH--hemoprotein reductase]` then sums that stub with
real NADPH and hands an ENZYME C33/N11/P3, while its reduced twin, having no stub to
collide with, falls through to the placeholder library's `[Fe+2]`. The couple stops
balancing and `concrete_balance` refuses every reaction that uses it -- 1,122 of them in
the r6 universe, all completed, none mapped.

The fixture reproduces the collision rather than asserting the outcome downstream of it,
so the test fails if the guard is removed OR if the stub stops colliding.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit")

from ecspr.bake.aam import curation as C                              # noqa: E402

# (mnxm, name, formula, smiles, n_C, n_N, n_S, n_P) -- verbatim from MNXref 4.5 where
# the row is real; the two carrier ids are structureless there and are structureless here.
MET_ROWS = [
    ("MNXM588580", "Oxidized-", "C12H9N4O2*",
     "[*]N1C2=CC(C)=C(C)C=C2N=C2C(=O)NC(=O)N=C21", 12, 4, 0, 0),
    ("MNXM738702", "NADPH", "C21H26N7O17P3", "CC(=O)NCCO", 21, 7, 0, 3),
    ("MNXM1090405", "reduced [NADPH--hemoprotein reductase]", None, None,
     None, None, None, None),
    ("MNXM1090406", "oxidized [NADPH--hemoprotein reductase]", None, None,
     None, None, None, None),
    # the control: an ordinary dipeptide, the case the lane exists for
    ("MNXMGLY", "glycine", "C2H5NO2", "NCC(=O)O", 2, 1, 0, 0),
    ("MNXMGG", "glycyl-glycine", None, None, None, None, None, None),
]

RXN_ROWS = [
    ("MNXRHEMO", ["MNXM738702", "MNXM1090406"], ["MNXM1090405"],
     ["MNXM1090405", "MNXM1090406"]),
    ("MNXRGG", ["MNXMGLY", "MNXMGLY"], ["MNXMGG"], ["MNXMGG"]),
]


@pytest.fixture(scope="module")
def refs(tmp_path_factory):
    d = tmp_path_factory.mktemp("lookups")
    pd.DataFrame(
        [dict(mnxm=m, name=n, formula=f, smiles=s, has_smiles=bool(s),
              n_C=c, n_N=nn, n_S=ss, n_P=p)
         for m, n, f, s, c, nn, ss, p in MET_ROWS]).to_parquet(d / "metabolites.parquet")
    pd.DataFrame([dict(mnxr=r, substrates=s, products=p, blockers=b, n_blockers=len(b))
                  for r, s, p, b in RXN_ROWS]).to_parquet(d / "reactions.parquet")
    return C.Refs(d)


@pytest.fixture(scope="module")
def targets(refs):
    return refs.reactions


def test_the_stub_still_collides_with_the_bare_adjective(refs):
    """The premise. `Oxidized-` is indexed under `oxidized`, carrying real atoms."""
    assert refs.name2id.get("oxidized") == "MNXM588580"
    assert refs.counts_of["MNXM588580"] == (12, 4, 0, 0)


@pytest.mark.parametrize("mnxm,smiles", [
    ("MNXM1090405", "[Fe+2]"),
    ("MNXM1090406", "[Fe+3]"),
])
def test_the_placeholder_library_covers_both_states(refs, mnxm, smiles):
    got = C.placeholder_for(refs.name_of[mnxm])
    assert got is not None and got[0] == smiles


def test_lane_fragment_defers_where_the_library_already_has_the_pair(refs, targets):
    """Neither member gets an inferred budget, so neither can outweigh its twin."""
    claimed = {r["mnxm"] for r in C.lane_fragment(refs, targets)}
    assert "MNXM1090406" not in claimed
    assert "MNXM1090405" not in claimed


def test_lane_fragment_still_sums_an_ordinary_dipeptide(refs, targets):
    """The guard is scoped to the library, not to two-residue names in general."""
    rows = [r for r in C.lane_fragment(refs, targets) if r["mnxm"] == "MNXMGG"]
    assert rows, "the dipeptide lost its fragment sum -- the guard is over-blocking"
    assert {r["element"]: r["n_atoms"] for r in rows}["C"] == 4
