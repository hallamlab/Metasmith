"""The unspecified `*` body: counted once, from whichever source drew it.

A body reaches a reaction two ways -- a curated row draws it as `*` in a SMILES this run
supplies, and MetaNetX draws it as an R-group in a structure it already had. Those used
to be tallied in two places that each saw half the participants: `gate_bodies_cancel`
counted only the curated half, `concrete_balance` only the recounted half. A curated body
on one side and a MetaNetX body on the other therefore failed BOTH checks, each for the
half it could not see, when between them the two cancel exactly. Measured over the 24,098
`blocked_no_structure` targets, closing the split recovers 1,683 reactions and 3,250
balanced `(mnxr, element)` keys, and costs none of either.

The second subject here is the other half of the same problem: a body the NAME declares
but a lane declines to draw. `lane_conserved` drew `4-methyl-trans-hex-2-enoyl-ACP` with
one `*` while a vehicle-building lane drew its substrate twin with none, so a dehydratase
step whose carrier cannot leave was refused for an imbalance neither lane's chemistry
claims.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit")

from ecspr.bake.aam import curation as C                              # noqa: E402


# =====================================================================
# the ledger
# =====================================================================

def test_a_body_is_counted_from_whichever_source_drew_it():
    """The four routes into `residue_slots`, and the one that must stay a refusal."""
    resolved = {"CUR": "CC*"}
    counts_of = {"MNX": (6, 0, 0, 0), "UNK": (6, 0, 0, 0)}
    residue_of = {"MNX": 1, "UNK": None}
    slots = lambda m: C.residue_slots(m, resolved, {"PH"}, counts_of, residue_of)

    assert slots("CUR") == 1        # curated: the `*` in the SMILES this run supplies
    assert slots("MNX") == 1        # MetaNetX: the R-group its own structure carries
    assert slots("PH") == 0         # a placeholder is scaffolding, not a body
    assert slots("PLAIN") == 0      # a countable formula has no remainder by construction
    assert slots("UNK") is None     # an unknown slot count is never a zero


def test_a_curated_body_pairs_with_a_metanetx_one():
    """The defect, reproduced. `A` is curated `C*`; `B` is MetaNetX with one R-group.

    Each check used to see one of them and refuse. Both now see both, and the reaction
    completes -- one carbon in, one carbon out, one unspecified body on each side.
    """
    subs, prods = ["A"], ["B"]
    resolved = {"A": "C*"}
    counts_of = {"B": (1, 0, 0, 0)}
    residue_of = {"B": 1}
    kw = dict(counts_of=counts_of, residue_of=residue_of)

    assert C.gate_bodies_cancel(subs, prods, resolved, **kw) is True
    assert C.concrete_balance(subs, prods, {}, set(), "C", resolved, **kw) is True


def test_an_unpaired_curated_body_is_still_refused():
    """Widening the ledger must not weaken it: one body in, none out, still a refusal."""
    resolved = {"A": "C*"}
    counts_of = {"B": (1, 0, 0, 0)}
    residue_of = {"B": 0}
    kw = dict(counts_of=counts_of, residue_of=residue_of)

    assert C.gate_bodies_cancel(["A"], ["B"], resolved, **kw) is False


def test_an_unknown_slot_count_is_refused_by_the_balance_and_not_by_the_gate():
    """Both are refusals and the reaction dies either way. Keeping them in separate
    functions is what keeps `bodies do not cancel` a diagnosable bucket."""
    resolved = {"A": "C*"}
    counts_of = {"B": (1, 0, 0, 0)}
    residue_of = {"B": None}
    kw = dict(counts_of=counts_of, residue_of=residue_of)

    assert C.gate_bodies_cancel(["A"], ["B"], resolved, **kw) is True
    assert C.concrete_balance(["A"], ["B"], {}, set(), "C", resolved, **kw) is None


# =====================================================================
# the body the name declares
# =====================================================================

MET_ROWS = [
    ("MNXMALA", "alanine", "C3H7NO2", "CC(N)C(=O)O", 3, 1, 0, 0),
    ("MNXMGLY", "glycine", "C2H5NO2", "NCC(=O)O", 2, 1, 0, 0),
    ("MNXMCARGO", "alanyl-glycyl-[acp]", None, None, None, None, None, None),
    ("MNXMAPO", "apo-alanyl-glycyl-[acp]", None, None, None, None, None, None),
    ("MNXMPLAIN", "alanyl-glycine", None, None, None, None, None, None),
]

RXN_ROWS = [("MNXRACP", ["MNXMALA"], ["MNXMCARGO", "MNXMAPO", "MNXMPLAIN"],
             ["MNXMCARGO", "MNXMAPO", "MNXMPLAIN"])]


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
def caps_of(refs):
    return {r["mnxm"]: r["smiles"].count("*")
            for r in C.lane_fragment(refs, refs.reactions)}


def test_a_fragment_sum_draws_the_body_its_name_declares(caps_of):
    """`alanyl-glycyl-[acp]` sums two residues AND carries an ACP. Drawing the budget
    without the body is what let two lanes disagree about the same carrier."""
    assert caps_of.get("MNXMCARGO") == 1


def test_a_state_prefix_is_not_a_second_body(caps_of):
    """`apo-` marks an oxidation-like state, not another cap. The bare form of the same
    protein carries no word at all, so counting the prefix would manufacture on one side
    the asymmetry this rule removes on the other."""
    assert caps_of.get("MNXMAPO") == 1


def test_a_name_that_declares_no_body_still_gets_none(caps_of):
    """The rule is scoped to names that name a carrier, not to fragment sums at large."""
    assert caps_of.get("MNXMPLAIN") == 0
