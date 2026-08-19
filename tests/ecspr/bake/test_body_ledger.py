from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit")

from ecspr.bake.aam import curation as C                              # noqa: E402


def test_a_body_is_counted_from_whichever_source_drew_it():
    resolved = {"CUR": "CC*"}
    counts_of = {"MNX": (6, 0, 0, 0), "UNK": (6, 0, 0, 0)}
    residue_of = {"MNX": 1, "UNK": None}
    slots = lambda m: C.residue_slots(m, resolved, {"PH"}, counts_of, residue_of)

    assert slots("CUR") == 1
    assert slots("MNX") == 1
    assert slots("PH") == 0
    assert slots("PLAIN") == 0
    assert slots("UNK") is None


def test_a_curated_body_pairs_with_a_metanetx_one():
    subs, prods = ["A"], ["B"]
    resolved = {"A": "C*"}
    counts_of = {"B": (1, 0, 0, 0)}
    residue_of = {"B": 1}
    kw = dict(counts_of=counts_of, residue_of=residue_of)

    assert C.gate_bodies_cancel(subs, prods, resolved, **kw) is True
    assert C.concrete_balance(subs, prods, {}, set(), "C", resolved, **kw) is True


def test_an_unpaired_curated_body_is_still_refused():
    resolved = {"A": "C*"}
    counts_of = {"B": (1, 0, 0, 0)}
    residue_of = {"B": 0}
    kw = dict(counts_of=counts_of, residue_of=residue_of)

    assert C.gate_bodies_cancel(["A"], ["B"], resolved, **kw) is False


def test_an_unknown_slot_count_is_refused_by_the_balance_and_not_by_the_gate():
    resolved = {"A": "C*"}
    counts_of = {"B": (1, 0, 0, 0)}
    residue_of = {"B": None}
    kw = dict(counts_of=counts_of, residue_of=residue_of)

    assert C.gate_bodies_cancel(["A"], ["B"], resolved, **kw) is True
    assert C.concrete_balance(["A"], ["B"], {}, set(), "C", resolved, **kw) is None


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
    assert caps_of.get("MNXMCARGO") == 1


def test_a_state_prefix_is_not_a_second_body(caps_of):
    assert caps_of.get("MNXMAPO") == 1


def test_a_name_that_declares_no_body_still_gets_none(caps_of):
    assert caps_of.get("MNXMPLAIN") == 0
