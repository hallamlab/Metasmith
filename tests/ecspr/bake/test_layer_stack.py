from __future__ import annotations

import pandas as pd
import pytest

from ecspr.bake.aam import layers as L


def _atoms(rows: list[tuple]) -> pd.DataFrame:
    out = [dict(zip(("mnxr", "element", "substrate", "product", "sub_idx", "prod_idx"), r),
                pair_w=1.0, method="test", source="test", confidence=1.0) for r in rows]
    return pd.DataFrame(out, columns=list(L.ATOM_COLS))


ONE = ("R1", "S", "M1", "M2", 0, 0)


def test_gate_refuses_a_layer_that_reclaims_a_lower_layer_s_reaction_element():
    below = _atoms([ONE])
    added = _atoms([("R1", "S", "M1", "M3", 0, 1)])
    with pytest.raises(SystemExit, match="not additive"):
        L.additive_gates(below, added, "partial")


def test_gate_refuses_the_same_atom_correspondence_from_two_layers():
    below = _atoms([ONE])
    with pytest.raises(SystemExit):
        L.additive_gates(below, _atoms([ONE]), "dup")


def test_gate_3_cannot_fire_and_is_pinned_as_inert():
    below = _atoms([ONE, ("R2", "S", "M1", "M2", 0, 0)])
    assert L.additive_gates(below, _atoms([]), "empty") is True
    assert L.additive_gates(below, _atoms([("R3", "N", "M1", "M2", 0, 0)]), "add") is True


def test_gate_refuses_negative_canonical_ranks():
    with pytest.raises(SystemExit):
        L.additive_gates(_atoms([]), _atoms([("R1", "S", "M1", "M2", -1, 0)]), "neg")


def test_gate_passes_a_genuinely_additive_layer():
    below = _atoms([ONE])
    added = _atoms([("R2", "N", "M1", "M2", 0, 0)])
    assert L.additive_gates(below, added, "next") is True


def test_consensus_full_weight_single_member_half_disagreement_diluted():
    agree = {"a": _atoms([ONE]), "b": _atoms([ONE])}
    df, tally = L.fuse_members(agree)
    assert tally == {"consensus": 1}
    assert list(df["pair_w"]) == [1.0]

    df, tally = L.fuse_members({"a": _atoms([ONE])})
    assert tally == {"a_only": 1}
    assert list(df["pair_w"]) == [L.SINGLE_MEMBER_CREDIT]
    assert list(df["method"]) == ["a_only"]

    disagree = {"a": _atoms([ONE]), "b": _atoms([("R1", "S", "M1", "M3", 0, 0)])}
    df, tally = L.fuse_members(disagree)
    assert tally == {"disagree_diluted": 1}
    assert len(df) == 2, "a disputed atom must spread, not resolve to one row"
    assert sum(df["pair_w"]) == pytest.approx(1.0), (
        "a disputed atom's total transfer must equal a confident one's -- the "
        "dilution redistributes weight, it does not create or destroy it")


def test_a_nan_confidence_cannot_poison_every_weight_in_the_reaction():
    a = _atoms([ONE])
    b = _atoms([("R1", "S", "M1", "M3", 0, 0)])
    a["confidence"] = float("nan")
    b["confidence"] = float("nan")
    df, tally = L.fuse_members({"a": a, "b": b})
    assert tally == {"disagree_diluted": 1}
    assert df["pair_w"].notna().all(), "a NaN confidence poisoned the pair weights"
    assert sum(df["pair_w"]) == pytest.approx(1.0)


def test_a_substrate_atom_reaching_two_molecules_is_a_violation():
    df = _atoms([("R1", "C", "M1", "M2", 5, 0), ("R1", "C", "M1", "M3", 5, 7)])
    _mA, _mB, bad = L.injectivity_violations(df)
    assert bad.all()


def test_a_product_atom_claimed_by_two_molecules_is_a_violation():
    df = _atoms([("R1", "C", "M1", "M3", 5, 7), ("R1", "C", "M2", "M3", 2, 7)])
    _mA, _mB, bad = L.injectivity_violations(df)
    assert bad.all()


def test_two_atoms_of_one_molecule_reaching_two_molecules_is_not():
    df = _atoms([("R1", "C", "M1", "M2", 5, 0), ("R1", "C", "M1", "M3", 6, 7)])
    _mA, _mB, bad = L.injectivity_violations(df)
    assert not bad.any(), "a molecule may split its atoms between two products"


def test_a_duplicated_participant_explains_the_violation():
    df = _atoms([("R1", "C", "M1", "M3", 5, 7), ("R1", "C", "M2", "M3", 2, 7)])
    # `1 M1 + 1 M2 = 2 M3` -- the identity space cannot tell M3's two copies apart, so
    # both substrates correctly reach one node.
    coef = {"R1": ({"M1": 1, "M2": 1}, {"M3": 2})}
    _mA, _mB, bad = L.injectivity_violations(df, coef)
    assert not bad.any()
    _mA, _mB, bad = L.injectivity_violations(df, {"R1": ({"M1": 1, "M2": 1}, {"M3": 1})})
    assert bad.all(), "one copy of M3 cannot receive two atoms"


def test_audit_refuses_above_the_ceiling(tmp_path):
    import argparse
    p = tmp_path / "pairs.parquet"
    _atoms([("R1", "C", "M1", "M2", 5, 0), ("R1", "C", "M1", "M3", 5, 7)]).to_parquet(p)
    args = argparse.Namespace(pairs=str(p), reac_prop=None, worklist=None, out=None,
                              max_reactions=0)
    with pytest.raises(SystemExit, match="injectivity"):
        L.cmd_audit(args)
    args.max_reactions = 1
    assert L.cmd_audit(args) == 0


def test_a_member_offering_two_destinations_breaks_its_tie_on_the_destination():
    a = _atoms([("R1", "C", "M1", "M9", 5, 0), ("R1", "C", "M1", "M2", 5, 3)])
    b = a.iloc[::-1].reset_index(drop=True)
    ra, _ = L.fuse_members({"m": a})
    rb, _ = L.fuse_members({"m": b})
    assert ra["product"].tolist() == rb["product"].tolist() == ["M9"]


def test_a_doubled_substrate_keeps_both_of_its_destinations():
    # `2 M1 = M2 + M3`: the two copies share one atom identity, so one node legitimately
    # sends an atom to each product. Keeping one destination is a lost edge, not a
    # resolved ambiguity.
    both = _atoms([("R1", "C", "M1", "M2", 5, 0), ("R1", "C", "M1", "M3", 5, 1)])
    coef = {"R1": ({"M1": 2}, {"M2": 1, "M3": 1})}
    df, _tally = L.fuse_members({"a": both, "b": both.copy()}, coefficients=coef)
    assert sorted(df["product"]) == ["M2", "M3"], (
        "a participant written twice may reach both of its products")
    assert sum(df["pair_w"]) == pytest.approx(1.0), (
        "the atom's margin is still one atom's worth -- two copies share one node, so "
        "the weight divides across the destinations rather than doubling")
    assert all(w == pytest.approx(0.5) for w in df["pair_w"])


def test_a_singly_written_substrate_may_not_fan_out_past_its_coefficient():
    df = _atoms([("R1", "C", "M1", "M2", 5, 0), ("R1", "C", "M1", "M3", 5, 1),
                 ("R1", "C", "M1", "M4", 5, 2)])
    _mA, _mB, bad = L.injectivity_violations(df, {"R1": ({"M1": 1}, {})})
    assert bad.all(), "one copy of M1 cannot reach three molecules"
    _mA, _mB, bad = L.injectivity_violations(df, {"R1": ({"M1": 2}, {})})
    assert bad.all(), "two copies cannot reach three molecules either"
    two = _atoms([("R1", "C", "M1", "M2", 5, 0), ("R1", "C", "M1", "M3", 5, 1)])
    _mA, _mB, bad = L.injectivity_violations(two, {"R1": ({"M1": 2}, {})})
    assert not bad.any(), "two copies reaching two molecules is the coefficient's own rule"


def test_a_fractional_coefficient_is_not_a_coefficient_of_zero(tmp_path):
    # `reac_prop` carries 2,729 sub-1 coefficient terms. Rounding one to zero would make
    # every row of that participant a violation, and would let a fanout cap delete it.
    p = tmp_path / "reac_prop.tsv"
    p.write_text("#comment\n"
                 "MNXR1\t1 MNXM1@MNXD1 + 0.000503 MNXM2@MNXD1 = 1 MNXM3@MNXD1\tx\n")
    sub, prod = L.load_coefficients(p)["MNXR1"]
    assert sub["MNXM2"] >= 1, "a trace coefficient still names one participant, not none"
    assert sub["MNXM1"] == 1 and prod["MNXM3"] == 1


def test_a_packed_layer_is_exploded_whatever_backs_its_strings(tmp_path):
    # pandas stopped backing string columns with `object`, and the old dtype test read
    # that as "already exploded" -- so a packed layer loaded with its indices still
    # strings and failed a numeric gate several layers later.
    import pandas as pd
    from ecspr.bake.aam import layers as L

    p = tmp_path / "packed.parquet"
    pd.DataFrame({
        "mnxr": ["MNXR1"], "element": ["C"], "substrate": ["MNXM1"],
        "product": ["MNXM2"], "n_atoms": [3],
        "sub_idx": ["0,1,2"], "prod_idx": ["2,1,0"], "pair_w": ["1.0,1.0,1.0"],
    }).to_parquet(p, index=False)

    for dtype in ("object", "string"):
        d = pd.read_parquet(p)
        d["sub_idx"] = d["sub_idx"].astype(dtype)
        assert not pd.api.types.is_integer_dtype(d["sub_idx"])

    got = L.explode(p, method="forced", source="partial", confidence=1.0)
    assert len(got) == 3
    assert pd.api.types.is_integer_dtype(got["sub_idx"])
    assert sorted(got["sub_idx"]) == [0, 1, 2]
