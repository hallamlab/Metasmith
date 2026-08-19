from __future__ import annotations

import collections

import pytest

from ecspr.bake.direction import forecast as F

pytest.importorskip("rdkit", reason="the forecast profiles compounds with RDKit")


def _profiles(**kw):
    props = {m: ({"smiles": s, "inchikey": f"KEY-{m}"} if s else {})
             for m, s in kw.items()}
    return F.profile_compounds(list(kw), props)


def test_the_first_failing_participant_is_the_one_named():
    profiles = _profiles(A=None, B="*C(=O)O", C="O")
    stoich = collections.OrderedDict([("A", -1.0), ("B", -1.0), ("C", 1.0)])
    assert F.predict_dgbyg(stoich, profiles) == ("no_smiles", "A")

    reversed_order = collections.OrderedDict([("B", -1.0), ("A", -1.0), ("C", 1.0)])
    assert F.predict_dgbyg(reversed_order, profiles) == ("wildcard", "B")


def test_a_participant_absent_from_the_props_table_blocks_both_members():
    profiles = _profiles(A="O")
    stoich = collections.OrderedDict([("A", -1.0), ("WATER", 1.0)])
    assert F.predict_dgbyg(stoich, profiles) == ("no_smiles", "WATER")
    assert F.predict_eq(stoich, profiles, None) == ("no_props", "WATER")


def test_balance_is_tested_only_after_every_participant_was_readable():
    profiles = _profiles(A="O", B=None)
    stoich = collections.OrderedDict([("A", -1.0), ("B", 1.0)])
    assert F.predict_dgbyg(stoich, profiles)[0] == "no_smiles"


def test_hydrogen_is_left_out_of_the_balance_ledger():
    # dGbyG transforms protonation at pH 7, so its H count is not the SMILES'.
    #
    # Measured against the r7 member table, heavy-atom balance has no false
    # positives in 27,534 reactions that reached the check, while counting H gives
    # 1,280. The predicate is deliberately the conservative one.
    profiles = _profiles(A="[OH-]", B="O")
    stoich = collections.OrderedDict([("A", -1.0), ("B", 1.0)])   # O vs H2O
    assert F.predict_dgbyg(stoich, profiles) == ("expected_ok", "")

    heavy = _profiles(A="O", B="CO")
    st2 = collections.OrderedDict([("A", -1.0), ("B", 1.0)])      # a carbon appears
    assert F.predict_dgbyg(st2, heavy) == ("unbalanced", "C")


def test_unresolved_is_not_predicted_at_all_when_no_resolution_is_supplied():
    # Silence about a mechanism, rather than a guess at it.
    #
    # eQuilibrator's cache is frozen at an older MetaNetX, so whether it holds a
    # compound is not a function of chem_prop. With no `resolve` table the forecast
    # says `expected_ok`, which the summary records as an UPPER BOUND -- and the
    # r7 backtest shows exactly why: 19,120 reactions the member called
    # `unresolved` split 11,183 / 7,937 across `expected_ok` and `no_props` on that
    # basis alone.
    profiles = _profiles(A="O")
    stoich = collections.OrderedDict([("A", -1.0)])
    assert F.predict_eq(stoich, profiles, None) == ("expected_ok", "")
    assert F.predict_eq(stoich, profiles, {"A": False}) == ("unresolved", "A")
    assert F.predict_eq(stoich, profiles, {"A": True}) == ("expected_ok", "")


def test_every_emitted_mechanism_belongs_to_the_member_that_emitted_it():
    assert set(F.MEMBER_MECHANISMS) == {"eq", "dgbyg"}
    for member, mechs in F.MEMBER_MECHANISMS.items():
        assert set(mechs) <= set(F.MECHANISMS), member
        assert "expected_ok" in mechs and "no_stoich" in mechs, member
    assert set().union(*F.MEMBER_MECHANISMS.values()) == set(F.MECHANISMS)

    profiles = _profiles(A="O")
    stoich = {"MNXR_ABSENT": ({}, True, False)}
    rows, tally = F.build(["MNXR_ABSENT", "MNXR_MISSING"], stoich, profiles, None)
    assert {r[2] for r in rows if r[0] == "MNXR_MISSING"} == {"no_stoich"}
