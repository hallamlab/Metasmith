"""The direction forecast: does it abstain where the member abstains, and for the same reason.

The forecast's whole value is that its mechanism names ARE the member's `reason`
strings, so a backtest is a string comparison rather than a calibration. What can
break that quietly is the propagation, not the per-compound tests: both members
walk a reaction's participants in equation order and return at the FIRST one they
cannot use, so a forecast that evaluated participants in any other order would
agree about the silence and disagree about the cause -- and the cause is the whole
product, because it is what says which repair would help.

So these tests are about ORDER and PRECEDENCE. The per-compound tests
(`no_props`, `wildcard`) are one-liners against chem_prop and are covered where
they matter, in the backtest against the real member tables.
"""
from __future__ import annotations

import collections

import pytest

from ecspr.bake.direction import forecast as F

pytest.importorskip("rdkit", reason="the forecast profiles compounds with RDKit")


def _profiles(**kw):
    """`{mnxm: CompoundProfile}` from `{mnxm: smiles-or-None}` plus overrides."""
    props = {m: ({"smiles": s, "inchikey": f"KEY-{m}"} if s else {})
             for m, s in kw.items()}
    return F.profile_compounds(list(kw), props)


def test_the_first_failing_participant_is_the_one_named():
    """Two broken participants, one reason -- and it must be the earlier one.

    `EquilibratorMember.dgr` returns inside its loop, so the reason a real run
    records is a fact about position. A forecast that scanned for the "worst"
    problem would report `wildcard` where the member reported `no_smiles`, and the
    carrier work reads exactly that column to decide what to go and curate.
    """
    profiles = _profiles(A=None, B="*C(=O)O", C="O")
    stoich = collections.OrderedDict([("A", -1.0), ("B", -1.0), ("C", 1.0)])
    assert F.predict_dgbyg(stoich, profiles) == ("no_smiles", "A")

    reversed_order = collections.OrderedDict([("B", -1.0), ("A", -1.0), ("C", 1.0)])
    assert F.predict_dgbyg(reversed_order, profiles) == ("wildcard", "B")


def test_a_participant_absent_from_the_props_table_blocks_both_members():
    """The water bug's shape, in miniature: an unlisted participant is a silence.

    Neither member ever reaches chemistry -- and neither can say whether the
    reaction was thermodynamically interesting, which is why the abstention must
    be attributed to the compound rather than to the reaction.
    """
    profiles = _profiles(A="O")
    stoich = collections.OrderedDict([("A", -1.0), ("WATER", 1.0)])
    assert F.predict_dgbyg(stoich, profiles) == ("no_smiles", "WATER")
    assert F.predict_eq(stoich, profiles, None) == ("no_props", "WATER")


def test_balance_is_tested_only_after_every_participant_was_readable():
    """`no_smiles` beats `unbalanced`, because there is no ledger to balance.

    Not a stylistic ordering: an unreadable participant contributes no atoms, so a
    balance check run first would call the reaction unbalanced and blame the
    stoichiometry for what is a missing structure.
    """
    profiles = _profiles(A="O", B=None)
    stoich = collections.OrderedDict([("A", -1.0), ("B", 1.0)])
    assert F.predict_dgbyg(stoich, profiles)[0] == "no_smiles"


def test_hydrogen_is_left_out_of_the_balance_ledger():
    """dGbyG transforms protonation at pH 7, so its H count is not the SMILES'.

    Measured against the r7 member table, heavy-atom balance has no false
    positives in 27,534 reactions that reached the check, while counting H gives
    1,280. The predicate is deliberately the conservative one.
    """
    profiles = _profiles(A="[OH-]", B="O")
    stoich = collections.OrderedDict([("A", -1.0), ("B", 1.0)])   # O vs H2O
    assert F.predict_dgbyg(stoich, profiles) == ("expected_ok", "")

    heavy = _profiles(A="O", B="CO")
    st2 = collections.OrderedDict([("A", -1.0), ("B", 1.0)])      # a carbon appears
    assert F.predict_dgbyg(st2, heavy) == ("unbalanced", "C")


def test_unresolved_is_not_predicted_at_all_when_no_resolution_is_supplied():
    """Silence about a mechanism, rather than a guess at it.

    eQuilibrator's cache is frozen at an older MetaNetX, so whether it holds a
    compound is not a function of chem_prop. With no `resolve` table the forecast
    says `expected_ok`, which the summary records as an UPPER BOUND -- and the
    r7 backtest shows exactly why: 19,120 reactions the member called
    `unresolved` split 11,183 / 7,937 across `expected_ok` and `no_props` on that
    basis alone.
    """
    profiles = _profiles(A="O")
    stoich = collections.OrderedDict([("A", -1.0)])
    assert F.predict_eq(stoich, profiles, None) == ("expected_ok", "")
    assert F.predict_eq(stoich, profiles, {"A": False}) == ("unresolved", "A")
    assert F.predict_eq(stoich, profiles, {"A": True}) == ("expected_ok", "")


def test_every_emitted_mechanism_belongs_to_the_member_that_emitted_it():
    """The closed set, asserted at the row rather than checked in a report.

    `worklist.OUTCOMES`' discipline: a new failure mode must not be able to land
    in an unnamed bucket, and a mechanism must not be attributed to a member whose
    code has no branch for it.
    """
    assert set(F.MEMBER_MECHANISMS) == {"eq", "dgbyg"}
    for member, mechs in F.MEMBER_MECHANISMS.items():
        assert set(mechs) <= set(F.MECHANISMS), member
        assert "expected_ok" in mechs and "no_stoich" in mechs, member
    assert set().union(*F.MEMBER_MECHANISMS.values()) == set(F.MECHANISMS)

    profiles = _profiles(A="O")
    stoich = {"MNXR_ABSENT": ({}, True, False)}
    rows, tally = F.build(["MNXR_ABSENT", "MNXR_MISSING"], stoich, profiles, None)
    assert {r[2] for r in rows if r[0] == "MNXR_MISSING"} == {"no_stoich"}
