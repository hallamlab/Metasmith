"""The adjudication: one closed-set verdict per reaction, and the order it decides in.

The verdict gates are a cascade, and the cascade's ORDER is the behaviour. Three
of the orderings below are not stylistic and have a failure attached to each:

  * atoms are counted only UNDER the character cap. `rxn_smiles` is a
    stoichiometric expansion, so while no metabolite's SMILES exceeds 3,199
    characters MNXR144749's reaction string is 80.7 MB, and RDKit does not
    return from parsing it -- a run once walked 20,000 reactions in three
    seconds and then spent an hour inside one call. The char gate refuses those
    reactions anyway, so the parse was never needed.
  * `non_molecule` outranks every other blocker verdict. There is nothing to
    stand in FOR an electron or a photon, so no rescue lane and no threshold
    change the answer.
  * `no_transfer` outranks `blocked_no_structure`. A blocked reaction whose two
    sides are the same multiset has nothing to map even if the blocker were
    resolved, and calling it blocked would send a rescue lane after it forever.

The over-length fixture is synthetic. Never reach for the real 80.7 MB reaction
to test the bound that exists to avoid parsing it.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit", reason="`count_atoms` is an rdkit parse")

from ecspr.bake.aam import worklist as W


def _reactions(rows: list[dict]) -> pd.DataFrame:
    """The `lookup::reactions` columns `adjudicate` reads, with sane defaults."""
    base = dict(mnxr="MNXR1", substrates=["A"], products=["B"], blockers=[],
                n_blockers=0, rxn_smiles="CC>>CC", is_transport="False",
                is_balanced="True", classifs="")
    return pd.DataFrame([{**base, **r} for r in rows])


def _verdicts(rows, names=None, atom_limit=600, char_limit=8000) -> list[str]:
    df = W.adjudicate(_reactions(rows), names or {}, atom_limit, char_limit)
    return list(df["verdict"])


# --- the closed set -------------------------------------------------------

def test_every_verdict_emitted_is_in_the_declared_set():
    """The ledger's spine: a reaction that produced nothing must say why.

    A verdict outside VERDICTS is a reason the tier-4 gate cannot report, which
    is indistinguishable downstream from a bug.
    """
    rows = [
        dict(mnxr="ok"),
        dict(mnxr="nosub", substrates=[], products=["B"]),
        dict(mnxr="unparse", n_blockers=-1),
        dict(mnxr="blocked", blockers=["X"], n_blockers=1, products=["B"]),
        dict(mnxr="nonmol", blockers=["e-"], n_blockers=1),
        dict(mnxr="same", substrates=["A"], products=["A"], blockers=["X"], n_blockers=1),
        dict(mnxr="nosmi", rxn_smiles=None),
        dict(mnxr="long", rxn_smiles="C" * 9000),
        dict(mnxr="big", rxn_smiles="C" * 700),
    ]
    got = _verdicts(rows, names={"e-": "e-", "X": "an acceptor"})
    assert set(got) <= set(W.VERDICTS), f"verdict outside the closed set: {set(got) - set(W.VERDICTS)}"
    assert got == ["mappable", "pseudo_reaction", "unparseable_equation",
                   "blocked_no_structure", "non_molecule", "no_transfer",
                   "blocked_no_structure", "too_long", "oversize"]


# --- the orderings, each with its failure ---------------------------------

def test_atoms_are_not_counted_above_the_character_cap():
    """The gate that exists so RDKit is never handed an 80 MB string.

    Asserted through the recorded `atoms` column rather than by timing: over the
    char cap the count must be absent, which is the observable form of "the
    parse did not happen". Reordering the two gates would make this None a
    number, and would make the run that spent an hour inside one call possible
    again.
    """
    df = W.adjudicate(_reactions([dict(rxn_smiles="C" * 9000)]), {}, 600, 8000)
    assert df["verdict"][0] == "too_long"
    assert df["atoms"][0] is None, "atoms were counted above the character cap"
    assert df["chars"][0] == 9000, "the character count is recorded either way"


def test_non_molecule_outranks_the_other_blocker_verdicts():
    """An electron has no structure to stand in for, so it is terminal.

    If `blocked_no_structure` won instead, the rescue lanes would be pointed at
    reactions no placeholder can ever complete.
    """
    rows = [dict(blockers=["e-", "some protein"], n_blockers=2,
                 substrates=["A"], products=["B"])]
    assert _verdicts(rows, names={"e-": "e-", "some protein": "a protein"}) == ["non_molecule"]


def test_no_transfer_outranks_blocked_when_both_sides_are_the_same_multiset():
    """Nothing to map even if the blocker resolved -- so it is not a rescue target."""
    rows = [dict(substrates=["A", "B"], products=["B", "A"], blockers=["A"], n_blockers=1)]
    assert _verdicts(rows, names={"A": "a protein"}) == ["no_transfer"]


def test_the_two_size_bounds_are_separate_bounds():
    """`too_long` is about transformer context; `oversize` is about mapper cost.

    Merging them would make the atom threshold unmovable -- it could only be
    changed by changing a character count, which is not what it measures. The
    pin: a reaction can be over one and under the other, in both directions.
    """
    long_but_small = "O" * 300 + ">>" + "O" * 300          # 603 chars, 600 atoms
    assert _verdicts([dict(rxn_smiles=long_but_small)], char_limit=100) == ["too_long"]
    assert _verdicts([dict(rxn_smiles=long_but_small)], atom_limit=100) == ["oversize"]
    assert _verdicts([dict(rxn_smiles=long_but_small)]) == ["mappable"]


def test_unparseable_smiles_is_oversize_not_mappable():
    """`count_atoms` returns None when RDKit cannot read the string.

    None must fall to the refusing side. Treating an uncountable reaction as
    small is how an unbounded one reaches the mapper.
    """
    assert W.count_atoms("this is not smiles") is None
    assert _verdicts([dict(rxn_smiles="!!!not smiles!!!")]) == ["oversize"]


# --- the measurement itself -----------------------------------------------

def test_count_atoms_spans_both_sides_and_counts_copies():
    """What the cap measures today, stated as a fact rather than as an intention.

    `>>` becomes `.` so one parse covers the whole string, and a stoichiometric
    repeat is counted once PER COPY. That second half is the defect the scope
    exists for -- it is pinned here so the collapse change has something to move
    against rather than a claim to argue with.
    """
    assert W.count_atoms("CC>>CC") == 4
    one = W.count_atoms("CCO")
    assert W.count_atoms(".".join(["CCO"] * 16) + ">>C") == 16 * one + 1


# --- the blocker families -------------------------------------------------

@pytest.mark.parametrize("name,family", [
    ("e-", "non_molecule"),
    ("photon", "non_molecule"),
    ("a reduced ferredoxin", "electron_carrier"),
    ("an acyl-carrier protein", "acyl_carrier"),
    ("a holo-tRNA", "trna_holo"),
    ("glycogen", "polymer"),
    ("an acceptor", "generic_rgroup"),
    ("some unnamed thing", "other_structureless"),
    ("", "other_structureless"),
    (None, "other_structureless"),
])
def test_blocker_family_assignment(name, family):
    """First match wins, and the pattern order is a SPECIFICITY order.

    The families name the rescue lanes that aim at them, so a reclassification
    silently retargets a lane. `an acyl-carrier protein` is the case that shows
    the ordering matters: it matches both the acyl_carrier and generic_rgroup
    patterns, and acyl_carrier is listed first.
    """
    assert W.family_of(name) == family
