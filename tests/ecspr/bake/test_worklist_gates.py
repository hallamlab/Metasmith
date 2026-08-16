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
    # One component per side, so the collapse has nothing to remove -- both caps
    # therefore act on the same string and the separation is what is being shown.
    long_but_small = "O" * 300 + ">>" + "O" * 300          # 603 chars, 600 atoms
    df = W.adjudicate(_reactions([dict(rxn_smiles=long_but_small)]), {}, 600, 100)
    assert list(df["verdict"]) == ["too_long"]
    df = W.adjudicate(_reactions([dict(rxn_smiles=long_but_small)]), {}, 100, 8000,
                      collapsed_atom_limit=100)
    assert list(df["verdict"]) == ["oversize"]
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


# --- the collapse ---------------------------------------------------------

def test_collapse_writes_each_distinct_molecule_once_per_side():
    """Per side, not across the reaction.

    Water on the left and water on the right are two occurrences that both have
    to exist for the equation to read; the same molecule twice on ONE side is
    the copy the cap should never have counted. A collapse that deduped across
    the reaction would delete one side of every water-conserving reaction.
    """
    assert W.collapse("CC.CC.O>>CCO.O") == "CC.O>>CCO.O"
    assert W.collapse(".".join(["OP(=O)(O)O"] * 16) + ">>N") == "OP(=O)(O)O>>N"
    assert W.collapse("CC>>CC") == "CC>>CC", "an already-distinct reaction must not move"


def test_collapse_splits_only_at_component_boundaries():
    """A `.` inside brackets or parentheses is part of a token, not a boundary.

    Splitting on every `.` would cut molecules in half and then dedupe fragments
    of them -- producing a shorter string that is not the same chemistry, which
    is the one failure mode this whole change must not have.
    """
    assert W.collapse("[Na+].[Cl-]>>[Na+]") == "[Na+].[Cl-]>>[Na+]"
    tricky = "C(=O)([O-])[O-].C(=O)([O-])[O-]>>O"
    assert W.collapse(tricky) == "C(=O)([O-])[O-]>>O"


def test_collapse_never_parses_and_so_is_safe_on_the_unbounded_string():
    """A text split, not a parse. MNXR144749's expanded string is 80.7 MB and
    RDKit does not return from parsing it, so a collapse that parsed first would
    reintroduce the hour-inside-one-call failure the gate ordering exists to
    avoid. Ten megabytes here, and it returns."""
    huge = ".".join(["C" * 40] * 250_000) + ">>C"
    assert len(huge) > 10_000_000
    assert W.collapse(huge) == "C" * 40 + ">>C"


def test_a_reaction_under_the_expanded_cap_keeps_its_expanded_string():
    """The invariant that makes "coverage may only go up" structural.

    A reaction the expanded measure admits is mapped byte for byte as before --
    same universe, same map, same pairs, same weights. Only a reaction that
    would otherwise have been REFUSED is mapped collapsed, so no existing row
    can move, and the monotonicity is a property of the construction rather than
    something to verify after the fact.
    """
    smi = "CC.CC>>CCCC"
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 600, 8000)
    assert df["verdict"][0] == "mappable"
    assert df["rxn_smiles"][0] == smi, "an admitted reaction's string was rewritten"
    assert df["collapsed"][0] is False or not df["collapsed"][0]
    assert df["atoms_collapsed"][0] is None, "a collapse was attempted where none was needed"


def test_a_reaction_the_expanded_cap_refuses_is_recovered_collapsed():
    """The nitrogenase shape: sixteen copies of one cofactor, small chemistry.

    Both counts are recorded, and `collapsed` says which string `rxn_smiles`
    holds -- so no consumer has to infer it from a length, and the pre-collapse
    yield curve stays recomputable from a post-collapse table.
    """
    smi = ".".join(["CCOCCOCCO"] * 40) + ">>CCO"
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 200, 8000)
    assert df["verdict"][0] == "mappable"
    assert bool(df["collapsed"][0]) is True
    assert df["rxn_smiles"][0] == "CCOCCOCCO>>CCO"
    assert df["atoms"][0] > 200, "the expanded count must still describe the input"
    assert df["atoms_collapsed"][0] <= 200


def test_the_collapsed_cap_is_its_own_bound_and_routes_to_indigo():
    """Collapse is a second chance, not an exemption -- and `oversize` is a ROUTE.

    A reaction whose DISTINCT chemistry is genuinely large keeps the `oversize`
    verdict; the verdict set does not grow, because every reader of the worklist
    would then need a branch for a new one. What that verdict now means is
    "the neural members will not see this", not "nothing will".
    """
    smi = ".".join(["C" * 300] * 5) + ">>C"
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 600, 8000,
                      collapsed_atom_limit=100)
    assert df["verdict"][0] == "oversize"
    assert df["atoms_collapsed"][0] == 301, (          # the 300-carbon component + the product
        "the collapsed count is recorded even when the collapse does not rescue")
    # It is still handed to Indigo, so it too carries the SMALLER of the two readings.
    assert bool(df["collapsed"][0]) is True
    assert df["rxn_smiles"][0] == "C" * 300 + ">>C"


def test_the_two_members_read_two_universes_and_only_indigo_takes_the_tail():
    """The whole of the routing change, as the two constants the readers share.

    An `oversize` row is a cost statement about a 512-token transformer and a lane
    that was OOM-killed twice. Indigo is neither, so it reads a wider set -- and
    the sets live in one module precisely so a member cannot drift into seeing a
    universe its siblings do not.
    """
    assert W.NEURAL_ADMITS == ("mappable",)
    assert set(W.INDIGO_ADMITS) == {"mappable", "oversize"}
    assert set(W.NEURAL_ADMITS) < set(W.INDIGO_ADMITS), (
        "the neural universe must stay a subset; a reaction one member sees and "
        "another cannot is what the shared worklist exists to prevent")
    assert all(v in W.VERDICTS for v in W.INDIGO_ADMITS)


def test_the_character_cap_applies_to_the_collapsed_string_and_still_gates_the_parse():
    """`too_long` is about transformer context, so it is about the string the
    mapper SEES -- which for a recovered reaction is the collapsed one. That
    alone rescues part of the too_long tail. The parse stays behind the cap: a
    collapsed string still over it is never handed to RDKit."""
    smi = ".".join(["CCO"] * 4000) + ">>C"          # far over 8,000 chars expanded
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 600, 8000)
    assert df["verdict"][0] == "mappable"
    assert bool(df["collapsed"][0]) is True
    assert df["atoms"][0] is None, "the expanded string was parsed above the char cap"
    assert df["chars_collapsed"][0] < 8000

    still = ".".join(["C" * 9000, "O"]) + ">>C"     # one component alone is over
    df = W.adjudicate(_reactions([dict(rxn_smiles=still)]), {}, 600, 8000)
    assert df["verdict"][0] == "too_long"
    assert df["atoms_collapsed"][0] is None, "a collapsed string over the cap was parsed"


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


# --- the same ordering, in the rescue's completion ------------------------
# `curation.complete` measures a reaction it has just BUILT, so it re-applies these two
# cuts rather than inheriting the worklist's. It did so in the wrong order: it counted
# atoms unconditionally and only then consulted the character cap, which put the
# unbounded parse ahead of the cheap bound that excludes it. A 33-second loop became a
# two-hour one, killed at its walltime with nothing written.

class _StubRefs:
    """Just the attributes `complete` reads. A real Refs is a 1.5 M-row table."""

    def __init__(self, smiles_of):
        self.smiles_of = smiles_of
        self.name_of = {}
        self.formula_of = {}
        self.counts_of = {}
        self.residue_of = {}


def _targets_frame(rows):
    return pd.DataFrame(rows)


def test_complete_gates_the_parse_on_the_character_cap(monkeypatch):
    """The parse must never be reached for a string the char cap already refuses.

    Enforced by making `count_atoms` FAIL on an over-length string rather than by timing
    it: a timing assertion would pass on a fast machine with the order wrong, and the
    failure this guards against is unbounded rather than slow. RDKit does not return from
    MNXR144749's 80.7 MB expansion, so "how long" is not the question.
    """
    from ecspr.bake.aam import curation as C

    original, calls = W.count_atoms, []

    def _counted(smi):
        calls.append(len(smi))
        if len(smi) > 8000:
            raise AssertionError(
                f"count_atoms was handed {len(smi):,} characters, which the char cap "
                f"already refuses -- the parse is ahead of the bound that excludes it")
        return original(smi)

    monkeypatch.setattr(C.aam_worklist, "count_atoms", _counted)

    # One curated body and 400 copies of one substrate: an expansion the char cap
    # refuses, which the collapse recovers to three components.
    subs = ["G"] + ["W"] * 400
    refs = _StubRefs({"W": "C" * 40, "P": "O"})
    refs.formula_of = {"W": "C40H82", "P": "C16000H2"}
    rescued, _bal, _ph, _tag, tally = C.complete(
        refs, {"G": "N"},
        _targets_frame([dict(mnxr="MNXR1", substrates=subs, products=["P"])]),
        smiles_limit=8000, atom_limit=600, collapsed_atom_limit=600)

    assert calls, "the collapsed string was never measured at all"
    assert max(calls) <= 8000
    assert tally["recovered by collapse"] == 1, tally
    assert rescued and rescued[0]["collapsed"] is True
    assert rescued[0]["rxn_smiles"] == "N." + "C" * 40 + ">>O"


def test_complete_keeps_the_expanded_string_when_it_fits():
    """The other half of the same rule: a reaction under both caps is untouched.

    Same invariant the worklist carries -- only a reaction that would otherwise be
    REFUSED is rewritten -- so no row this build already banks can move.
    """
    from ecspr.bake.aam import curation as C

    refs = _StubRefs({"W": "CC", "P": "CC"})
    refs.formula_of = {"W": "C2H6", "P": "C2H6"}
    rescued, _bal, _ph, _tag, tally = C.complete(
        refs, {"G": "N"},
        _targets_frame([dict(mnxr="MNXR1", substrates=["G", "W"], products=["P"])]),
        smiles_limit=8000, atom_limit=600, collapsed_atom_limit=600)
    assert tally["recovered by collapse"] == 0
    assert rescued and rescued[0]["rxn_smiles"] == "N.CC>>CC"
    assert rescued[0]["collapsed"] is False
