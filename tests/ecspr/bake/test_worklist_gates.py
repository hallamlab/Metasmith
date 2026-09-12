from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit", reason="`count_atoms` is an rdkit parse")

from ecspr.bake.aam import worklist as W


def _reactions(rows: list[dict]) -> pd.DataFrame:
    base = dict(mnxr="MNXR1", substrates=["A"], products=["B"], blockers=[],
                n_blockers=0, rxn_smiles="CC>>CC", is_transport="False",
                is_balanced="True", classifs="")
    return pd.DataFrame([{**base, **r} for r in rows])


def _verdicts(rows, names=None, atom_limit=600, char_limit=8000) -> list[str]:
    df = W.adjudicate(_reactions(rows), names or {}, atom_limit, char_limit)
    return list(df["verdict"])


def test_every_verdict_emitted_is_in_the_declared_set():
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


def test_atoms_are_not_counted_above_the_character_cap():
    df = W.adjudicate(_reactions([dict(rxn_smiles="C" * 9000)]), {}, 600, 8000)
    assert df["verdict"][0] == "too_long"
    assert df["atoms"][0] is None, "atoms were counted above the character cap"
    assert df["chars"][0] == 9000, "the character count is recorded either way"


def test_non_molecule_outranks_the_other_blocker_verdicts():
    rows = [dict(blockers=["e-", "some protein"], n_blockers=2,
                 substrates=["A"], products=["B"])]
    assert _verdicts(rows, names={"e-": "e-", "some protein": "a protein"}) == ["non_molecule"]


def test_no_transfer_outranks_blocked_when_both_sides_are_the_same_multiset():
    rows = [dict(substrates=["A", "B"], products=["B", "A"], blockers=["A"], n_blockers=1)]
    assert _verdicts(rows, names={"A": "a protein"}) == ["no_transfer"]


def test_the_two_size_bounds_are_separate_bounds():
    long_but_small = "O" * 300 + ">>" + "O" * 300
    df = W.adjudicate(_reactions([dict(rxn_smiles=long_but_small)]), {}, 600, 100)
    assert list(df["verdict"]) == ["too_long"]
    df = W.adjudicate(_reactions([dict(rxn_smiles=long_but_small)]), {}, 100, 8000,
                      collapsed_atom_limit=100)
    assert list(df["verdict"]) == ["oversize"]
    assert _verdicts([dict(rxn_smiles=long_but_small)]) == ["mappable"]


def test_unparseable_smiles_is_oversize_not_mappable():
    assert W.count_atoms("this is not smiles") is None
    assert _verdicts([dict(rxn_smiles="!!!not smiles!!!")]) == ["oversize"]


def test_count_atoms_spans_both_sides_and_counts_copies():
    assert W.count_atoms("CC>>CC") == 4
    one = W.count_atoms("CCO")
    assert W.count_atoms(".".join(["CCO"] * 16) + ">>C") == 16 * one + 1


def test_collapse_writes_each_distinct_molecule_once_per_side():
    assert W.collapse("CC.CC.O>>CCO.O") == "CC.O>>CCO.O"
    assert W.collapse(".".join(["OP(=O)(O)O"] * 16) + ">>N") == "OP(=O)(O)O>>N"
    assert W.collapse("CC>>CC") == "CC>>CC", "an already-distinct reaction must not move"


def test_collapse_splits_only_at_component_boundaries():
    assert W.collapse("[Na+].[Cl-]>>[Na+]") == "[Na+].[Cl-]>>[Na+]"
    tricky = "C(=O)([O-])[O-].C(=O)([O-])[O-]>>O"
    assert W.collapse(tricky) == "C(=O)([O-])[O-]>>O"


def test_collapse_never_parses_and_so_is_safe_on_the_unbounded_string():
    huge = ".".join(["C" * 40] * 250_000) + ">>C"
    assert len(huge) > 10_000_000
    assert W.collapse(huge) == "C" * 40 + ">>C"


def test_a_reaction_under_the_expanded_cap_keeps_its_expanded_string():
    smi = "CC.CC>>CCCC"
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 600, 8000)
    assert df["verdict"][0] == "mappable"
    assert df["rxn_smiles"][0] == smi, "an admitted reaction's string was rewritten"
    assert df["collapsed"][0] is False or not df["collapsed"][0]
    assert df["atoms_collapsed"][0] is None, "a collapse was attempted where none was needed"


def test_a_reaction_the_expanded_cap_refuses_is_recovered_collapsed():
    smi = ".".join(["CCOCCOCCO"] * 40) + ">>CCO"
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 200, 8000)
    assert df["verdict"][0] == "mappable"
    assert bool(df["collapsed"][0]) is True
    assert df["rxn_smiles"][0] == "CCOCCOCCO>>CCO"
    assert df["atoms"][0] > 200, "the expanded count must still describe the input"
    assert df["atoms_collapsed"][0] <= 200


def test_the_collapsed_cap_is_its_own_bound_and_routes_to_indigo():
    smi = ".".join(["C" * 300] * 5) + ">>C"
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 600, 8000,
                      collapsed_atom_limit=100)
    assert df["verdict"][0] == "oversize"
    assert df["atoms_collapsed"][0] == 301, (
        "the collapsed count is recorded even when the collapse does not rescue")
    assert bool(df["collapsed"][0]) is True
    assert df["rxn_smiles"][0] == "C" * 300 + ">>C"


def test_the_two_members_read_two_universes_and_only_indigo_takes_the_tail():
    assert W.NEURAL_ADMITS == ("mappable",)
    assert set(W.INDIGO_ADMITS) == {"mappable", "oversize"}
    assert set(W.NEURAL_ADMITS) < set(W.INDIGO_ADMITS), (
        "the neural universe must stay a subset; a reaction one member sees and "
        "another cannot is what the shared worklist exists to prevent")
    assert all(v in W.VERDICTS for v in W.INDIGO_ADMITS)


def test_the_character_cap_applies_to_the_collapsed_string_and_still_gates_the_parse():
    smi = ".".join(["CCO"] * 4000) + ">>C"
    df = W.adjudicate(_reactions([dict(rxn_smiles=smi)]), {}, 600, 8000)
    assert df["verdict"][0] == "mappable"
    assert bool(df["collapsed"][0]) is True
    assert df["atoms"][0] is None, "the expanded string was parsed above the char cap"
    assert df["chars_collapsed"][0] < 8000

    still = ".".join(["C" * 9000, "O"]) + ">>C"
    df = W.adjudicate(_reactions([dict(rxn_smiles=still)]), {}, 600, 8000)
    assert df["verdict"][0] == "too_long"
    assert df["atoms_collapsed"][0] is None, "a collapsed string over the cap was parsed"


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
    assert W.family_of(name) == family


class _StubRefs:
    def __init__(self, smiles_of):
        self.smiles_of = smiles_of
        self.name_of = {}
        self.formula_of = {}
        self.counts_of = {}
        self.residue_of = {}


def _targets_frame(rows):
    return pd.DataFrame(rows)


def test_complete_gates_the_parse_on_the_character_cap(monkeypatch):
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
