"""The recount: what it recovers, and the two things it must never do.

The lane exists because a `*` formula states no count while the SMILES beside it states
one exactly. The danger in that trade is symmetric and both halves are asserted here:

  * a species whose count is genuinely unknown must come back NULL, never zero -- a zero
    is a claim, and it is the claim that lets an unbalanced reaction pass a balance gate;
  * an exact count of the EXPLICIT atoms is not a claim about the whole molecule, so the
    residue slots have to be carried and made to cancel before a balance believes it.
"""
from __future__ import annotations

import pytest

pytest.importorskip("rdkit")

from ecspr.bake import atom_pairs as AP                                # noqa: E402
from ecspr.bake.aam import curation as C                               # noqa: E402
from ecspr.bake.aam import recount as R                                # noqa: E402


def test_there_is_one_formula_counter_in_the_tree():
    """Three byte-identical copies of this used to exist and had to agree.

    `curation.count_formula` is now the extractor's function under its old name. The
    third copy lived in `mnx_lookups.py`, which imports it too -- that one cannot be
    asserted from here because it is library code staged into a container, so its
    guard is the `buildlib::ecspr` requirement on its transform.
    """
    assert C.count_formula is AP.count_element


@pytest.mark.parametrize("formula,smiles,inchi,expect_c,source", [
    # The lever: a residue formula refuses, the structure beside it answers exactly.
    ("C6H10O5*", "OC[C@H]1O[C@@H](*)[C@H](O)[C@@H](O)[C@@H]1O", None, 6, "smiles"),
    # No structure, but the `*` core is still countable once the marker is stripped.
    ("C70H131N3O9PS*2", None, None, 70, "star_formula"),
    # A plain formula with no structure stays on the formula route.
    ("C6H12O6", None, None, 6, "formula"),
    # Last resort: a single-component InChI formula layer.
    (None, None, "InChI=1S/C2H4O2/c1-2(3)4/h1H3,(H,3,4)", 2, "inchi"),
])
def test_each_route_answers_and_names_itself(formula, smiles, inchi, expect_c, source):
    counts, _residue, got = R.recount(formula, smiles, inchi)
    assert got == source
    assert counts["C"] == expect_c


@pytest.mark.parametrize("formula,smiles,inchi", [
    (None, None, None),                     # nothing said it
    ("(C6H10O5)n", None, None),             # a polymer: nesting
    ("C6H10O5*n", None, None),              # a residue whose core is still a polymer
    (None, None, "InChI=1S/C6H12O6.H2O/c1-2"),   # multi-component: the layer sums
])
def test_an_unknown_count_is_null_and_never_zero(formula, smiles, inchi):
    counts, residue, source = R.recount(formula, smiles, inchi)
    assert source == "none"
    assert residue is None
    for X in R.ELEMENTS:
        assert counts[X] is None, f"{X} came back {counts[X]!r}; a zero here is a claim"


def test_the_structure_wins_over_the_formula():
    """Because `atom_ranks` is keyed on the structure.

    `len(ranks)` for (mnxm, element) IS this count, so a count taken from anywhere else
    can disagree with the number of nodes a pair row indexes into.
    """
    _counts, _residue, source = R.recount("C6H12O6", "CCO", None)
    assert source == "smiles"


def test_a_wildcard_atom_contributes_to_no_element_and_is_reported_as_residue():
    counts, residue, source = R.recount(None, "CC(=O)N[*]", None)
    assert source == "smiles"
    assert counts["C"] == 2 and counts["N"] == 1
    assert residue == 1


@pytest.mark.parametrize("subs,prods,expect", [
    ([1, 0], [1, 0], True),      # one residue each side -- it cancels
    ([1], [0], False),           # a residue appears from nowhere
    ([None], [0], None),         # unknown is a refusal, not a zero
    ([0, 0], [0, 0], True),      # no residues at all
])
def test_residue_slots_must_cancel_before_a_balance_believes_the_count(subs, prods, expect):
    assert R.residue_slots_cancel(subs, prods) is expect


def test_star_multiplicity_is_read_from_the_marker():
    _counts, residue, _source = R.recount("C28H47N3O10PS*2", None, None)
    assert residue == 2
    _counts, residue, _source = R.recount("C28H47N3O10PS*", None, None)
    assert residue == 1


def test_a_star_formula_whose_core_is_still_uncountable_is_refused():
    counts, _residue, source = R.recount("(C6H10O5)n*", None, None)
    assert source == "none"
    assert counts["C"] is None


@pytest.mark.parametrize("formula", ["Mn", "Zn", "Sn", "In", "Rn", "C4H12Sn"])
def test_a_trailing_n_that_is_an_ELEMENT_is_not_mistaken_for_a_polymer(formula):
    """Measured against MNXref 4.5, and it is why no route guards a bare `n`.

    The formula column holds ZERO `(...)n` polymers -- MetaNetX writes an unspecified
    remainder as `*` instead -- and all 131 formulas ending in `n` are Mn / Zn / Sn /
    In / Rn. A guard that refused a trailing `n` would refuse the manganese and zinc
    cofactors and nothing else, so the polymer guard is scoped to an `n` that follows
    no capital.
    """
    counts, residue, source = R.recount(formula, None, None)
    assert source == "formula"
    assert residue == 0
    assert all(counts[X] is not None for X in R.ELEMENTS)
