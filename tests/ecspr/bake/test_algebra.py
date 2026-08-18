"""Conservation algebra: what it may bank, and the three things it may only claim.

The lane's whole risk is a category error -- a number about a molecule written into a
table of atom correspondences. So the assertions are split the way the products are:

  * the conjugate arm cancels on IDENTITY and at EQUAL MULTIPLICITY, and what survives is
    banked only when it indexes a rank list of the matching length;
  * the single-unknown arm and the carrier class produce CLAIMS and nothing else, however
    certain the arithmetic is;
  * a `*` species contributes its explicit atoms and its unspecified slots, and the slots
    have to cancel before either arm is allowed to speak.
"""
from __future__ import annotations

import pytest

pytest.importorskip("rdkit")

from ecspr.bake.aam import algebra as A                                # noqa: E402


def _ctx(counts=None, residue=None, formulas=None, ranks=None, names=None):
    return dict(counts_of=counts or {}, residue_of=residue or {},
                formulas=formulas or {}, ranks_of=ranks or {}, names=names or {})


# =====================================================================
# cancellation
# =====================================================================

def test_a_species_on_both_sides_at_equal_multiplicity_cancels():
    ks, kp, cancelled = A.cancel_conjugates(["X", "A"], ["X", "B"])
    assert cancelled == ["X"]
    assert ks == ["A"] and kp == ["B"]


def test_unequal_coefficients_do_NOT_cancel():
    """`2 X + A >> X + B` leaves one copy of an unknown amount behind.

    Treating X as absent would subtract it from one side only -- the same unfaithfulness
    that makes a naive stoichiometric collapse wrong, and the reason the rule is equal
    multiplicity rather than mere presence.
    """
    ks, kp, cancelled = A.cancel_conjugates(["X", "X", "A"], ["X", "B"])
    assert cancelled == []
    assert ks == ["X", "X", "A"] and kp == ["X", "B"]


def test_a_cancellation_that_empties_a_side_is_refused():
    """Both sides the same multiset is `no_transfer`, not a settled reaction."""
    assert A.cancel_conjugates(["X", "Y"], ["Y", "X"]) is None


# =====================================================================
# the exact arm
# =====================================================================

def test_the_conjugate_arm_banks_the_pairing_conservation_leaves_no_choice_about():
    """One unknown cancels; one carrier of C on each side; a unique bijection at 1.0."""
    forced, claims, _t = A.settle(
        "MNXR1", ["BLOB", "A"], ["BLOB", "B"],
        **_ctx(counts={"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)},
               residue={"A": 0, "B": 0},
               ranks={("A", "C"): [0], ("B", "C"): [0]}))
    assert claims == []
    assert len(forced) == 1
    mnxr, X, sm, pm, n, si, pi, w = forced[0]
    assert (mnxr, X, sm, pm, n) == ("MNXR1", "C", "A", "B", 1)
    assert (si, pi, w) == ("0", "0", "1.0")


def test_n_greater_than_one_spreads_a_total_of_one_over_the_candidates():
    """Nothing is CHOSEN: every candidate correspondence is emitted, diluted by the
    fanout, so each source atom's margin is exactly a confident pairing's."""
    forced, _c, _t = A.settle(
        "MNXR2", ["BLOB", "A"], ["BLOB", "B"],
        **_ctx(counts={"A": (2, 0, 0, 0), "B": (2, 0, 0, 0)},
               residue={"A": 0, "B": 0},
               ranks={("A", "C"): [0, 1], ("B", "C"): [0, 1]}))
    assert len(forced) == 1
    assert forced[0][4] == 4                                   # the 2x2 candidate matrix
    assert set(forced[0][7].split(",")) == {"0.5"}


def test_a_carrier_written_twice_is_refused_rather_than_paired():
    """`atom_ranks` holds ONE rank list per (metabolite, element), so two copies of a
    participant offer two indistinguishable copies of it and a bijection over them would
    be a choice rather than a consequence."""
    forced, _c, tally = A.settle(
        "MNXR3", ["BLOB", "A", "A"], ["BLOB", "B"],
        **_ctx(counts={"A": (1, 0, 0, 0), "B": (2, 0, 0, 0)},
               residue={"A": 0, "B": 0},
               ranks={("A", "C"): [0], ("B", "C"): [0, 1]}))
    assert forced == []
    assert tally["carrier written more than once: C"] == 1


def test_a_count_that_disagrees_with_atom_ranks_is_refused():
    """The ranks ARE the node identity; a pair row is an index into that list. A count
    from anywhere else would index into a list of the wrong length, silently."""
    forced, _c, tally = A.settle(
        "MNXR4", ["BLOB", "A"], ["BLOB", "B"],
        **_ctx(counts={"A": (3, 0, 0, 0), "B": (3, 0, 0, 0)},
               residue={"A": 0, "B": 0},
               ranks={("A", "C"): [0, 1, 2], ("B", "C"): [0, 1]}))
    assert forced == []
    assert tally["count disagrees with atom_ranks: C"] == 1


# =====================================================================
# the claim arms -- computed, reported, never banked
# =====================================================================

def test_the_single_unknown_arm_produces_a_CLAIM_and_no_pair():
    """Conservation fixes the number exactly. It fixes no atom correspondence, and the
    species has no structure to hang a canonical rank on in any case."""
    forced, claims, _t = A.settle(
        "MNXR5", ["A"], ["B", "U"],
        **_ctx(counts={"A": (5, 0, 0, 0), "B": (3, 0, 0, 0)},
               residue={"A": 0, "B": 0},
               ranks={("A", "C"): list(range(5)), ("B", "C"): [0, 1, 2]}))
    assert forced == [], "a species-grain number must never enter the pair table"
    got = [c for c in claims if c[3] == "single_unknown" and c[1] == "C"]
    assert len(got) == 1
    assert got[0][2] == "U" and got[0][4] == 2


def test_the_carrier_class_is_counted_and_not_banked():
    """`Acceptor` on one side and `AH2` on the other is the same cancellation argued from
    NAMES rather than from identity, so it stays a claim and the rescue's per-element
    balance decides whether anything comes of it."""
    forced, claims, tally = A.settle(
        "MNXR6", ["ACC", "A"], ["AH2", "B"],
        **_ctx(counts={"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)},
               residue={"A": 0, "B": 0},
               ranks={("A", "C"): [0], ("B", "C"): [0]},
               names={"ACC": "Acceptor", "AH2": "AH2"}))
    assert tally["carrier-class conjugate (claim only)"] == 1
    assert {c[2] for c in claims if c[3] == "carrier_class"} == {"ACC", "AH2"}
    # The two named roles are still unknowns for every element, so nothing is forced:
    # two unknowns is two unknowns whatever they are called.
    assert forced == []


# =====================================================================
# the residue rule -- the price of reading a count off a `*` structure
# =====================================================================

def test_residue_slots_must_cancel_before_either_arm_may_speak():
    """`C70H131N3O9PS*2` states its explicit atoms exactly and its remainder not at all.

    An exact count of the explicit atoms is not a conservation claim about the molecule,
    so a reaction whose two sides carry different amounts of unspecified remainder has
    been shown to conserve nothing -- and an unknown slot count is a refusal, not a zero.
    """
    base = dict(counts={"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)},
                ranks={("A", "C"): [0], ("B", "C"): [0]})
    ok, _c, _t = A.settle("R", ["BLOB", "A"], ["BLOB", "B"],
                          **_ctx(residue={"A": 1, "B": 1}, **base))
    assert len(ok) == 1

    bad, _c, tally = A.settle("R", ["BLOB", "A"], ["BLOB", "B"],
                              **_ctx(residue={"A": 1, "B": 2}, **base))
    assert bad == [] and tally["residue slots do not cancel: C"] == 1

    unk, _c, tally = A.settle("R", ["BLOB", "A"], ["BLOB", "B"],
                              **_ctx(residue={"A": None, "B": 1}, **base))
    assert unk == [] and tally["residue slots do not cancel: C"] == 1


def test_the_recount_is_read_before_the_formula():
    """A `*` formula refuses; the structure beside it answers. Without that order this
    lane abstains on precisely the species that make its targets unreadable."""
    assert A.counts_for("M", "C", {"M": (70, 3, 1, 1)}, {"M": "C70H131N3O9PS*2"}) == 70
    assert A.counts_for("M", "C", {}, {"M": "C70H131N3O9PS*2"}) is None
    assert A.counts_for("M", "C", {}, {"M": "C6H12O6"}) == 6


def test_an_unknown_count_never_reads_as_zero():
    """Two unknowns is not one unknown and a zero, which is the reading that would let an
    unbalanced reaction be 'settled'."""
    forced, claims, tally = A.settle(
        "R", ["U1", "A"], ["U2", "B"],
        **_ctx(counts={"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)},
               residue={"A": 0, "B": 0},
               ranks={("A", "C"): [0], ("B", "C"): [0]}))
    assert forced == [] and claims == []
    assert tally["more than one unknown: C"] == 1
