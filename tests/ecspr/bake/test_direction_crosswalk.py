# The supplementary MetaCyc crosswalk: matching the reactions reac_xref never joined.
#
# 545 of MetaCyc's 18,591 directed reactions carry no `metacyc.reaction` row in reac_xref,
# so the curated member never sees them -- and one of them, `GLYCOPHOSPHORYL-RXN`, holds
# the physiological direction for glycogen phosphorylase.
#
# Everything here is about the one hazard `curated.py` exists for. REACTION-DIRECTION is
# stated in MetaCyc's orientation and MNXref re-canonicalises on import, so a join that
# guesses at identity can invert a direction as easily as supply one -- silently, and with
# a confident-looking category attached. The tests are therefore about what the matcher
# REFUSES far more than about what it matches.
from __future__ import annotations

import pytest

from ecspr.bake.direction import curated as C


SIDES = {
    "MNXR1": (frozenset({"MNXM_A", "MNXM_B"}), frozenset({"MNXM_C"})),
    "MNXR2": (frozenset({"MNXM_D"}), frozenset({"MNXM_E"})),
    "MNXR6": (frozenset({"MNXM_P", "MNXM_Q"}), frozenset({"MNXM_R"})),
    "MNXR4": (frozenset({"MNXM_X", "MNXM_Y"}), frozenset({"MNXM_Z"})),
    "MNXR5": (frozenset({"MNXM_X"}), frozenset({"MNXM_Y", "MNXM_Z"})),
}
CMAP = {c: f"MNXM_{c}" for c in "ABCDEPQRXYZ"}

# A held-out corpus the key gets right, so `supplementary_map` can be exercised without
# every test also having to satisfy the precision floor.
TRUTH = {"HELD-1": "MNXR1", "HELD-2": "MNXR2", "HELD-6": "MNXR6"}


def rec(uid, left, right, direction="LEFT-TO-RIGHT"):
    return dict(unique_id=uid, left=list(left), right=list(right), direction=direction)


HELD = [rec("HELD-1", "AB", "C"), rec("HELD-2", "D", "E"), rec("HELD-6", "PQ", "R")]


def idx():
    return C._participant_index(SIDES)


def test_a_record_whose_sides_are_an_mnxrs_sides_is_matched():
    assert C._match_one(rec("R", "AB", "C"), idx(), SIDES, CMAP) == ("MNXR1", None)


def test_the_match_is_orientation_blind_because_alignment_is_not_its_job():
    m, _ = C._match_one(rec("R", "C", "AB"), idx(), SIDES, CMAP)
    assert m == "MNXR1"


def test_a_shared_compound_inventory_is_not_identity():
    m, why = C._match_one(rec("R", "P", "QR"), idx(), SIDES, CMAP)
    assert (m, why) == (None, "sides_do_not_correspond")


def test_an_inventory_two_mnxrs_share_identifies_neither():
    m, why = C._match_one(rec("R", "XY", "Z"), idx(), SIDES, CMAP)
    assert (m, why) == (None, "ambiguous")


def test_a_partially_mapped_equation_is_refused_whole():
    m, why = C._match_one(rec("R", ["A", "NOT_A_KNOWN_COMPOUND"], "C"), idx(), SIDES, CMAP)
    assert (m, why) == (None, "compound_unmapped")


def test_a_record_no_mnxr_holds_is_refused_rather_than_approximated():
    m, why = C._match_one(rec("R", "D", "C"), idx(), SIDES, CMAP)
    assert (m, why) == (None, "no_mnxr_has_those_participants")


def test_an_empty_side_is_refused():
    assert C._match_one(rec("R", "", "C"), idx(), SIDES, CMAP) == (None, "empty_side")


def test_the_arm_never_lands_on_an_mnxr_the_primary_crosswalk_already_claims():
    supp, ledger, _ = C.supplementary_map(
        HELD + [rec("GAP-RXN", "AB", "C"), rec("REACHABLE-RXN", "XY", "Z")],
        TRUTH, SIDES, CMAP)
    assert "GAP-RXN" not in supp
    assert ledger["primary_already_claims_it"] == 1


def test_a_record_the_primary_crosswalk_already_joined_is_never_rematched():
    supp, _, _ = C.supplementary_map(HELD, TRUTH, SIDES, CMAP)
    assert supp == {}


def test_a_reachable_gap_record_is_matched_and_reported():
    sides = dict(SIDES, MNXR7=(frozenset({"MNXM_A"}), frozenset({"MNXM_E"})))
    supp, ledger, _ = C.supplementary_map(
        HELD + [rec("GAP-RXN", "A", "E")], TRUTH, sides, CMAP)
    assert supp == {"GAP-RXN": "MNXR7"}
    assert ledger["matched"] == 1


def test_the_key_is_priced_against_reac_xrefs_own_answers():
    # reac_xref is ground truth on every record it joins, so the key is measured rather
    # than argued about -- and the measurement is a build gate, not a printout.
    correct, fired = C.crosswalk_precision(HELD, TRUTH, idx(), SIDES, CMAP)
    assert (correct, fired) == (3, 3)


def test_a_key_that_stops_identifying_reactions_fails_the_build():
    poisoned = dict(TRUTH, **{"HELD-1": "MNXR2"})
    with pytest.raises(AssertionError, match="no longer identifying reactions"):
        C.supplementary_map(HELD, poisoned, SIDES, CMAP)


def test_a_matched_record_is_still_flipped_into_mnxr_orientation():
    aligned, reason = C.align_one("LEFT-TO-RIGHT", ["C"], ["A", "B"], SIDES["MNXR1"], CMAP)
    assert (aligned, reason) == ("RIGHT-TO-LEFT", "flipped")


def test_an_undecidable_orientation_yields_no_direction_rather_than_a_guess():
    aligned, reason = C.align_one("LEFT-TO-RIGHT", ["D"], ["E"], SIDES["MNXR1"], CMAP)
    assert (aligned, reason) == (None, "no_overlap")
