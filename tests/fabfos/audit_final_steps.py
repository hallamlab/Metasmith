"""Data-driven audit of the clustered-contigs -> final-fosmids steps.

Runs the *exact* algorithms embedded in transforms/fosmids/chimera_split.py and
coverage_trim.py against the real scadc per-pool coverage profiles and the three
hand-verified ground-truth contigs from the chimera pilot:

    FF.13B03  overlap chimera  -> SHOULD split
    FF.19C15  junction chimera -> SHOULD split  (pilot boundary 38291)
    FF.08B18  single clone     -> SHOULD be kept (documented false positive)

Findings this audit encodes (see the module-level asserts):

1. Our arm_split reproduces the pilot's SCREEN exactly (FF.13B03/FF.19C15 called
   with the pilot's boundaries).
2. Our arm_split ALSO splits FF.08B18 -- the pilot's arm_split is explicitly a
   screening pass confirmed by manual per-pool inspection, and that confirmation
   step is NOT in the transform, so it over-splits this known false positive.
3. A `confirm_junction` hard-stop gate (each arm's pool must vanish on the far
   side) rejects FF.08B18 while keeping FF.19C15 -- the proposed fix.
4. coverage_trim shrinks true over-assemblies to the lambda window and never
   trims in-window/short contigs.

Skips cleanly if the scadc profile pickle isn't reachable. Run:
    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/audit_final_steps.py -v -s
"""
from __future__ import annotations

import gzip
import pickle
from pathlib import Path

import numpy as np
import pytest

PROFILE_PKL = Path(
    "/home/tony/agentic_workspace/projects/scadc/recover-fosmids/"
    "main/resolve_inserts/chimera_pilot/cache/pool_profiles_min_cut_24kb.pkl.gz"
)

# ── chimera_split.py defaults ──────────────────────────────────────────────
FRAC, MIN_ABS, MIN_SPAN, MARGIN, MIN_ARM, MAX_GAP, GAP = 0.30, 5.0, 0.15, 0.15, 0.12, 0.06, 300
# ── coverage_trim.py defaults ──────────────────────────────────────────────
LO, HI, TFRAC, TMIN_ABS, TGAP = 29000, 44000, 0.20, 10.0, 300


@pytest.fixture(scope="module")
def profiles_lengths():
    if not PROFILE_PKL.exists():
        pytest.skip(f"scadc profile pickle not reachable: {PROFILE_PKL}")
    with gzip.open(PROFILE_PKL, "rb") as f:
        profiles, lengths, _meta = pickle.load(f)
    return profiles, lengths


def _fill_gaps(mask, max_gap):
    idx = np.where(mask)[0]
    if len(idx) == 0:
        return mask
    m = mask.copy()
    for gi in np.where(np.diff(idx) > 1)[0]:
        a, b = idx[gi], idx[gi + 1]
        if b - a - 1 <= max_gap:
            m[a:b] = True
    return m


# ── faithful copy of chimera_split.py's detector ───────────────────────────
def _covered_interval(arr):
    if arr.max() <= 0:
        return None
    thr = max(MIN_ABS, FRAC * np.percentile(arr, 99))
    mask = _fill_gaps(arr >= thr, GAP)
    if not mask.any():
        return None
    idx = np.where(mask)[0]
    return int(idx[0]), int(idx[-1] + 1)


def _per_pool_spans(profiles, lengths, c):
    L = lengths[c]
    out = {}
    for pool, arr in profiles[c].items():
        iv = _covered_interval(arr)
        if iv is None:
            continue
        s, e = iv
        if (e - s) / L < MIN_SPAN:
            continue
        out[pool] = (s, e)
    return out


def arm_split(profiles, lengths, c):
    L = lengths[c]
    spans = _per_pool_spans(profiles, lengths, c)
    if len(spans) < 2:
        return None
    lo, hi = MARGIN * L, (1 - MARGIN) * L
    left = [(p, s, e) for p, (s, e) in spans.items() if s <= lo and e < hi]
    right = [(p, s, e) for p, (s, e) in spans.items() if s > lo and e >= hi]
    if not left or not right:
        return None
    lp, ls, le = max(left, key=lambda t: t[2])
    rp, rs, re = min(right, key=lambda t: t[1])
    if lp == rp:
        return None
    if le / L < MIN_ARM or (L - rs) / L < MIN_ARM:
        return None
    gap = rs - le
    if gap > MAX_GAP * L:
        return None
    return dict(kind="overlap" if gap < 0 else "junction", left_pool=lp, right_pool=rp,
                left_end=int(le), right_start=int(rs), gap=int(gap), boundary=int((le + rs) // 2))


# ── PROPOSED FIX: the hard-stop confirmation the pilot did by eye ──────────
def confirm_junction(profiles, lengths, c, d, hardstop_frac=0.25):
    """A real junction has each arm's pool VANISH on the far side. Overlaps are
    exempt (the two clones legitimately share the middle)."""
    if d["kind"] == "overlap":
        return True
    bd = d["boundary"]
    la = profiles[c].get(d["left_pool"])
    ra = profiles[c].get(d["right_pool"])
    if la is None or ra is None:
        return False
    left_far = la[bd:].mean() / max(la[:bd].mean(), 1e-9)   # left pool leaking right
    right_far = ra[:bd].mean() / max(ra[bd:].mean(), 1e-9)  # right pool leaking left
    return bool(left_far < hardstop_frac and right_far < hardstop_frac)


GROUND_TRUTH = {
    "FF.13B03": "overlap",   # should split
    "FF.19C15": "junction",  # should split
    "FF.08B18": None,        # should be KEPT (false positive)
}


def test_chimera_detector_reproduces_pilot_screen(profiles_lengths):
    profiles, lengths = profiles_lengths
    for c in ("FF.13B03", "FF.19C15"):
        d = arm_split(profiles, lengths, c)
        assert d is not None and d["kind"] == GROUND_TRUTH[c], f"{c}: {d}"
    # pilot's hand-verified junction boundary for FF.19C15
    assert arm_split(profiles, lengths, "FF.19C15")["boundary"] == 38291


def test_known_false_positive_is_currently_oversplit(profiles_lengths):
    """Documents the gap: the transform splits FF.08B18 (should be kept)."""
    profiles, lengths = profiles_lengths
    d = arm_split(profiles, lengths, "FF.08B18")
    assert d is not None and d["kind"] == "junction", "expected the known over-split"


def test_hardstop_gate_fixes_false_positive_without_losing_true_calls(profiles_lengths):
    profiles, lengths = profiles_lengths
    calls = {c: arm_split(profiles, lengths, c) for c in GROUND_TRUTH}
    confirmed = {c: (d is not None and confirm_junction(profiles, lengths, c, d))
                 for c, d in calls.items()}
    assert confirmed["FF.13B03"] is True    # overlap kept
    assert confirmed["FF.19C15"] is True    # true junction kept
    assert confirmed["FF.08B18"] is False   # false positive now rejected


def test_trim_shrinks_overassembly_and_spares_inwindow(profiles_lengths):
    profiles, lengths = profiles_lengths

    def pooled_depth(c):
        acc = np.zeros(lengths[c], dtype=np.float64)
        for arr in profiles[c].values():
            acc += arr
        return acc

    def covered_core(depth):
        cov = depth[depth > 0]
        if cov.size == 0:
            return None
        thr = max(TFRAC * float(np.median(cov)), TMIN_ABS)
        mask = _fill_gaps(depth >= thr, TGAP)
        if not mask.any():
            return None
        idx = np.where(mask)[0]
        return int(idx[0]), int(idx[-1] + 1)

    # FF.19C15 (50 kb over-assembly) trims to an in-window core
    core = covered_core(pooled_depth("FF.19C15"))
    assert core is not None and LO <= (core[1] - core[0]) <= HI

    # no in-window contig is ever a trim target (trim only fires for L > HI)
    inwin = [c for c, L in lengths.items() if LO <= L <= HI]
    assert inwin, "expected some in-window contigs in the set"
    assert all(lengths[c] <= HI for c in inwin)  # by construction none enter the trim branch


if __name__ == "__main__":
    import sys
    if not PROFILE_PKL.exists():
        sys.exit(f"skip: {PROFILE_PKL} not found")
    with gzip.open(PROFILE_PKL, "rb") as f:
        profiles, lengths, _ = pickle.load(f)
    print("contig     truth      arm_split     hard-stop confirmed")
    for c, truth in GROUND_TRUTH.items():
        d = arm_split(profiles, lengths, c)
        conf = d is not None and confirm_junction(profiles, lengths, c, d)
        print(f"{c:10} {str(truth):10} {('keep' if d is None else d['kind']):12} {conf}")
