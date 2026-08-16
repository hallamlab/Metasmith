"""One pass instead of three: what the forecast may claim, and what the universe asserts.

The collapse rests on three claims and each is checked here rather than argued:

  * THE FORECAST MAY ONLY ADD. Its offer is what the partial lane submits, and the only
    thing that suppresses an offer is a (reaction, element) the algebra already banked --
    where the stack would discard the result unread.
  * THE MECHANISM IS NAMED AND ORDERED. Deterministic rules are reported ahead of the
    prior run's records, so a reaction that is over the character cap AND timed out last
    time reports the cap: it is why no member will see it THIS time either.
  * THE KEYS CANNOT COLLIDE. A member reads the universe into a dict, so a duplicate key
    does not raise -- it drops one of two different molecules submitted under one name.

Plus the two resume mechanisms one pass makes load-bearing: a cache keyed on the
SUBMISSION STRING, and sidecars that re-partition instead of refusing.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit", reason="the forecast reaches atom_pairs for the vocabulary")

from ecspr.bake.aam import forecast as F                                # noqa: E402
from ecspr.bake.aam import partial as P                                 # noqa: E402
from ecspr.bake.aam import shard as S                                   # noqa: E402
from ecspr.bake.aam import universe as U                                # noqa: E402
from ecspr.bake.aam import neural_members as NM                         # noqa: E402

NO_PRIOR = {"prior_timeout": set(), "prior_hang": set(), "prior_empty": set()}


def _prior(**kw):
    d = {k: set(v) for k, v in NO_PRIOR.items()}
    d.update({k: set(v) for k, v in kw.items()})
    return d


# =====================================================================
# the mechanisms
# =====================================================================

def test_the_context_window_fires_on_the_string_and_only_above_the_threshold():
    """RXNMapper's limit is a property of the SUBMISSION, computable before the run.

    512 is not a guess: joined against the prior run's 44,547 recorded outcomes it
    catches 97.2% of the silences while offering 13.2% of the universe, and the
    successful population's p90 is 497.
    """
    below = F.classify(F.CONTEXT_WINDOW_CHARS, 100, "mappable", "MNXR1", NO_PRIOR)
    above = F.classify(F.CONTEXT_WINDOW_CHARS + 1, 100, "mappable", "MNXR1", NO_PRIOR)
    assert below == []
    assert above == ["context_window"]


def test_the_deterministic_mechanism_is_reported_ahead_of_the_recorded_one():
    """Both fired; the cap is the actionable reason.

    A reaction over the character cap reaches NO member this time, whatever happened to
    it last time -- so naming the timeout would send a reader looking for a budget to
    raise on a submission nobody will make.
    """
    fired = F.classify(20_000, None, "too_long", "MNXR1",
                       _prior(prior_timeout={"MNXR1"}))
    assert fired == ["char_cap", "context_window", "prior_timeout"]
    assert F.predict(fired, "too_long") == "silent"


def test_the_atom_cap_is_at_risk_and_not_silent():
    """`oversize` refuses the two neural members and admits Indigo, so one member can
    still answer -- and the reduced submission is what gives the other two something
    they can read."""
    fired = F.classify(300, 900, "oversize", "MNXR1", NO_PRIOR)
    assert "atom_cap" in fired
    assert F.predict(fired, "oversize") == "at_risk"


def test_a_hang_is_derived_from_attempted_minus_returned(tmp_path):
    """The only record of a hang is a shard saying what it was about to do.

    Derived here rather than trusted from `indigo_unreturned.tsv` alone: that file was
    written by one generation of the lane, and the sidecars are what every generation
    leaves behind.
    """
    logs = tmp_path / "logs"
    (logs / "attempted").mkdir(parents=True)
    pd.DataFrame({"mnxr": ["MNXR1", "MNXR2"], "shard": [0, 0],
                  "status": ["ok", "timeout"]}).to_csv(
        logs / "indigo_status.tsv", sep="\t", index=False)
    (logs / "attempted" / "indigo_0.attempted").write_text(
        "#shard 0/8\nMNXR1\nMNXR2\nMNXR3\n")
    prior, had = F.read_prior(logs)
    assert had
    assert prior["prior_timeout"] == {"MNXR2"}
    assert prior["prior_hang"] == {"MNXR3"}, "attempted and never written back"
    # And the denominator: a reaction with no record at all was never asked.
    assert F.prior_coverage(logs) == {"MNXR1", "MNXR2", "MNXR3"}


def test_absent_prior_logs_narrow_nothing_silently():
    """The deterministic half still fires and the summary says the other half was not
    consulted. Reading absence as success is how the empirical half would quietly
    shrink the offer."""
    prior, had = F.read_prior(None)
    assert not had and all(not v for v in prior.values())
    assert F.classify(1_000, 100, "mappable", "MNXR1", prior) == ["context_window"]


# =====================================================================
# what may suppress an offer, and what may not
# =====================================================================

def test_only_an_already_banked_pair_suppresses_an_offer():
    counts = {"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)}
    rows = [("MNXR1", "mappable", 1_000, 100, "whole", ["A"], ["B"])]

    out, _t = F.build(rows, counts, set(), NO_PRIOR)
    assert [r for r in out if r[2] == "C"][0][9] is True

    out, _t = F.build(rows, counts, {("MNXR1", "C")}, NO_PRIOR)
    row = [r for r in out if r[2] == "C"][0]
    assert row[4] == "settled" and row[9] is False


def test_an_unknown_count_makes_the_element_PRESENT_not_absent():
    """`None` is not a zero here either. A structureless participant states nothing
    about its sulfur, and declining to offer on that basis would withhold the reduction
    from exactly the reactions the rescue exists to complete."""
    assert F.elements_of(["A"], ["B"], {"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)}) == ["C"]
    assert F.elements_of(["A"], ["B"], {"A": (1, 0, 0, 0)}) == ["C", "N", "S", "P"]
    assert F.elements_of(["A"], ["B"], {"A": (1, None, 0, 0),
                                        "B": (1, 0, 0, 0)}) == ["C", "N"]


def test_a_reaction_nothing_fired_for_is_offered_nothing():
    out, _t = F.build([("MNXR1", "mappable", 100, 50, "whole", ["A"], ["B"])],
                      {"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)}, set(), NO_PRIOR)
    assert [r[4] for r in out] == ["expected_ok"]
    assert not any(r[9] for r in out)


def test_the_offer_keys_on_the_partial_lane_s_own_convention():
    """One composite-key convention, `partial.KEY_SEP`. A second would be a key the
    universe's uniqueness check cannot reason about."""
    out, _t = F.build([("MNXR1", "mappable", 1_000, 100, "whole", ["A"], ["B"])],
                      {"A": (1, 0, 0, 0), "B": (1, 0, 0, 0)}, set(), NO_PRIOR)
    assert out[0][0] == f"MNXR1{P.KEY_SEP}C"
    assert P.split_key(out[0][0]) == ("MNXR1", "C")


# =====================================================================
# the universe
# =====================================================================

def _wl(tmp_path, rows):
    df = pd.DataFrame(rows, columns=["mnxr", "verdict", "rxn_smiles", "atoms", "chars",
                                     "atoms_collapsed", "chars_collapsed", "collapsed"])
    p = tmp_path / "worklist.parquet"
    df.to_parquet(p, index=False)
    return p


def _rescued(tmp_path, rows):
    df = pd.DataFrame(rows, columns=["mnxr", "verdict", "rxn_smiles", "atoms", "chars"])
    p = tmp_path / "rescued.parquet"
    df.to_parquet(p, index=False)
    return p


def _partial(tmp_path, rows):
    df = pd.DataFrame(rows, columns=list(P.UNIVERSE_COLS))
    p = tmp_path / "partial.parquet"
    df.to_parquet(p, index=False)
    return p


def test_three_classes_land_in_one_table_under_keys_that_cannot_collide(tmp_path):
    """A reaction submitted WHOLE and again as a carbon reduction is two rows, and the
    two ids are distinguishable by shape rather than by a column nobody checks."""
    wl = _wl(tmp_path, [("MNXR1", "mappable", "A>>B", 10, 4, None, None, False)])
    rs = _rescued(tmp_path, [("MNXR2", "mappable", "C>>D", 10, 4)])
    pa = _partial(tmp_path, [(f"MNXR1{P.KEY_SEP}C", "mappable", "A>>B", "MNXR1", "C",
                              ["A"], ["B"], 10, 4, False)])
    rows, tally = U.assemble(wl, rs, pa)
    assert tally == {"whole": 1, "completed": 1, "reduced": 1}
    assert [r[0] for r in rows] == ["MNXR1", "MNXR2", f"MNXR1{P.KEY_SEP}C"]
    # base_mnxr is the REAL reaction on every row; `mnxr` is the submission.
    assert [r[3] for r in rows] == ["MNXR1", "MNXR2", "MNXR1"]


def test_a_duplicate_submission_key_refuses(tmp_path):
    """The rescue completes what the worklist REFUSED, so the two whole classes are
    disjoint on mnxr by construction. If they ever stop being, two different strings
    would be submitted under one id and a member would silently map one of them."""
    wl = _wl(tmp_path, [("MNXR1", "mappable", "A>>B", 10, 4, None, None, False)])
    rs = _rescued(tmp_path, [("MNXR1", "mappable", "X>>Y", 10, 4)])
    with pytest.raises(SystemExit, match="duplicate submission key"):
        U.assemble(wl, rs, None)


def test_a_collapsed_row_carries_the_size_of_the_string_it_will_actually_send(tmp_path):
    """The adjudication keeps the EXPANDED measure so the pre-collapse yield curve stays
    recomputable. The universe must not: a cache keyed on the submission string is
    checked against what was actually sent."""
    wl = _wl(tmp_path, [("MNXR1", "mappable", "A>>B", 1200, 900, 300, 4, True)])
    rows, _t = U.assemble(wl, None, None)
    assert (rows[0][7], rows[0][8]) == (300, 4)


# =====================================================================
# the two resume mechanisms one pass makes load-bearing
# =====================================================================

def test_a_cached_row_is_reused_only_when_its_SUBMISSION_STRING_still_matches(tmp_path):
    """A reaction has one id and up to four strings -- whole, collapsed, rescue-completed,
    element-reduced. Keyed on the id alone the cache serves whichever ran last, which is
    not staleness but a map of a different molecule under the right name."""
    c = tmp_path / "cache.tsv"
    c.write_text("mnxr\trxn_smiles\tmapped_rxn_smiles\tconfidence\n"
                 "MNXR1\tA>>B\tA:1>>B:1\t1.0\n"
                 "MNXR2\tOLD>>OLD\tstale\t1.0\n"
                 "MNXR9\tZ>>Z\twhatever\t1.0\n")
    rows, stale, foreign = S.read_cache([c], {"MNXR1": "A>>B", "MNXR2": "NEW>>NEW"})
    assert [r[0] for r in rows] == ["MNXR1"]
    assert (stale, foreign) == (1, 1)


def test_sidecars_repartition_onto_this_run_s_spec_instead_of_refusing(tmp_path):
    """`crc32(mnxr) % n` is recomputable, so the partition of a UNION is recomputable
    whatever wrote it. Refusing across the change turned a re-shard into a full re-map
    of a lane that had been killed two thirds of the way through."""
    ids = [f"MNXR{i}" for i in range(200)]
    old = []
    for j in range(8):
        p = tmp_path / f"indigo_{j}.attempted"
        mine = [m for m in ids if S.shard_of(m, 8) == j]
        p.write_text(f"#shard {j}/8\n" + "\n".join(mine) + "\n")
        old.append(p)

    kept = S.read_sidecars(old, (0, 28))
    assert kept == {m for m in ids if S.shard_of(m, 28) == 0}
    assert kept, "the re-partition must not be empty for a shard that has members"

    # The union over ALL prior shards is what makes it sound: one file re-filtered would
    # drop the ids that now belong here but were attempted under another file's number.
    only_one = S.read_sidecars(old[:1], (0, 28))
    assert only_one < kept


def test_a_sidecar_with_no_header_is_read_rather_than_refused(tmp_path):
    p = tmp_path / "x.attempted"
    p.write_text("MNXR1\nMNXR2\n")
    assert S.read_sidecars([p], None) == {"MNXR1", "MNXR2"}


def test_a_missing_sidecar_costs_work_and_not_coverage(tmp_path):
    """Its ids are simply re-attempted, which is the direction a resume is allowed to be
    wrong in."""
    assert S.read_sidecars([tmp_path / "nope.attempted"], (0, 4)) == set()


# =====================================================================
# the gap, at the grain one pass demands
# =====================================================================

def test_a_reduced_submission_is_answered_only_when_its_OWN_element_was():
    """With three passes the gap set was per pass and this conflation could not bite. In
    one pass a reaction whose carbon mapped would mark its own NITROGEN reduction as
    covered, and the gap-filler would skip exactly the submission the forecast built."""
    uni = {"MNXR1": "A>>B", f"MNXR1{P.KEY_SEP}N": "a>>b", f"MNXR1{P.KEY_SEP}S": "c>>d"}
    meta = {"MNXR1": ("MNXR1", None),
            f"MNXR1{P.KEY_SEP}N": ("MNXR1", "N"),
            f"MNXR1{P.KEY_SEP}S": ("MNXR1", "S")}
    gap = NM.gap_of(uni, meta, {("MNXR1", "C"), ("MNXR1", "N")})
    assert set(gap) == {f"MNXR1{P.KEY_SEP}S"}, \
        "the whole reaction is answered and so is N; only S is still open"


# =====================================================================
# the containment -- the blocker one pass made non-optional
# =====================================================================

def _hangs_forever(conn, _timeout_s):
    """A child that receives and never answers. Nothing in-process bounds this."""
    import time
    conn.recv()
    while True:
        time.sleep(3600)


def _answers(conn, _timeout_s):
    while True:
        smi = conn.recv()
        if smi is None:
            return
        conn.send((smi + ":mapped", "ok"))


def test_a_search_that_never_returns_is_killed_at_the_budget():
    """THE CLAIM THE OVERSIZED TAIL RESTS ON. `automap` can stall below the Python layer,
    where `signal.alarm` does not reach and its own `aam-timeout` does not bound it -- so
    the sweep's 36% hang rate on reactions above the atom cap used to mean routing them
    into this lane traded them for a stalled member. A child process is killable, so the
    same hang costs one budget and a fork.

    Checked with a child that genuinely never answers, because a mapper that always
    returns cannot test containment.
    """
    from ecspr.bake.aam.indigo_member import Mapper

    m = Mapper(1, serve=_hangs_forever)
    proc = m.proc
    try:
        assert m.map_one("A>>B") == ("", "killed")
        assert m.n_killed == 1
        proc.join(timeout=10)
        assert not proc.is_alive(), "the hung child must be dead, not merely abandoned"
        # AND THE LANE CARRIES ON, which is the whole point: a respawned child is a
        # working mapper, so one runaway search costs a submission rather than a shard.
        assert m.proc is not proc and m.proc.is_alive()
    finally:
        m.close()


def test_the_mapper_is_an_ordinary_pipe_when_nothing_hangs():
    from ecspr.bake.aam.indigo_member import Mapper

    m = Mapper(5, serve=_answers)
    try:
        assert m.map_one("A>>B") == ("A>>B:mapped", "ok")
        assert m.map_one("C>>D") == ("C>>D:mapped", "ok")
        assert (m.n_killed, m.n_respawned) == (0, 0), \
            "a respawn per reaction would make the containment cost the lane"
    finally:
        m.close()


# =====================================================================
# the parquet seam
# =====================================================================
# `build` is exercised above with python lists, which is what a unit test naturally
# writes. The CLI reads its participants from `lookup::reactions`, and a list column read
# back from parquet is a numpy ARRAY -- so a guard spelled `array or []` raises rather
# than defaulting, on the first reaction with two substrates. The tests all passed and
# the lane died in the queue.

def test_participants_survive_the_parquet_round_trip(tmp_path):
    """The array's truth value is ambiguous, and a null-guard must not ask for it."""
    p = tmp_path / "reactions.parquet"
    pd.DataFrame([
        dict(mnxr="MNXR1", substrates=["A", "B"], products=["C"]),
        dict(mnxr="MNXR2", substrates=["A"], products=["B", "C", "D"]),
        dict(mnxr="MNXR3", substrates=[], products=[]),
        dict(mnxr="MNXR4", substrates=None, products=None),
    ]).to_parquet(p, index=False)

    parts = F.participants(p)
    assert parts["MNXR1"] == (["A", "B"], ["C"])
    assert parts["MNXR2"] == (["A"], ["B", "C", "D"])
    assert parts["MNXR3"] == ([], [])
    assert parts["MNXR4"] == ([], []), "a null list column arrives as NaN, not None"
    assert all(isinstance(s, list) and isinstance(q, list)
               for s, q in parts.values()), "a numpy array downstream is a second bug"
