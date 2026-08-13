"""`SourceOf` — pairing two grouped slots by ancestry rather than by position.

A collecting transform receives N of one thing and N of another and has to say
which goes with which. Position cannot answer it: the two lists are accumulated
independently, in task-arrival order, and deduped separately, so
`InputGroup(a)[i]` and `InputGroup(b)[i]` are related only by luck. The on-wire
index knows — every item carries the ancestry hashes it arrived with — and these
tests pin the resolver that reads it.

The semantics live here rather than in a flow test because they are decisions,
not data motion: what to do with no match, with several, and with two slots the
wire genuinely cannot tell apart. Those are worth milliseconds, not a runtime.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries.execution import (
    AmbiguousProvenance,
    AmbiguousSlotChannel,
    ContextData,
    ExecutionContext,
)
from metasmith.models.paths import ContextPath
from metasmith.models.solver import Dependency, Endpoint


def _dep(prop: str) -> Dependency:
    return Dependency(properties={prop}, parents=set())


def _path(name: str) -> ContextPath:
    p = Path("/ws") / name
    return ContextPath(local=p, external=p, container=p)


def _ctx(slots: dict, slot_keys: dict, ambiguous=frozenset()) -> ExecutionContext:
    """One batch member. `slots` is {dep: [(path, index_map), ...]}."""
    member = {}
    for dep, items in slots.items():
        member[dep] = ContextData(
            input_group=[p for p, _ in items],
            endpoint=Endpoint(properties={"x"}),
            type_name="mock::x",
            provenance=[m for _, m in items],
        )
    return ExecutionContext(
        _inputs=[member],
        _get_output_paths=lambda *a, **k: None,
        external_shell=None,
        external_cwd=Path("/ws"),
        external_agent_home=Path("/msm_home"),
        _slot_keys=slot_keys,
        _ambiguous_slots=set(ambiguous),
    )


# -- the designed case -------------------------------------------------------


def _paired_context():
    """Three names and three genomes, deliberately NOT in matching order.

    `gbk_c` is first in its group while its name `nm_c` is last in hers — the
    shape a positional zip gets wrong and ancestry gets right.
    """
    nm, gbk = _dep("name"), _dep("gbk")
    names = [
        (_path("nm_a"), {"NM": ["a"]}),
        (_path("nm_b"), {"NM": ["b"]}),
        (_path("nm_c"), {"NM": ["c"]}),
    ]
    gbks = [
        (_path("gbk_c"), {"NM": ["c"], "GBK": ["gc"]}),
        (_path("gbk_a"), {"NM": ["a"], "GBK": ["ga"]}),
        (_path("gbk_b"), {"NM": ["b"], "GBK": ["gb"]}),
    ]
    ctx = _ctx({nm: names, gbk: gbks}, {nm: "NM", gbk: "GBK"})
    return ctx, nm, gbk


def test_each_genome_resolves_to_its_own_name():
    ctx, nm, gbk = _paired_context()
    got = {
        p.local.name: ctx.SourceOf(p, nm).local.name
        for p in ctx.InputGroup(gbk)
    }
    assert got == {"gbk_a": "nm_a", "gbk_b": "nm_b", "gbk_c": "nm_c"}


def test_position_is_not_the_pairing():
    """The zip a protocol would reach for first gets two of the three wrong."""
    ctx, nm, gbk = _paired_context()
    positional = dict(zip(
        (p.local.name for p in ctx.InputGroup(gbk)),
        (p.local.name for p in ctx.InputGroup(nm)),
    ))
    assert positional == {"gbk_c": "nm_a", "gbk_a": "nm_b", "gbk_b": "nm_c"}


def test_asking_a_slot_about_its_own_item_is_that_item():
    ctx, nm, _ = _paired_context()
    p = ctx.InputGroup(nm)[1]
    assert ctx.SourceOf(p, nm) is p


# -- absence, which is not the same as "no ancestor" -------------------------


def test_no_provenance_captured_answers_none():
    """A runtime that emits none must not look like a file with no ancestors."""
    nm, gbk = _dep("name"), _dep("gbk")
    ctx = _ctx(
        {nm: [(_path("nm_a"), {})], gbk: [(_path("gbk_a"), {})]},
        {nm: "NM", gbk: "GBK"},
    )
    ctx.GetMeta(nm).provenance = []
    ctx.GetMeta(gbk).provenance = []
    assert ctx.SourceOf(_path("gbk_a"), nm) is None


def test_an_unrelated_slot_answers_none():
    nm, gbk = _dep("name"), _dep("gbk")
    ctx = _ctx(
        {nm: [(_path("nm_a"), {"NM": ["a"]})],
         gbk: [(_path("gbk_a"), {"GBK": ["ga"]})]},   # carries no NM ancestry
        {nm: "NM", gbk: "GBK"},
    )
    assert ctx.SourceOf(ctx.InputGroup(gbk)[0], nm) is None


# -- the two refusals --------------------------------------------------------


def test_more_than_one_match_raises_rather_than_picking():
    """A collecting producer has several ancestors at the slot; there is no
    single answer, and returning the first would be the mislabelling bug this
    mechanism exists to remove."""
    nm, gbk = _dep("name"), _dep("gbk")
    ctx = _ctx(
        {nm: [(_path("nm_a"), {"NM": ["a"]}), (_path("nm_b"), {"NM": ["b"]})],
         gbk: [(_path("merged"), {"NM": ["a", "b"], "GBK": ["g"]})]},
        {nm: "NM", gbk: "GBK"},
    )
    p = ctx.InputGroup(gbk)[0]
    with pytest.raises(AmbiguousProvenance, match="descends from 2 items"):
        ctx.SourceOf(p, nm)
    assert {x.local.name for x in ctx.SourcesOf(p, nm)} == {"nm_a", "nm_b"}


def test_two_slots_sharing_a_channel_are_refused():
    """Two requirements of one dtype are one stream on the wire, so an ancestry
    hash cannot say which was meant.

    Note the setup collapses exactly as the real thing does: `a == b` and
    `a.key == b.key`, both being functions of the property set, so they are one
    entry in every map here. That collapse is *why* the refusal exists — the
    duplication survives only in the transform's requires list, which is where
    bootstrap counts it.
    """
    a, b = _dep("same"), _dep("same")
    assert a == b and a.key == b.key, "the collapse this test is about"
    gbk = _dep("gbk")
    ctx = _ctx(
        {a: [(_path("x"), {"SAME": ["1"]})],
         gbk: [(_path("g"), {"SAME": ["1"], "GBK": ["gg"]})]},
        {a: "SAME", gbk: "GBK"},
        ambiguous={"SAME"},
    )
    with pytest.raises(AmbiguousSlotChannel, match="shares channel"):
        ctx.SourceOf(ctx.InputGroup(gbk)[0], b)


def test_a_slot_with_no_recorded_channel_is_refused_by_name():
    """A workspace staged before the compiler recorded slot channels. The
    refusal has to name what is missing, or it reads as a code bug."""
    nm, gbk = _dep("name"), _dep("gbk")
    ctx = _ctx(
        {nm: [(_path("nm_a"), {"NM": ["a"]})],
         gbk: [(_path("gbk_a"), {"NM": ["a"]})]},
        {gbk: "GBK"},   # nm absent
    )
    with pytest.raises(KeyError, match="slk"):
        ctx.SourceOf(ctx.InputGroup(gbk)[0], nm)


def test_hashes_of_mixed_types_do_not_false_match():
    """Leaf ids arrive as hex strings, the legacy seeds mint Longs. A mixed run
    must degrade to no-match, never to a wrong match."""
    nm, gbk = _dep("name"), _dep("gbk")
    ctx = _ctx(
        {nm: [(_path("nm_a"), {"NM": [12345]})],
         gbk: [(_path("gbk_a"), {"NM": ["12345"]})]},
        {nm: "NM", gbk: "GBK"},
    )
    # Same value, different spelling -- str() coercion makes these agree, which
    # is the intended behaviour: one id, written two ways by two seeds.
    assert ctx.SourceOf(ctx.InputGroup(gbk)[0], nm).local.name == "nm_a"
