"""The cache unit is one group member's invocation, keyed on what it consumed.

A member is one entry of the `lin` payload. Its `PROV` names, per slot, the
own-id of every staged item. The key folds those ids and nothing else: not the
staged paths, not the batch it rode in, not the order of its siblings.
"""

from __future__ import annotations

import pytest

from metasmith.models.lineage import LinPayload
from metasmith.models.workflow.payload import build_entry


def _invocation():
    try:
        from metasmith.caching import invocation
    except ImportError:
        pytest.fail("metasmith.caching.invocation does not exist: the key has no single implementation")
    return invocation


SLOTS = ("seed", "ref")
TK, SIG = "cf::tr", "sig-1"


def _member(paths_a: list[str], paths_ref: list[str], a_ids: list[str], ref_ids: list[str]):
    seed = [(p, {"seed": [i], "root": ["r0"]}) for p, i in zip(paths_a, a_ids)]
    ref = [(p, {"ref": [i]}) for p, i in zip(paths_ref, ref_ids)]
    return build_entry([("seed", seed), ("ref", ref)])


def test_two_members_with_the_same_consumed_ids_share_a_key():
    inv = _invocation()
    m1 = _member(["/run1/work/ab/s1.txt"], ["/run1/work/ab/db"], ["id-s1"], ["id-db"])
    m2 = _member(["/run2/work/cd/s1.txt"], ["/run2/work/cd/db"], ["id-s1"], ["id-db"])
    c1 = inv.consumed_of(m1, SLOTS)
    c2 = inv.consumed_of(m2, SLOTS)
    assert c1 == c2 == {"seed": ["id-s1"], "ref": ["id-db"]}
    assert inv.member_key(TK, SIG, c1) == inv.member_key(TK, SIG, c2)


def test_the_order_of_a_slots_items_does_not_move_the_key():
    inv = _invocation()
    m1 = _member(["/a", "/b"], ["/db"], ["id-a", "id-b"], ["id-db"])
    m2 = _member(["/b", "/a"], ["/db"], ["id-b", "id-a"], ["id-db"])
    k1 = inv.member_key(TK, SIG, inv.consumed_of(m1, SLOTS))
    k2 = inv.member_key(TK, SIG, inv.consumed_of(m2, SLOTS))
    assert k1 == k2


def test_a_different_consumed_id_moves_the_key():
    inv = _invocation()
    m1 = _member(["/a"], ["/db"], ["id-a"], ["id-db"])
    m2 = _member(["/a"], ["/db"], ["id-a2"], ["id-db"])
    assert inv.member_key(TK, SIG, inv.consumed_of(m1, SLOTS)) != inv.member_key(
        TK, SIG, inv.consumed_of(m2, SLOTS)
    )


def test_a_member_whose_item_lacks_its_own_id_is_uncacheable():
    inv = _invocation()
    seed = [("/a", {"root": ["r0"]})]  # nothing under its own channel name
    entry = build_entry([("seed", seed), ("ref", [("/db", {"ref": ["id-db"]})])])
    assert inv.consumed_of(entry, SLOTS) is None


def test_a_member_without_prov_is_uncacheable():
    inv = _invocation()
    entry = {"seed": ["id-a"], "ref": ["id-db"], LinPayload.FILES_KEY: [["/a"], ["/db"]]}
    assert inv.consumed_of(entry, SLOTS) is None


def test_the_key_is_a_blake3_multihash_under_epoch_6():
    inv = _invocation()
    from metasmith.caching.keys import CACHE_KEY_VERSION, KEY_PREFIX

    assert CACHE_KEY_VERSION == 6
    key = inv.member_key(TK, SIG, {"seed": ["id-a"], "ref": ["id-db"]})
    assert isinstance(key, bytes) and key[: len(KEY_PREFIX)] == KEY_PREFIX
    assert len(key) == len(KEY_PREFIX) + 32


def test_a_file_id_does_not_depend_on_its_batch_position():
    at_three = LinPayload.mint_file_id("sid", "3-1-1.abc-annotation-x.tsv")
    at_one = LinPayload.mint_file_id("sid", "1-1-1.abc-annotation-x.tsv")
    assert at_three == at_one, "a member's product changes identity with its position in the batch"


def test_key_is_a_reserved_payload_key():
    assert "KEY" in LinPayload.RESERVED_KEYS
