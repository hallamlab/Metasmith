"""Which instances belong to which `group_by` key — one answer, four callers.

`group_by` partitions a step's inputs by the grouping requirement's instances:
one instance is one key, and one key's task member holds EVERY item matched to
it. Answering "which items" is a lineage question, not a positional one, and it
had been answered positionally in two places (`virtual_runtime`,
`cache_decisions`) while the Nextflow runtime answered it by lineage join. The
disagreement is invisible for a dependency that fans out ALONGSIDE the key
(instance i of each lines up either way) and silently loses data for one that
COLLECTS into it, where N instances descend from a single key and a positional
slice hands over one of them.

`expected_per_key` exists so the runtime can flush a key the moment it is whole
instead of waiting for its channel to close. The risk there is asymmetric and
shapes the whole function: over-counting degrades to the close-flush that
already happens, while UNDER-counting emits a partial group — the exact failure
this module exists to prevent. So it answers `None` unless the attribution is
unambiguous and identical for every key.
"""

from __future__ import annotations


InstanceMark = tuple[str, str]


def instance_mark(inst) -> InstanceMark:
    """Identity of one instance across libraries: (library key, path)."""
    return (inst.parent_lib.GetKey(), str(inst.path))


def ancestor_marks(inst) -> set[InstanceMark]:
    """Every instance `inst` descends from, itself included."""
    marks: set[InstanceMark] = set()
    stack = [inst]
    while stack:
        curr = stack.pop()
        m = instance_mark(curr)
        if m in marks:
            continue
        marks.add(m)
        for pm in curr.parent_lib.parents.get(curr.path, []):
            if pm.path in curr.parent_lib.manifest:
                stack.append(curr.parent_lib.Get(pm.path))
    return marks


def related_to_key(dep_insts: list, key_inst) -> list:
    """The instances of one dependency that descend from / are descended by
    `key_inst`. Empty when nothing relates — the caller decides the fallback.

    Both directions count: the dependency may descend from the grouping key
    (the collecting case), or the key may descend from the dependency (grouping
    by a fan-out output while still needing its shared parent).
    """
    if not dep_insts or key_inst is None:
        return []
    key_mark = instance_mark(key_inst)
    key_ancestors = ancestor_marks(key_inst)
    return [
        inst
        for inst in dep_insts
        if key_mark in ancestor_marks(inst) or instance_mark(inst) in key_ancestors
    ]


def positional_slice(insts: list, start: int, end: int) -> list:
    """Fallback for a dependency with no lineage relation to the key: line the
    instances up by position, broadcasting a singleton to every key."""
    if len(insts) == 0:
        return []
    if len(insts) == 1:
        return list(insts)
    chunk = list(insts[start:end])
    if chunk:
        return chunk
    if start < len(insts):
        return [insts[start]]
    return [insts[-1]]


def select_for_key(dep_insts: list, key_inst, key_idx: int) -> list:
    """One group key's slice of one dependency. Lineage first, position after."""
    if not dep_insts:
        return []
    if len(dep_insts) == 1:
        # A shared reference DB or container image: every key sees it.
        return list(dep_insts)
    related = related_to_key(dep_insts, key_inst)
    if related:
        return related
    return positional_slice(dep_insts, key_idx, key_idx + 1)


def expected_per_key(dep_insts: list, key_insts: list) -> int | None:
    """How many items of `dep_insts` each key will receive, or None if unsure.

    None means "flush this stream at channel close" — always safe. A number
    means the runtime may emit a key as soon as its bag reaches it, so this
    only answers when EVERY key has a lineage-attributed slice and all the
    slices are the same size.

    A dependency holding ONE instance answers None, not 1. The plan is
    archetypal: a collecting step's input slot carries a single instance
    standing for however many the fan-out above it produces at runtime, and
    a shared reference DB carries a single instance that really is one. The
    two are indistinguishable here, and guessing 1 flushes a collecting key
    after its FIRST item — the exact shatter this whole change exists to
    undo. So a slot that cannot be attributed per key waits for close.
    """
    if not dep_insts or not key_insts:
        return None
    if len(dep_insts) == 1:
        return None
    sizes = set()
    for key_inst in key_insts:
        related = related_to_key(dep_insts, key_inst)
        if not related:
            return None
        sizes.add(len(related))
        if len(sizes) > 1:
            return None
    return sizes.pop() if sizes else None
