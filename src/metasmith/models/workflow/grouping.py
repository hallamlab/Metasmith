from __future__ import annotations


InstanceMark = tuple[str, str]


def instance_mark(inst) -> InstanceMark:
    return (inst.parent_lib.GetKey(), str(inst.path))


def ancestor_marks(inst) -> set[InstanceMark]:
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
    if not dep_insts:
        return []
    if len(dep_insts) == 1:
        return list(dep_insts)
    related = related_to_key(dep_insts, key_inst)
    if related:
        return related
    return positional_slice(dep_insts, key_idx, key_idx + 1)


def expected_per_key(dep_insts: list, key_insts: list) -> int | None:
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
