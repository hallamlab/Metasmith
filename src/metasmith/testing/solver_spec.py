from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "SpecCheck",
    "check_spec",
    "CLAUSES",
]

#: The clauses of `Valid`, in the order `docs/metasmith/solver-spec.md` states
#: them. A decoy test asserts the clause it targets is among those violated, so
#: these names are a contract rather than a label.
CLAUSES = (
    "indexed",
    "shape",
    "conformance",
    "emission",
    "derived",
    "uniqueProducer",
    "provenance",
    "givens",
    "schedulable",
    "target",
)


def _clause_of(violation: str) -> str:
    # A violation is the clause name followed by any of `/`, `[` or a space, and
    # the decoy tests key on the name alone.
    for i, ch in enumerate(violation):
        if ch in "/[ ":
            return violation[:i]
    return violation


@dataclass
class SpecCheck:
    violations: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def __bool__(self) -> bool:
        return self.ok

    def violated(self, clause: str) -> bool:
        return any(_clause_of(v) == clause for v in self.violations)

    def __str__(self) -> str:
        return "plan ok" if self.ok else "; ".join(self.violations)


def check_spec(request: dict, reply: dict) -> SpecCheck:
    res = SpecCheck()
    nodes: list[Any] = request["nodes"]
    eps: list[Any] = reply["endpoints"]
    trs: list[Any] = request["transforms"]
    given_tr, target_tr = request["given_index"], request["target_index"]
    n_props = request["n_properties"]

    # The specification has no given step: the givens are a parameter, and the
    # adapter is what strips the step. Failing here is a malformed reply rather
    # than an unsound plan, so it is reported apart from the clauses.
    given_steps = [s for s in reply["steps"] if s["transform"] == given_tr]
    if len(given_steps) != 1:
        res.violations.append(f"malformed/given-steps={len(given_steps)}")
        return res
    steps = [s for s in reply["steps"] if s["transform"] != given_tr]
    # Deduplicated: two samples may share a structurally identical given -- two
    # `read_metadata` endpoints with no lineage intern to one node -- and the
    # given step then presents that one pair once per group.
    givens: list[tuple[int, Any]] = []
    for g in given_steps[0]["produced"]:
        for _, e in g:
            pair = (e, eps[e]["source_node"])
            if pair not in givens:
                givens.append(pair)

    _check_indexed(res, request, reply, nodes, eps, trs, givens, n_props)
    # Every clause below indexes with ids the reply supplied. An out-of-range one
    # would read a default that satisfies everything, so nothing runs until the
    # ids are known to name something.
    if not res.ok:
        return res

    _check_steps(res, nodes, eps, trs, steps, givens)
    _check_unique_producer(res, steps)
    _check_givens(res, request, nodes, eps, givens)
    _check_schedulable(res, steps)

    n_target = sum(1 for s in steps if s["transform"] == target_tr)
    if n_target != 1:
        res.violations.append(f"target/applications={n_target}")
    return res


def _check_indexed(res, request, reply, nodes, eps, trs, givens, n_props) -> None:
    if not (given_tr_ok := request["given_index"] < len(trs)) or request["target_index"] >= len(trs):
        res.violations.append("indexed/boundary-transform")
        return
    assert given_tr_ok
    for grp in request["given"]:
        for n in grp:
            if n >= len(nodes):
                res.violations.append("indexed/given-node")
    for e, n in givens:
        if e is None or e >= len(eps):
            res.violations.append("indexed/given-endpoint")
        # The pairing comes from `source_node`. A given the reply cannot name is
        # one the `givens` clause could only accept on trust.
        if n is None or n >= len(nodes):
            res.violations.append("indexed/given-source")
    # Parents precede their child in both tables. This is what makes the ancestor
    # walk terminate and the two tables a topological order, and the encoder
    # emits them that way, so a violation is a malformed reply.
    for i, nd in enumerate(nodes):
        for a in nd["parents"]:
            if a >= i:
                res.violations.append(f"indexed/node-order[{i}]")
        for x in nd["props"]:
            if x >= n_props:
                res.violations.append(f"indexed/node-prop[{i}]")
    for i, e in enumerate(eps):
        for f in e["parents"]:
            if f >= i:
                res.violations.append(f"indexed/endpoint-order[{i}]")
        for x in e["props"]:
            if x >= n_props:
                res.violations.append(f"indexed/endpoint-prop[{i}]")
    for i, t in enumerate(trs):
        # A transform must have unique requirements. Two structurally identical
        # ones intern to a single node id, and `shape` would then accept one
        # binding for two inputs.
        if len(t["requires"]) != len(set(t["requires"])):
            res.violations.append(f"indexed/requires-nodup[{i}]")
        for d in t["requires"]:
            if d >= len(nodes):
                res.violations.append(f"indexed/requires[{i}]")
        for g in t["produces"]:
            for d in g:
                if d >= len(nodes):
                    res.violations.append(f"indexed/produces[{i}]")
    for i, s in enumerate(reply["steps"]):
        if s["transform"] >= len(trs):
            res.violations.append(f"indexed/step-transform[{i}]")
            continue
        for d, e in s["used"]:
            if d >= len(nodes) or e >= len(eps):
                res.violations.append(f"indexed/used[{i}]")
        for g in s["produced"]:
            for d, e in g:
                if d >= len(nodes) or e >= len(eps):
                    res.violations.append(f"indexed/produced[{i}]")


def _ancestors(eps, a: int) -> set[int]:
    # Reflexive: an endpoint satisfies its own anchor. Parents strictly decrease,
    # so this terminates without a visited set doing the work.
    out = {a}
    todo = list(eps[a]["parents"])
    while todo:
        x = todo.pop()
        if x in out:
            continue
        out.add(x)
        todo += eps[x]["parents"]
    return out


def _satisfies(nodes, eps, used: dict, a: int, d: int) -> str | None:
    if not set(nodes[d]["props"]).issubset(set(eps[a]["props"])):
        return "props"
    anc = _ancestors(eps, a)
    for anchor in nodes[d]["parents"]:
        # Instancing: the anchor resolves to the endpoint THIS step bound to it,
        # and the binding must descend from that very endpoint. Comparing against
        # anything equal to it instead is what lets one sample's data satisfy a
        # constraint anchored to another's.
        f = used.get(anchor)
        if f is None:
            return "unbound-anchor"
        if f not in anc:
            return "lineage"
    return None


def _check_steps(res, nodes, eps, trs, steps, givens) -> None:
    emitted = {e for s in steps for g in s["produced"] for _, e in g}
    given_eps = {e for e, _ in givens}
    for i, s in enumerate(steps):
        t = trs[s["transform"]]
        used = dict(s["used"])
        if len(used) != len(s["used"]):
            res.violations.append(f"shape/slot-twice[{i}]")
        if sorted(used) != sorted(set(t["requires"])):
            res.violations.append(f"shape/requires[{i}]")
        if len(s["produced"]) != len(t["produces"]):
            res.violations.append(f"shape/group-count[{i}]")
        else:
            for g, declared in zip(s["produced"], t["produces"]):
                if sorted({d for d, _ in g}) != sorted(set(declared)):
                    res.violations.append(f"shape/product-slots[{i}]")

        for d, e in s["used"]:
            if (why := _satisfies(nodes, eps, used, e, d)) is not None:
                res.violations.append(f"conformance/{why}[{i}] slot={d} ep={e}")
            if e not in emitted and e not in given_eps:
                res.violations.append(f"provenance[{i}] ep={e}")

        # The lineage this step confers: what it consumed, and those endpoints'
        # own parents. One hop, which is why the ancestor walk above is transitive.
        consumed = [e for _, e in s["used"]]
        confers = set(consumed) | {p for e in consumed for p in eps[e]["parents"]}
        for g in s["produced"]:
            for d, e in g:
                if (why := _satisfies(nodes, eps, used, e, d)) is not None:
                    res.violations.append(f"emission/{why}[{i}] slot={d} ep={e}")
                # Equality, not containment: a product free to declare extra
                # parents can buy any anchor it likes.
                if set(eps[e]["parents"]) != confers:
                    res.violations.append(f"derived[{i}] ep={e}")


def _check_unique_producer(res, steps) -> None:
    seen: dict[int, int] = {}
    for i, s in enumerate(steps):
        for g in s["produced"]:
            for _, e in g:
                if e in seen and seen[e] != i:
                    res.violations.append(f"uniqueProducer/two-steps ep={e} steps={seen[e]},{i}")
                seen[e] = i


def _check_givens(res, request, nodes, eps, givens) -> None:
    ep_of = {e for e, _ in givens}
    node_of = {n for _, n in givens}
    if len(ep_of) != len(givens) or len(node_of) != len(givens):
        res.violations.append("givens/not-injective")
        return
    # Membership in SOME declared group, never in one group only. The groups are
    # one per sample and a plan legitimately spans all of them -- a multi-sample
    # workflow processes every sample. What keeps a single step from mixing two
    # samples is the lineage anchors in `conformance`, not group membership.
    declared = {n for grp in request["given"] for n in grp}
    for n in sorted(node_of - declared):
        res.violations.append(f"givens/undeclared node={n}")
    e2n = dict(givens)
    n2e = {n: e for e, n in givens}
    for e, n in givens:
        if sorted(set(eps[e]["props"])) != sorted(set(nodes[n]["props"])):
            res.violations.append(f"givens/props ep={e}")
        # A lineage edge on one side exactly when there is one on the other. The
        # declared givens are closed under parents, so no edge dangles.
        for f in eps[e]["parents"]:
            if f not in e2n or e2n[f] not in nodes[n]["parents"]:
                res.violations.append(f"givens/lineage ep={e} parent={f}")
        for a in nodes[n]["parents"]:
            if a not in n2e or n2e[a] not in eps[e]["parents"]:
                res.violations.append(f"givens/lineage node={n} parent={a}")


def _check_schedulable(res, steps) -> None:
    producer: dict[int, int] = {}
    for i, s in enumerate(steps):
        for g in s["produced"]:
            for _, e in g:
                producer[e] = i
    for j, s in enumerate(steps):
        for _, e in s["used"]:
            i = producer.get(e)
            if i is not None and i >= j:
                res.violations.append(f"schedulable[{j}] ep={e} produced-by={i}")
