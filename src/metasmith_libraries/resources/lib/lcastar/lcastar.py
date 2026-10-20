from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

GTDB_RANKS = ("domain", "phylum", "class", "order", "family", "genus", "species")
EC_RANKS = ("class", "subclass", "sub_subclass", "enzyme")
_PREFIX_RANK = {r[0]: r for r in GTDB_RANKS}

# A lineage is a root-first tuple of node labels. A GTDB label keeps its rank
# prefix ("p__Cyanobacteriota"), so the rank is recoverable and labels stay
# unique across ranks. Nodes are keyed by the whole prefix, never by the label
# alone, so a homonym under two parents stays two nodes.
Lineage = tuple


def parse_gtdb(text: str) -> Lineage:
    out = []
    for part in (text or "").split(";"):
        part = part.strip()
        if "__" not in part:
            continue
        prefix, name = part.split("__", 1)
        # Metabuli doubles the prefix: "p_p__Cyanobacteriota".
        rank = prefix.split("_")[-1] if prefix.count("_") else prefix
        if rank not in _PREFIX_RANK or not name:
            continue
        out.append(f"{rank}__{name}")
    return tuple(out)


def gtdb_rank(lineage: Lineage) -> str:
    return _PREFIX_RANK[lineage[-1][0]] if lineage else "root"


def parse_ec(text: str) -> Lineage:
    text = (text or "").strip()
    if text.upper().startswith("EC:"):
        text = text[3:]
    out, parts = [], text.split(".")
    for i, part in enumerate(parts[:len(EC_RANKS)]):
        if part in ("", "-"):
            break
        out.append(".".join(parts[:i + 1]))
    return tuple(out)


def ec_rank(lineage: Lineage) -> str:
    return EC_RANKS[len(lineage) - 1] if lineage else "root"


def to_text(lineage: Lineage) -> str:
    return ";".join(lineage)


def lca(lineages: Iterable[Lineage]) -> Lineage:
    it = iter(lineages)
    common = next(it, ())
    for lin in it:
        n = 0
        for a, b in zip(common, lin):
            if a != b:
                break
            n += 1
        common = common[:n]
        if not common:
            break
    return tuple(common)


@dataclass
class Result:
    lineage: Lineage
    votes: float
    total: float
    fraction: float
    p_value: float
    majority: bool


def supremacy_pvalue(x_k: float, m: float) -> float:
    if x_k <= m:
        return 1.0
    first = m * math.log((2 * m) / (m + x_k)) if m > 0 else 0.0
    second = x_k * math.log((2 * x_k) / (m + x_k))
    t = 2.0 * (first + second)
    # chi-square with one degree of freedom: P(X > t) = erfc(sqrt(t / 2)).
    return math.erfc(math.sqrt(max(t, 0.0) / 2.0))


def lcastar(observations: Iterable[tuple[Lineage, float]], alpha: float = 0.51) -> Result | None:
    counts: dict[Lineage, float] = defaultdict(float)
    for lineage, weight in observations:
        if weight > 0:
            counts[tuple(lineage)] += weight
    total = sum(counts.values())
    if total <= 0:
        return None

    top, top_n = max(counts.items(), key=lambda kv: (kv[1], len(kv[0])))
    if top_n > alpha * total:
        chosen, majority = top, True
    else:
        mass: dict[Lineage, float] = defaultdict(float)
        nlogn: dict[Lineage, float] = defaultdict(float)
        for lineage, n in counts.items():
            term = n * math.log(n)
            for depth in range(len(lineage) + 1):
                node = lineage[:depth]
                mass[node] += n
                nlogn[node] += term
        # Entropy of the vote distribution inside the node's subtree, in nats.
        # Ties go to the deeper node, as the legacy post-order walk did.
        chosen = min(
            (node for node, s in mass.items() if s > alpha * total),
            key=lambda node: (math.log(mass[node]) - nlogn[node] / mass[node], -len(node)),
        )
        majority = False

    depth = len(chosen)
    votes = sum(n for lin, n in counts.items() if lin[:depth] == chosen)
    others = [n for lin, n in counts.items() if lin[:depth] != chosen]
    return Result(
        lineage=chosen,
        votes=votes,
        total=total,
        fraction=votes / total,
        p_value=supremacy_pvalue(votes, max(others, default=0.0)) if chosen else 1.0,
        majority=majority,
    )
