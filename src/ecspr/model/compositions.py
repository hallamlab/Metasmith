# A community, declared: which members, and which metabolites join them.
#
# A MEMBER IS A CONDITION, not a GPR table. That is the whole reason this table exists
# rather than a list of file names: a member can be perturbed, so "Nostoc beside
# Erythrobacter" and "Nostoc beside a nifH knockout of Erythrobacter" are two compositions
# over the same two annotation tables, and the perturbation is written in the conditions
# table where every other perturbation in this project is written.
#
# TWO LEVELS, AND CONFUSING THEM IS THE MISTAKE THIS FILE EXISTS TO PREVENT:
#
#   genome_unit  ONE PHYSICALLY SEPARATE COPY OF DNA INSIDE ONE CELL -- a chromosome, a
#                plasmid, a fosmid insert, an ASKA clone. Two units are one cell. Their
#                evidence ADDS, and their conductances are on one graph.
#   member       ONE CELL. Two members are two cells. Each gets a PRIVATE copy of every
#                metabolite, and a molecule made by one reaches the other only across a
#                declared bridge.
#
# THE TABLE IS LONG, one row per (composition, member pair, metabolite):
#
#   composition_id   member_a       member_b        on
#   nos_ery          nos_baseline   ery_baseline    MNXM40      (CO2)
#   nos_ery          nos_baseline   ery_baseline    MNXM26      (acetate)
#   nos_alone        nos_baseline                               (a one-member network)
#   nos_ery_open     nos_baseline   ery_baseline                (bridge on everything)
#
# A BLANK `member_b` DECLARES A ONE-MEMBER NETWORK, which is what lets every `ecspr_test`
# name a composition rather than sometimes a composition and sometimes a condition.
#
# A BLANK `on` ON A TWO-MEMBER ROW means "every metabolite both members carry", scored by
# the mismatch in `ecspr.model.compose`. Naming metabolites instead restricts the bridges
# to those, and each named one is REPORTED whether or not an edge came out -- a metabolite
# both members use identically has zero mismatch and yields no edge, which is a real answer
# and reads exactly like a typo unless the report says which it was.
#
# BRIDGES ARE SYMMETRIC. There is no `direction` column and there will not be one: nothing
# in the bake knows which way a metabolite crosses between two cells, and a declared
# direction would be a transport claim no measurement here can check.
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

SEP = "|"

COLUMNS = ("composition_id", "member_a", "member_b", "on")


def _split(v) -> tuple:
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return ()
    if isinstance(v, (list, tuple, set, frozenset)):
        return tuple(str(x) for x in v)
    return tuple(x for x in str(v).split(SEP) if x)


def _scalar(v, default=""):
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return default
    return str(v)


@dataclass(frozen=True)
class Composition:
    """One community: its members, and the metabolites declared to join each pair."""
    composition_id: str
    members: tuple
    #: `{(member_a, member_b): (metabolite, ...)}`, the pair ordered as declared. An empty
    #: tuple of metabolites means "bridge on everything the mismatch finds".
    bridges: dict

    @property
    def is_singleton(self) -> bool:
        return len(self.members) == 1

    def declared_for(self, a: str, b: str) -> tuple:
        return self.bridges.get((a, b)) or self.bridges.get((b, a)) or ()

    def pairs(self) -> tuple:
        return tuple(self.bridges)


def read(path) -> list:
    p = Path(path)
    df = (pd.read_parquet(p) if p.suffix == ".parquet"
          else pd.read_csv(p, sep="\t" if p.suffix in (".tsv", ".txt") else ","))
    missing = {"composition_id", "member_a"} - set(df.columns)
    if missing:
        raise ValueError(f"compositions table {p} has no {sorted(missing)} column(s). "
                         f"The schema is {list(COLUMNS)}")
    out = []
    for cid, g in df.groupby("composition_id", sort=False):
        members, bridges = [], {}
        for row in g.to_dict("records"):
            a = _scalar(row.get("member_a"))
            b = _scalar(row.get("member_b"))
            if not a:
                raise ValueError(f"composition {cid!r} has a row with no member_a")
            for m in (a, b):
                if m and m not in members:
                    members.append(m)
            if not b:
                continue
            key = (a, b) if (a, b) in bridges or (b, a) not in bridges else (b, a)
            bridges[key] = tuple(dict.fromkeys(
                bridges.get(key, ()) + _split(row.get("on"))))
        out.append(Composition(composition_id=str(cid), members=tuple(members),
                               bridges=bridges))
    return out


def write(compositions, path):
    rows = []
    for c in compositions:
        if c.is_singleton and not c.bridges:
            rows.append(dict(composition_id=c.composition_id, member_a=c.members[0],
                             member_b="", on=""))
            continue
        for (a, b), mets in c.bridges.items():
            if not mets:
                rows.append(dict(composition_id=c.composition_id, member_a=a,
                                 member_b=b, on=""))
                continue
            for m in mets:
                rows.append(dict(composition_id=c.composition_id, member_a=a,
                                 member_b=b, on=m))
    df = pd.DataFrame(rows, columns=list(COLUMNS))
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".parquet":
        for col in COLUMNS:
            df[col] = df[col].fillna("").astype(str)
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, sep="\t", index=False)
    return p


def single(composition_id, member) -> Composition:
    """A one-member network, which is what a plain condition looks like to a test."""
    return Composition(composition_id=composition_id, members=(str(member),), bridges={})


def open_pair(composition_id, a, b) -> Composition:
    """Two members bridged on everything the mismatch finds."""
    return Composition(composition_id=composition_id, members=(str(a), str(b)),
                       bridges={(str(a), str(b)): ()})


def resolve_members(composition: Composition, conditions: dict, corpus: dict) -> dict:
    """`{member: the member's GPR rows}` -- each member resolved as a condition.

    THIS IS WHERE A MEMBER BECOMES PERTURBABLE. The rows come out of the same set algebra
    a single-organism measurement uses, so a knockout member and an unperturbed member are
    built by one code path and differ only in their steps.
    """
    from .conditions import resolve
    missing = [m for m in composition.members if m not in conditions]
    if missing:
        raise ValueError(
            f"composition {composition.composition_id!r} names member(s) {missing}, "
            f"which are not condition_ids in the conditions table "
            f"(have {sorted(conditions)[:5]}...)")
    return {m: resolve(conditions[m], corpus) for m in composition.members}
