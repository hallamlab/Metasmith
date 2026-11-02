# One normalised row per published concentration measurement, and the policy that
# aggregates them.
#
# WHY A SCHEMA RATHER THAN A CURATED LIST. A hand-picked table of forty metabolites is not
# defensible in a publication: the selection is the result. A panel of cited databases under
# one stated policy is, and it is also checkable -- every number here names the databases
# behind it, and no metabolite receives a value by a decision specific to it.
#
# THE POLICY, applied uniformly and never per metabolite:
#
#   1. E. coli intracellular pools only. That grounding is an assumption, stated, not
#      hidden. A blood, serum, urine or CSF concentration is a different physical quantity
#      from a cytoplasmic pool.
#   2. MEASURED values only. A fitted concentration -- one a thermodynamic model solved for
#      -- fed back into a thermodynamic calculation is circular. A stated convention is not
#      evidence. Relative and fold-change data carries no absolute scale.
#   3. Normalise to mM. REFUSE a row whose unit does not parse rather than guessing: a
#      mis-scaled concentration is a silent three-order error in every ratio it touches.
#   4. Aggregate across conditions by GEOMETRIC mean, because a pool spans decades and the
#      arithmetic mean of a log-distributed quantity is a number no condition exhibits.
#   5. Take the log10 spread across contributing rows as the width.
#
# An adapter's whole job is to turn one source into these columns. Every source-specific
# quirk lives in its adapter and nowhere else, which is what keeps the policy above
# readable as one thing rather than as a pile of exceptions.
from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

# `source_accession` is the id IN THE SOURCE, kept so a row can be traced back by hand.
# `id_namespace`/`id` is the handle the MetaNetX join uses (kegg.compound, chebi, hmdb,
# ...). `citation` is the PRIMARY study, not the aggregator -- ECMDB's measurements are
# Bennett 2009 and Ishii 2007, and citing ECMDB for them would misattribute the work.
ROW_COLUMNS = ("source", "source_accession", "id_namespace", "id", "name",
               "organism", "strain", "media", "growth_phase",
               "value_mM", "sd_mM", "citation")

# What a metabolite carries after aggregation. `n_sources`/`sources` are what make the
# panel citable; `n_conditions`/`log10_spread` are what make the width honest.
TABLE_COLUMNS = ("mnxm", "name", "conc_mM", "log10_spread", "n_sources", "sources",
                 "n_conditions", "citations")

TO_MM = {"M": 1e3, "mM": 1.0, "uM": 1e-3, "µM": 1e-3, "nM": 1e-6, "pM": 1e-9}


class UnitRefused(ValueError):
    """A unit that does not parse. Never guessed -- see policy item 3."""


def to_mM(value: float, unit: str) -> float:
    key = str(unit).strip()
    if key not in TO_MM:
        raise UnitRefused(
            f"unit {unit!r} does not parse to a concentration. Guessing would be a silent "
            f"three-order error in every ratio this metabolite touches; known units are "
            f"{sorted(TO_MM)}")
    return float(value) * TO_MM[key]


@dataclass(frozen=True)
class Source:
    # An adapter, registered by name. `read` takes the pinned chunk directory and returns
    # a frame with ROW_COLUMNS.
    name: str
    read: callable
    citation: str


_REGISTRY: dict[str, Source] = {}


def register(source: Source) -> Source:
    if source.name in _REGISTRY:
        raise ValueError(f"source {source.name!r} is already registered")
    _REGISTRY[source.name] = source
    return source


def registry() -> dict[str, Source]:
    return dict(_REGISTRY)


def validate(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    missing = set(ROW_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"[{source}] adapter returned no {sorted(missing)} column(s)")
    bad = frame[frame["value_mM"].isna() | (frame["value_mM"] <= 0)]
    if len(bad):
        raise ValueError(
            f"[{source}] {len(bad)} row(s) carry a null or non-positive concentration. "
            f"A row whose value did not parse must be dropped in the adapter with a "
            f"count, not passed through as a number the aggregator will take the log of")
    return frame[list(ROW_COLUMNS)]


def aggregate(rows: pd.DataFrame, mnxms_of, names: dict | None = None) -> pd.DataFrame:
    # Rows -> one concentration and one width per MNXM.
    #
    # `mnxms_of(namespace, accession) -> [mnxm]` is the MetaNetX join, injected rather than
    # imported: it needs `chem_xref`, which is 678 MB, and building its name index twice is
    # the expensive part of this lane.
    #
    # GEOMETRIC MEAN, and the spread is the log10 range across contributing rows -- so a
    # metabolite measured once gets spread 0.0 and is floored downstream, while one
    # measured across eight carbon sources carries what those conditions actually did.
    names = names or {}
    rows = rows.copy()
    # ONE MEASUREMENT CAN BELONG TO SEVERAL MNXM. MetaNetX splits some pools by anomeric
    # or protonation specification, and a measurement of the pool is a measurement of every
    # id MetaNetX gives it -- so the join returns a set and the rows explode over it.
    rows["mnxm"] = [mnxms_of(ns, acc) for ns, acc in zip(rows["id_namespace"], rows["id"])]
    placed = rows.explode("mnxm")
    placed = placed[placed["mnxm"].notna()]
    out = []
    for mnxm, g in placed.groupby("mnxm"):
        logs = [math.log10(v) for v in g["value_mM"]]
        srcs = sorted(set(g["source"]))
        cites = sorted({c for c in g["citation"] if isinstance(c, str) and c})
        out.append(dict(
            mnxm=mnxm,
            name=names.get(mnxm) or (g["name"].iloc[0] if len(g) else ""),
            conc_mM=10.0 ** (sum(logs) / len(logs)),
            log10_spread=(max(logs) - min(logs)) if len(logs) > 1 else 0.0,
            n_sources=len(srcs), sources="|".join(srcs),
            n_conditions=len(g), citations="|".join(cites),
        ))
    frame = pd.DataFrame(out, columns=list(TABLE_COLUMNS))
    return frame.sort_values("mnxm").reset_index(drop=True)
