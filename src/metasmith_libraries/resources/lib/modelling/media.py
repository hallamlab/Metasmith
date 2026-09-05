"""The growth medium table, and what applying one to a model means.

`modelling::media` is a TSV with one row per exchange bound:

    exchange              lower_bound  upper_bound  [medium]
    EX_glc__D_e           -10          1000         minimal_glucose

`lower_bound` is negative for uptake, which is cobra's sign convention. The
optional `medium` column lets one file carry several media, which is what a
conditions table selects between by name.

Applying a medium closes every other exchange. A medium that only opened the
rows it names would leave the model eating whatever the reconstruction happened
to leave open, and two media would then differ by less than they say.
"""
from __future__ import annotations

import pandas as pd

REQUIRED = ("exchange", "lower_bound", "upper_bound")


def load(path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep="\t")
    missing = [c for c in REQUIRED if c not in frame.columns]
    assert not missing, f"[{path}] is missing {missing}; it has {list(frame.columns)}"
    return frame


def apply(model, table: pd.DataFrame, medium: str | None = None) -> dict[str, tuple[float, float]]:
    rows = table
    if medium and "medium" in table.columns:
        rows = table[table.medium.astype(str) == str(medium)]
        assert len(rows), f"no rows in the media table for medium [{medium}]"

    for reaction in model.exchanges:
        reaction.lower_bound = 0.0

    applied: dict[str, tuple[float, float]] = {}
    unknown = []
    for row in rows.itertuples(index=False):
        rid = str(row.exchange)
        if rid not in model.reactions:
            unknown.append(rid)
            continue
        reaction = model.reactions.get_by_id(rid)
        reaction.lower_bound = float(row.lower_bound)
        reaction.upper_bound = float(row.upper_bound)
        applied[rid] = (reaction.lower_bound, reaction.upper_bound)
    if unknown:
        print(f"medium names {len(unknown)} exchanges this model has no reaction for: "
              f"{unknown[:8]}{' ...' if len(unknown) > 8 else ''}")
    return applied
