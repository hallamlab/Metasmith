"""The conditions table: one row per thing to measure.

A condition is TERMINALS plus a MASK. The terminals say where current is injected
and where it is read; the mask says which GPR rows are in the network while that
happens. Nothing else. That is why ``ecspr draw`` can emit its null pool as a
conditions table indistinguishable from a study's own -- the null arm is then the
identical probe command with a different ``--conditions`` file, and "one pool
shared across every arm" is file identity rather than a convention someone has to
keep.

COLUMNS
-------
Required: ``condition_id``.
Terminals: ``element``, ``source_hub``, ``sink_hub`` (``|``-joined for several),
``media``. Missing ones fall back to the command line's defaults, so a table that
measures one substrate against one precursor set need not repeat it per row.
``readout_hub`` is a FILTER, not a terminal: under the universal ground every
metabolite has a draw, and which ones are written out changes no physics. It is
separate from ``sink_hub`` because naming a metabolite as a sink gives it a PORT
to ground instead of a leak, which does change the answer -- reading a metabolite
and porting it are two different asks and one column cannot mean both.
Mask, three ``(column, values)`` pairs with the values ``|``-joined:
``background_column`` / ``background_values`` is what is always in (the host),
``mask_column`` / ``mask_values`` is what this condition adds, and
``drop_column`` / ``drop_values`` withholds rows the other two let through.
See :mod:`ecspr.model.gpr` for the semantics and why the background is stated.
Study metadata, carried through and never interpreted here: ``arm``, ``cohort``,
``is_control``, ``stratum``, ``n_units``, ``draw_id``.

``is_control`` is the one field scoring reads. A control is a unit the study
asserts is a no-op; its mask reaches no atom-mapped reaction, so it must return
the baseline exactly, and the spread over the controls is therefore the numerical
floor every z-score has to clear. See :mod:`ecspr.model.scoring`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

SEP = "|"

REQUIRED = ("condition_id",)
TERMINAL_COLS = ("element", "source_hub", "sink_hub", "readout_hub", "media")
MASK_COLS = ("background_column", "background_values", "mask_column", "mask_values",
             "drop_column", "drop_values")
META_COLS = ("arm", "cohort", "is_control", "stratum", "n_units", "draw_id")
COLUMNS = REQUIRED + TERMINAL_COLS + MASK_COLS + META_COLS


def _split(v) -> tuple:
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return ()
    if isinstance(v, (list, tuple)):
        return tuple(str(x) for x in v)
    return tuple(x for x in str(v).split(SEP) if x)


def _scalar(v):
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return None
    return v


@dataclass(frozen=True)
class Condition:
    condition_id: str
    element: str = "C"
    source_hub: str = ""
    sinks: tuple = ()
    readouts: tuple = ()
    media: str = ""
    background_column: str = None
    background_values: tuple = ()
    mask_column: str = None
    mask_values: tuple = ()
    drop_column: str = None
    drop_values: tuple = ()
    meta: dict = field(default_factory=dict)

    @property
    def is_control(self) -> bool:
        return bool(self.meta.get("is_control"))

    def mask_kwargs(self) -> dict:
        return dict(background_column=self.background_column,
                    background_values=self.background_values,
                    mask_column=self.mask_column, mask_values=self.mask_values,
                    drop_column=self.drop_column, drop_values=self.drop_values)


def read(path, *, element=None, source=None, sinks=(), readouts=(),
         media="") -> list:
    """A conditions file (parquet or TSV) -> ``[Condition, ...]``.

    The command line's ``--element`` / ``--source`` / ``--sinks`` are DEFAULTS for
    rows that do not carry their own; a row that names its own terminals always
    wins, because the table is the experiment's claim about what it is testing.
    """
    p = Path(path)
    df = (pd.read_parquet(p) if p.suffix == ".parquet"
          else pd.read_csv(p, sep="\t" if p.suffix in (".tsv", ".txt") else ","))
    for c in REQUIRED:
        if c not in df.columns:
            raise ValueError(f"conditions table {p} has no {c!r} column")

    out = []
    for row in df.to_dict("records"):
        src = _scalar(row.get("source_hub")) or source
        snk = _split(row.get("sink_hub")) or tuple(sinks)
        if not src:
            raise ValueError(f"condition {row['condition_id']!r} names no source_hub "
                             f"and no --source default was given")
        out.append(Condition(
            condition_id=str(row["condition_id"]),
            element=str(_scalar(row.get("element")) or element or "C"),
            source_hub=str(src), sinks=snk,
            readouts=_split(row.get("readout_hub")) or tuple(readouts),
            media=str(_scalar(row.get("media")) or media or ""),
            background_column=_scalar(row.get("background_column")),
            background_values=_split(row.get("background_values")),
            mask_column=_scalar(row.get("mask_column")),
            mask_values=_split(row.get("mask_values")),
            drop_column=_scalar(row.get("drop_column")),
            drop_values=_split(row.get("drop_values")),
            meta={k: row[k] for k in META_COLS if k in row},
        ))
    return out


def write(conditions, path):
    """``[Condition, ...]`` -> a conditions file. What ``ecspr draw`` emits."""
    rows = []
    for c in conditions:
        r = dict(condition_id=c.condition_id, element=c.element,
                 source_hub=c.source_hub, sink_hub=SEP.join(c.sinks),
                 readout_hub=SEP.join(c.readouts), media=c.media,
                 background_column=c.background_column or "",
                 background_values=SEP.join(c.background_values),
                 mask_column=c.mask_column or "",
                 mask_values=SEP.join(c.mask_values),
                 drop_column=c.drop_column or "",
                 drop_values=SEP.join(c.drop_values))
        r.update({k: c.meta.get(k) for k in META_COLS})
        rows.append(r)
    df = pd.DataFrame(rows, columns=list(COLUMNS))
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".parquet":
        # Object columns with mixed None/str do not round-trip stably through
        # parquet, and a null pool that is not byte-identical between two runs of
        # the same seed is not a shared pool. Cast once, here.
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].fillna("").astype(str)
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, sep="\t", index=False)
    return p


def single(condition_id, *, element, source, sinks, readouts=(),
           media="") -> Condition:
    """The explicit ``--source`` / ``--sinks`` form: the whole GPR table as one
    unit, with NO mask. A one-shot probe does not accept a mask on purpose --
    masking is what a conditions table is for, and two ways to say it is exactly
    the drift this package exists to end."""
    return Condition(condition_id=condition_id, element=element, source_hub=source,
                     sinks=tuple(sinks), readouts=tuple(readouts), media=media)
