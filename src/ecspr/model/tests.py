# What is MEASURED on a construction, as opposed to what the construction is.
#
# A condition (`ecspr.model.conditions`) says which annotation rows make up a network and
# how much of each. A test says what question is put to it. They were one row until now,
# which meant two probes on one network could not share a construction and the flux
# balance arm had nowhere to put a medium, an objective or a frame.
#
# TWO TABLES, SAME PURPOSE, DIFFERENT CONTENT, because the two arms ask different
# questions of the same construction:
#
#   ecspr_tests  a conductance measurement on the atom graph. Names an element, a source
#                hub, sink hubs and readouts. Runs on a COMPOSITION -- one member is
#                still a composition, with one member.
#   gem_tests    a flux balance measurement. Names a frame, a medium, an objective, a
#                target and a growth pin. Runs on a CONDITION: a community FBA needs
#                compartments and exchange reactions, which is a different construction
#                from `compose`'s atom graph and not one this builds.
#
# `read_against` IS ON BOTH, and it names a TEST rather than a network. A baseline is a
# baseline measurement -- the same probe, on the unperturbed construction, under the same
# medium and orientation. Putting it on the condition would have said a network has one
# baseline, which stops being true the moment two probes share a construction.
#
# `media` LEFT THE ECSPr SIDE ENTIRELY. It was declared on every condition and read by
# nothing: a conductance measurement has no exchange bounds to set. It is a genuine GEM
# input and it lives on `gem_tests` and in `ecspr::media`.
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

SEP = "|"

#: What every test carries, whichever arm it belongs to.
COMMON_COLS = ("test_id", "network", "read_against", "arm", "cohort", "is_control",
               "stratum", "n_units", "draw_id")

#: A conductance measurement. `network` names an `ecspr::compositions` row.
ECSPR_COLS = COMMON_COLS + ("element", "source_hub", "sink_hub", "readout_hub",
                            "probe", "orientation", "weighting")

#: A flux balance measurement. `network` names an `ecspr::conditions` condition_id.
GEM_COLS = COMMON_COLS + ("frame", "media", "objective", "target_hub", "growth_pin",
                          "min_lanes", "carve_threshold")

#: The columns a results table keys on. `test_id`, not `condition_id`: one condition can
#: carry several tests and they must not collapse into one row.
RESULT_KEY = ("test_id", "readout")

ARMS = ("ecspr", "gem")


def _split(v) -> tuple:
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return ()
    if isinstance(v, (list, tuple)):
        return tuple(str(x) for x in v)
    return tuple(x for x in str(v).split(SEP) if x)


def _scalar(v, default=None):
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return default
    return v


@dataclass(frozen=True)
class Test:
    """One measurement on one network. `settings` holds the arm's own columns."""
    test_id: str
    network: str
    arm: str
    read_against: str = ""
    settings: dict = field(default_factory=dict)
    meta: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.arm not in ARMS:
            raise ValueError(f"test {self.test_id!r} has arm {self.arm!r}; "
                             f"one of {ARMS}")

    @property
    def is_control(self) -> bool:
        return bool(self.meta.get("is_control"))

    # -- the ECSPr terminals, named rather than reached through `settings`, because
    # every conductance measurement has all four and a typo in a dict key is silent.
    @property
    def element(self) -> str:
        return str(self.settings.get("element") or "C")

    @property
    def source_hub(self) -> str:
        return str(self.settings.get("source_hub") or "")

    @property
    def sinks(self) -> tuple:
        return _split(self.settings.get("sink_hub"))

    @property
    def readouts(self) -> tuple:
        return _split(self.settings.get("readout_hub"))

    @property
    def probe(self) -> str:
        return str(self.settings.get("probe") or "ground")

    @property
    def media(self) -> str:
        return str(self.settings.get("media") or "")


def _columns_for(arm: str) -> tuple:
    return ECSPR_COLS if arm == "ecspr" else GEM_COLS


def read(path, arm: str, *, defaults=None) -> list:
    """A tests table -> one `Test` per row.

    `defaults` fills a setting the table leaves blank, which is how a driver supplies
    one source hub for a whole sweep without writing it on every row.
    """
    if arm not in ARMS:
        raise ValueError(f"arm must be one of {ARMS}, got {arm!r}")
    p = Path(path)
    df = (pd.read_parquet(p) if p.suffix == ".parquet"
          else pd.read_csv(p, sep="\t" if p.suffix in (".tsv", ".txt") else ","))
    for c in ("test_id", "network"):
        if c not in df.columns:
            raise ValueError(f"{arm}_tests table {p} has no {c!r} column. A test that "
                             f"does not name the network it runs on is not a test")
    cols = _columns_for(arm)
    setting_cols = [c for c in cols if c not in COMMON_COLS]
    defaults = dict(defaults or {})

    out = []
    for row in df.to_dict("records"):
        settings = {c: _scalar(row.get(c), defaults.get(c)) for c in setting_cols}
        settings = {k: v for k, v in settings.items() if v is not None}
        out.append(Test(
            test_id=str(row["test_id"]),
            network=str(row["network"]),
            arm=arm,
            read_against=str(_scalar(row.get("read_against"), "")),
            settings=settings,
            meta={k: row[k] for k in COMMON_COLS
                  if k in row and k not in ("test_id", "network", "read_against")},
        ))
    _check_baselines(out, p)
    return out


def _check_baselines(tests, path) -> None:
    """Every `read_against` names a test in the same table.

    A baseline that resolves to nothing is a scorer silently comparing a measurement
    against zero, which reads as a very large effect rather than as a broken reference.
    """
    known = {t.test_id for t in tests}
    dangling = sorted({t.read_against for t in tests
                       if t.read_against and t.read_against not in known})
    if dangling:
        raise ValueError(f"{path}: read_against names {dangling}, which are not "
                         f"test_ids in this table")


def write(tests, path, arm: str):
    cols = _columns_for(arm)
    rows = []
    for t in tests:
        r = dict(test_id=t.test_id, network=t.network, read_against=t.read_against)
        r.update({k: t.meta.get(k) for k in COMMON_COLS
                  if k not in ("test_id", "network", "read_against")})
        r["arm"] = arm
        for c in cols:
            if c in COMMON_COLS:
                continue
            v = t.settings.get(c)
            r[c] = SEP.join(v) if isinstance(v, (list, tuple)) else v
        rows.append(r)
    df = pd.DataFrame(rows, columns=list(cols))
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".parquet":
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].fillna("").astype(str)
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, sep="\t", index=False)
    return p


def single(test_id, network, arm="ecspr", **settings) -> Test:
    return Test(test_id=test_id, network=network, arm=arm, settings=settings)
