# The set4 biomass axes, and the lipid backbone axes added beside them.
#
# set4 is 79 hand-curated source->sink axes over four elements. It is graph-independent --
# the same table whichever universe the solve runs on -- so it carries a frozen sha rather
# than a producer, and `REFERENCES.md` C1 declares it git text for that reason. It lived
# outside this repository until now, behind an absolute path into a project directory that
# was later emptied; `SET4` below is the recovered copy and `SET4_SHA` is the pin every
# consumer already asserted against the old location.
#
# THE EXTENSION IS A SECOND FILE, and that is the whole design. set4's sha is asserted by
# consumers this module does not own, so an axis added by editing set4 would break them
# for a reason that has nothing to do with them. Adding a file leaves set4 byte-identical
# and makes "which axes are set4" answerable by pointing at one sha.
#
# WHAT THE LIPID EXTENSION IS FOR. set4's `phospholipid` category is five axes and every
# one arrives at PE or PG from the acyl-chain side (AcCoA), the head-group side (L-serine)
# or the phosphate side. None of them carries the glycerol backbone, whose carbon enters at
# DHAP and reaches the head group through gpsA, plsB/plsC and CDP-DAG -- so an insert that
# builds backbone and no acyl chain is invisible to set4 as frozen. The three added axes
# are that entry: glucose to DHAP, and DHAP to each of PE and PG.
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CURATED = REPO / "data/fabfos/curated/ECSPr_axes"

SET4 = CURATED / "biomass_dag_axes_set4.json"
SET4_SHA = "f63c6c8c7636b099aa872634f0a2a327222df6165057407e01fbf9c283d8c44c"
SET4_N = 79

LIPID_EXT = CURATED / "biomass_dag_axes_set4_lipid_ext.json"

COLUMNS = ("axis_id", "element", "src_mnxm", "dst_mnxm", "category", "route",
           "module_id", "module_name", "description", "purpose", "axis_set")


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _rows(path: Path, axis_set: str) -> list:
    spec = json.loads(Path(path).read_text())
    out = []
    for axis_id, r in spec.items():
        if len(r["source"]) != 1 or len(r["sink"]) != 1:
            raise ValueError(f"{path.name}: axis {axis_id} is not 1->1")
        out.append(dict(axis_id=axis_id, element=r["element"],
                        src_mnxm=r["source"][0], dst_mnxm=r["sink"][0],
                        category=r["category"], route=r["route"],
                        module_id=r["module_id"], module_name=r["module_name"],
                        description=r["description"], purpose=r["purpose"],
                        axis_set=axis_set))
    return out


def load(*, lipid_ext: bool = True) -> pd.DataFrame:
    # The sha is checked on every load rather than once at recovery, because the failure it
    # guards against is an EDIT, and an edit happens after recovery.
    got = sha256(SET4)
    if got != SET4_SHA:
        raise SystemExit(
            f"{SET4} has sha {got}\n"
            f"  set4 is frozen at   {SET4_SHA}\n"
            f"  add an axis in a second file; do not edit set4")
    rows = _rows(SET4, "set4")
    if len(rows) != SET4_N:
        raise ValueError(f"expected {SET4_N} set4 axes, got {len(rows)}")
    if lipid_ext:
        rows += _rows(LIPID_EXT, "lipid_ext")

    df = pd.DataFrame(rows, columns=list(COLUMNS))
    # (element, source, sink) is what a measurement IS, so two rows sharing it are one
    # test under two names -- and they would be scored against each other's null.
    dup = df[df.duplicated(["element", "src_mnxm", "dst_mnxm"], keep=False)]
    if len(dup):
        raise ValueError(
            f"axes duplicated on (element, source, sink):\n{dup.to_string(index=False)}")
    return df
