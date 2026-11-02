# BioNumbers -> the normalised row schema.
#
# BIONUMBERS SUPPLIES THE PHYSICS NO METABOLOMICS DATABASE HOLDS, and under the measured-
# E.-coli rule that turns out to be exactly one compound: DISSOLVED OXYGEN. ECMDB carries
# 1,186 measurements over 891 metabolites and not one of them is a gas or a carbonate,
# because mass spectrometry of a cell extract does not see dissolved gases.
#
# ONE COMPOUND IS STILL THE BIGGEST LEVER IN THE TABLE. O2 appears in 10,378 in-graph
# reactions -- the third most common participant in the whole universe, behind only water
# and the proton, both of which eQuilibrator's prime potentials already handle. Nothing
# else available moves as many rows.
#
# THE ADAPTER MAPS IDENTITY, NOT VALUES. BioNumbers records carry an organism and free text
# and no chemical identifier, so `bionumbers.tsv` states which BNID is about which compound
# and nothing else; the number and its unit are read from the pinned record. Every refusal
# is written down in `bionumbers_rejected.tsv` with its reason.
#
# DISSOLVED CO2 IS THE LARGEST THING THIS DECLINES -- 3,349 in-graph incidences left at the
# default. Every CO2 record BioNumbers has is a solubility in water at a stated partial
# pressure, organism Generic. That is an extracellular equilibrium and a respiring cell
# sits well above it, so there is no E. coli intracellular CO2 measurement to be had.
# Saying so is the deliverable; inventing one would be worse than defaulting.
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from . import Source, UnitRefused, register, to_mM

SELECTION = Path(__file__).with_name("bionumbers.tsv")
REJECTED = Path(__file__).with_name("bionumbers_rejected.tsv")
RECORDS = "records.json"

# BioNumbers writes an approximate value as '~200' and a range as '1.96 (1.1-3.5)'. The
# leading number is the record's own point estimate in both cases.
VALUE = re.compile(r"^\s*[~≈]?\s*([0-9]*\.?[0-9]+)")
# Both micro signs occur in the corpus -- U+00B5 MICRO SIGN and U+03BC GREEK SMALL MU --
# and they are different codepoints, so a single-character comparison misses one of them.
MICRO = {"µ": "u", "μ": "u"}


def _ledger(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep="\t", comment="#", dtype=str)


def _unit(raw: str) -> str:
    out = "".join(MICRO.get(ch, ch) for ch in str(raw)).strip()
    return out


def read(chunk: Path) -> pd.DataFrame:
    chunk = Path(chunk)
    import json
    records = {str(r["bnid"]): r for r in json.loads((chunk / RECORDS).read_text())}
    selection = _ledger(SELECTION)

    out = []
    for row in selection.itertuples(index=False):
        record = records.get(str(row.bnid))
        if record is None:
            raise ValueError(
                f"[bionumbers] BNID {row.bnid} is selected but absent from the pinned "
                f"{RECORDS}. The selection names a record this chunk does not carry, so "
                f"the value it would contribute cannot be checked against its source")
        m = VALUE.match(str(record.get("value") or ""))
        if not m:
            raise ValueError(
                f"[bionumbers] BNID {row.bnid} value {record.get('value')!r} carries no "
                f"leading number. Selected records must be point values, not tables")
        try:
            value = to_mM(float(m.group(1)), _unit(record.get("units")))
        except UnitRefused as e:
            raise ValueError(f"[bionumbers] BNID {row.bnid}: {e}") from e
        out.append(dict(
            source="bionumbers", source_accession=f"BNID:{row.bnid}",
            id_namespace=row.id_namespace, id=row.mnx_accession,
            name=row.compound, organism=record.get("organism") or "",
            strain="", media="", growth_phase="",
            value_mM=value, sd_mM=None,
            citation=f"BNID:{row.bnid}",
        ))
    frame = pd.DataFrame(out)
    refused = _ledger(REJECTED)
    print(f"[bionumbers] {len(frame)} rows over "
          f"{frame['id'].nunique() if len(frame) else 0} compound(s); "
          f"{len(refused)} record(s) refused and recorded")
    return frame


register(Source(name="bionumbers", read=read,
                citation="Milo R et al., BioNumbers, Nucleic Acids Res 2010;38:D750"))
