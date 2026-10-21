# ECMDB 2.0 -> the normalised row schema.
#
# ECMDB supplies COVERAGE, and it is already at its ceiling. Its alias-expanded join
# reaches 232 MNXM, covering a third of in-graph solute incidences, and widening it is not
# worth attempting: 708 of the 709 measurements it cannot place are enumerated lipid
# species -- `CL(16:0/16:1(9Z)/16:1(9Z)/18:1(9Z))` and its siblings, one entry per
# acyl-chain combination -- and no upstream database assigns an identifier to an individual
# acyl-chain combination, so there is nothing to join to.
#
# THE NAMESPACES ARE NESTED, NOT COMPLEMENTARY, which is why this adapter prefers KEGG and
# falls through rather than unioning everything it can find: kegg reaches 181 MNXM, chebi
# 168, hmdb 173, biocyc 166, and their union is 182. One more accession for four times the
# join surface.
#
# TWO FILES, because the measurements and the identifiers live apart. `concentrations.json`
# carries the value and the growth condition keyed by an M2MDB id; `ecmdb.json.zip` carries
# the kegg/chebi/hmdb crosswalk for that id and no concentrations whatsoever.
#
# THE CITATION IS THE PRIMARY STUDY, never ECMDB. These measurements are Bennett 2009 and
# Ishii 2007 among others, and attributing them to the aggregator would misattribute the
# work.
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

from . import Source, register

# Preference order, applied uniformly. Never per metabolite.
NAMESPACES = (("kegg_id", "kegg.compound"), ("chebi_id", "chebi"),
              ("hmdb_id", "hmdb"), ("biocyc_id", "metacyc.compound"))

BULK = "ecmdb.json.zip"
CONCENTRATIONS = "concentrations.json"


def _crosswalk(chunk: Path) -> dict[str, dict]:
    # M2MDB id -> the record's identifiers. Read from the zip as served: unpacking belongs
    # to this tier, not to the acquisition.
    with zipfile.ZipFile(chunk / BULK) as z:
        records = json.loads(z.read("ecmdb.json"))
    return {r["m2m_id"]: r for r in records if r.get("m2m_id")}


def _accession(record: dict):
    for field, namespace in NAMESPACES:
        value = record.get(field)
        if value:
            # ChEBI ids are stored bare here and prefixed in MetaNetX.
            if namespace == "chebi" and not str(value).upper().startswith("CHEBI"):
                value = f"CHEBI:{value}"
            return namespace, str(value)
    return None, None


def read(chunk: Path) -> pd.DataFrame:
    chunk = Path(chunk)
    rows = json.loads((chunk / CONCENTRATIONS).read_text())
    crosswalk = _crosswalk(chunk)

    out, unparsed, unplaceable = [], 0, set()
    for r in rows:
        value = r.get("conc_mM")
        if value is None or value <= 0:
            # The scraper already refused this row's unit rather than guessing it. Dropping
            # it here with a count is what keeps that refusal visible.
            unparsed += 1
            continue
        record = crosswalk.get(r.get("m2m")) or {}
        namespace, accession = _accession(record)
        if accession is None:
            unplaceable.add(r.get("ecmdb"))
            continue
        out.append(dict(
            source="ecmdb", source_accession=r.get("ecmdb"),
            id_namespace=namespace, id=accession,
            name=r.get("name") or record.get("name") or "",
            organism="Escherichia coli", strain=r.get("strain") or "",
            media=r.get("media") or "", growth_phase=r.get("growth") or "",
            value_mM=float(value), sd_mM=r.get("sd_mM"),
            citation=r.get("citation") or "",
        ))
    print(f"[ecmdb] {len(out)} rows placed, {unparsed} with an unparsed unit, "
          f"{len(unplaceable)} metabolites carrying no upstream identifier")
    return pd.DataFrame(out)


register(Source(name="ecmdb", read=read,
                citation="Wishart DS et al., ECMDB 2.0, Nucleic Acids Res 2016;44:D495"))
