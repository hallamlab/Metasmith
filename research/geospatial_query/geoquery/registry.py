"""Every adapter spec, deduplicated, joined to its family and observation type."""
import csv, importlib
from pathlib import Path
from . import pipeline as P

LANES = ["adapters_health_env", "adapters_census", "adapters_physical",
         "adapters_infrastructure", "adapters_s2signature", "adapters_embeddings"]
HERE = Path(__file__).parent


def _tsv(name, key):
    with open(HERE / name) as fh:
        return {r[key]: r for r in csv.DictReader(fh, delimiter="\t")}


SOURCES = _tsv("sources.tsv", "source")
UNITS = {k: r["SI_unit"] for k, r in _tsv("feature_units.tsv", "feature").items()}


def specs():
    """(slot, source, family, licence, auth, fn) for every adapter, first (slot, source) wins."""
    out, seen = [], set()
    for spec in [*P.ADAPTERS, *(s for m in LANES
                                for s in importlib.import_module(f"geoquery.{m}").ADAPTERS)]:
        if (spec[0], spec[1]) not in seen:
            seen.add((spec[0], spec[1]))
            out.append(spec)
    return out
