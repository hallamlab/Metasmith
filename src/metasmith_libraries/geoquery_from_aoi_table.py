#!/usr/bin/env python3
# Every source is a value under the AOI table, so one fetch_source runs per source
# and aggregate gathers them all. The source list is frozen into the template at
# build time from the adapter package's sources.tsv, which the image also carries:
# rebuild the template when an adapter joins or leaves.
import csv
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec, Template

NAME = "geoquery_from_aoi_table"
DESCRIPTION = """
Geospatial features for a table of areas of interest (KML or GeoJSON polygons)
from 75 public open-data sources: one table per source, the integrated long and
wide tables, an AOI by source coverage matrix, and every gap with its reason.
"""

SOURCES_TSV = A.MLIB.parents[1] / "research" / "geospatial_query" / "geoquery" / "sources.tsv"

TARGETS = [
    "geoquery::aois",
    "geoquery::source_table",
    "geoquery::observations_long",
    "geoquery::features_wide",
    "geoquery::coverage",
    "geoquery::gaps",
]


def sources() -> list[str]:
    with open(SOURCES_TSV) as fh:
        return [r["source"] for r in csv.DictReader(fh, delimiter="\t")]


def build_template(rebuild: bool = False) -> Template:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "geoquery.yml")
        table = lib.AddItem(DEFERRED, "geoquery::aoi_table")
        for s in sources():
            lib.AddValue(f"source_{s}.json", {"adapter": s}, "geoquery::source", parents={table})

    return Template(
        name=NAME, description=DESCRIPTION.strip(),
        cases=[Spec(
            input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
            sample_type="geoquery::aoi_table",
            target_types=TARGETS,
        )],
        transform_libraries=A.transforms("geoquery"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
