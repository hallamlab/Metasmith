Geospatial query
############################################################

The ``geoquery_from_aoi_table`` template collects geospatial features for a table of areas of
interest (AOIs) from 75 public open-data sources. Pick it in the GUI, or load its spec from
``src/metasmith_libraries/templates/geoquery_from_aoi_table/spec.yml``.

- Give one AOI table, a KML or GeoJSON file of named polygons. Every AOI in the table is queried
  against every source.
- Deselect a source to skip it. Each source is one input value under the AOI table, and each runs
  as its own ``fetch_source`` step.
- The run produces one table per source, a long and a wide table of every observation, an AOI by
  source coverage matrix, and a gap table that gives the reason for every missing AOI and source pair.
- A source that needs a login, has moved or times out is recorded as a gap, not a failure. The run
  completes with the sources that answered.

The steps run in the ``quay.io/hallamlab/geoquery`` image, so the agent needs the Docker or
Apptainer runtime. Run time depends on the slowest sources, not on the number of AOIs.

Run it from Python
============================================================

``research/geospatial_query/geoquery_run.py`` drives this template from one command and writes
a dataset folder with one CSV per source, the gaps and a README. Read
:ref:`running-a-shipped-template` for the pattern it follows.

.. code-block:: console
    :caption: Terminal

    $ python geoquery_run.py --aois my_aois.kml --out my_aois --plan-only
    $ python geoquery_run.py --aois my_aois.kml --out my_aois --sources era5_open_meteo,esa_worldcover

Run it with ``--help`` for every option.
