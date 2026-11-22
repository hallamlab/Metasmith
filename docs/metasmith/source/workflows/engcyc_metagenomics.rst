Engcyc metagenomics
############################################################

The ``engcyc_metagenomics`` template runs the Engcyc metagenomics workflow on
one or more read sets. Pick it in the GUI, or load its spec from
``src/metasmith_libraries/templates/engcyc_metagenomics/spec.yml``.

The template description lists every lane, its tools and the size of each lane's reference
download. Read it in the GUI's template list, or in ``DESCRIPTION`` in
``src/metasmith_libraries/engcyc_metagenomics.py``.

- Deselect a target to drop its lane. A lane's tools run only for the targets that read it.
- Give a sample long reads beside its short reads to assemble it with the hybrid lane.
  A hybrid sample does not yet solve with every target selected.
- Give a licensed package and add its target to turn on a licensed lane. The description
  names each one.
- Run ``engcyc_prep_references`` first, once per agent, to fetch every reference database
  ahead of the real run. It takes no samples, and its download steps are the ones
  ``engcyc_metagenomics`` plans, so the real run finds them in the cache.
