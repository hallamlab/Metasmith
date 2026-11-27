# Koonkie deliverable on metasmith 0.26.1: geoquery_from_aoi_table

## Context

metasmith 0.26.0 shipped the `geoquery_from_aoi_table` template, and the fresh-install test passed after one driver fix (journal dfb210a3). The engine/install-loop session is now finishing 0.26.1, which carries the solver fixes plus planning, orchestrator, pool and GUI changes. Before handing Tomer the data and the example driver, confirm that the template still works through the GUI and the Python API on 0.26.1. Use the data already collected and do not rerun the pipeline.

## What you said

> wait for metasmith e2e install test session to ship 0.26.1 with engcyc template and fixes

> confirm data and test usage for tomer under the new GUI + API with parallel solver fixes. want to avoid running the pipeline anymore since we have the data, just want to make sure the interface changes are not broken.

> Then deliverables are the same: Data + example driver + shipped release metasmith version

> do we need to check docs btw? maybe we can update it now?

## Issues

- I1. Between v0.26.0 and rc, the engine code under every driver call changed (plan.py, a new case_merge.py, a rewrite of Orchestrator.groovy, pool.py, the solver search), and no geoquery test covers it.
- I2. The published `mmap_canada_mining` dataset uses the geoquery 0.7 layout (TSV, `_logs/gaps.tsv` with `product`/`stem` columns, no `aois.csv`, no gap descriptions), while the driver Tomer gets writes the CSV layout.
- I3. metasmith.readthedocs.io serves 0.20.4, because it builds from upstream `hallamlab/Metasmith` `release` and PR #68 (fork `release` into upstream, open since 0.22.1) is not merged.
- I4. The readthedocs source has no page for `geoquery_from_aoi_table`, and no page on running a shipped template from Python. The driver relies on that pattern (`clone_stdlib`, `Template.Discover`, `PoolGivens`).
- I5. The Run section of `research/geospatial_query/README.md` on `feat/dev` points at `msm/main/geoquery.py`, `docker/geoquery/dev.sh` and `geoquery.oci`, and none of these exist on `feat/dev` any more.
- I6. `docs/geospatial_query/STACK.md` still lists the deliverable as undecided and R10 (the full rerun) as next.

## High-level goals

- G1. Know that Tomer can install metasmith 0.26.1 and use the geoquery template through both the GUI and the Python driver.
- G2. Give Tomer a dataset that looks the same as what his own runs of the driver produce, without collecting the data again.
- G3. Make the public metasmith docs describe the release Tomer installs, including the geoquery template and how to drive a template from Python.

## Acceptance criteria

- `api.anaconda.org` lists metasmith 0.26.1, and `quay.io/hallamlab/metasmith:0.26.1` pulls anonymously.
- In a clean conda env with 0.26.1 and a fresh HOME:
  - `geoquery_from_aoi_table` is in the shipped templates, and the GUI solves it to 1 + 75 + 1 steps.
  - `geoquery_run.py --plan-only` on the MMAP KML prints a plan with 75 sources and 77 transforms.
  - A smoke run on one AOI with 2 or 3 fast sources publishes the CSV layout. Its values match the same AOI and sources in the published dataset, apart from sources whose data changed upstream.
- `data/geospatial_query/mmap_canada_mining` is in the CSV layout: `aois.csv`, `<family>/<source>.csv`, `integrated/coverage.csv`, `_logs/gaps.csv` with `aoi,source,reason,detail`, `_logs/gaps_descriptions.csv`, `_logs/ledger.csv` and a README from the driver's template. The values in every table equal those in the 0.7 TSVs. The dataset is DVC-pinned and pushed.
- `docs/metasmith/source/workflows/geoquery.rst` exists and is in the standard-library toctree. A Python usage page shows how to run a shipped template. The pages Tomer follows (install, GUI, Python tutorial, workflow generation) agree with 0.26.1 behaviour.
- `sphinx-build -W` on `docs/metasmith/source` passes in `envs/metasmith/docs.yml`.
- After PR #68 merges, metasmith.readthedocs.io/en/latest shows 0.26.1 and the geoquery page.
- The `research/geospatial_query/README.md` Run section describes install from conda and `geoquery_run.py`, with no reference to removed paths.

## Tasks

- T1. Write the readthedocs updates on a docs branch and offer them to 0.26.1 (G3)
- T2. Write the dataset conversion and convert the published dataset (G2)
- T3. Compact (G1, G2)
- T4. Wait for 0.26.1 to ship (G1)
- T5. Verify the template on a fresh 0.26.1 install through the GUI and the API (G1)
- T6. Pin and push the converted dataset, fix the geoquery README, and get readthedocs live (G2, G3)
- T7. Simplify (G2)
- T8. Debrief (G1, G2, G3)

## Approach by task

### T1. Write the readthedocs updates on a docs branch and offer them to 0.26.1

Docs layer: `docs/metasmith/source/` on a branch `feat/geoquery/docs` cut from `feat/engine/rc`, so it carries install-loop's engcyc page and deployment edits. Changes:
- Add `workflows/geoquery.rst` in the shape of `engcyc_metagenomics.rst`: what the template takes (one AOI table, KML or GeoJSON), what it produces, how to pick sources, and how long a run takes. Point at the template description and at `geoquery_run.py` rather than restating adapters. Add it to `workflows/_index.rst`.
- Add a section to `tutorials/python.rst` or `usage/workflow_generation.rst` on running a shipped template from Python: clone the stdlib, discover the template, build givens from its spec, then plan and run. Base it on the driver's `run()` and link to the driver as the worked example.
- Read `setup/install.rst`, `setup/tutorials.rst` (the GUI), `tutorials/python.rst` and `usage/workflow_generation.rst` against the 0.26.1 code paths that changed (plan.py, case_merge.py, pool.py, bootstrap.py). Fix only statements that are now wrong. Check the known conflict with `agentic/setup.rst`, which says `pip install -e .` while install.rst says conda.

Build with `sphinx-build -W` in the docs env. Send install-loop a docs-ready message naming the branch head. It is docs-only, so it does not change the image build hash, and it can merge before they cut 0.26.1. If they have already cut, it lands on `feat/dev` and rides the next release.
Removes: statements in the pages above that no longer hold.
Gotchas:
- install-loop is editing `workflows/_index.rst` and `engcyc_metagenomics.rst` on rc. Branch from rc's head and merge again before handing off.
- The Python section must use only public `metasmith.python_api` names, plus `clone_stdlib`. If `clone_stdlib` is GUI-internal, say so in the message to install-loop instead of documenting a private import.

### T2. Write the dataset conversion and convert the published dataset

Data layer. Write a short script under `research/geospatial_query/` that rebuilds the dataset from the 0.7 tree:
- Convert each TSV to CSV.
- Rename the gap columns from `product, stem, reason, detail` to `source, aoi, reason, detail`.
- Write `aois.csv` from `aois/aois.geojson`.
- Regenerate the README and the ledger.

Reuse the driver's `publish()` text and the `GAP_REASONS` and `NOTES` tables. Either import them from `geoquery_run.py` or factor the README and ledger writer so both callers share it. Do not copy them. Keep `_logs/runs/01cwQadP.tar.gz` as it is. Write to a new folder, diff every table's values against the TSVs, then swap it into place.
Removes: the TSV files and `ledger.tsv` from the dataset (git keeps the old pin).
Gotchas:
- The 0.7 gap `detail` strings are the pre-0.8 messages. Keep them verbatim rather than inventing the new wording.
- Check that the 0.7 gap reasons use the same three values as `GAP_REASONS`.
- The README date and run fields must say 0.7 run 01cwQadP, not today's date.

### T3. Compact

Handoff: the docs branch head and whether install-loop took it, the conversion script path and its output folder, whether the swap happened, and the background watcher for 0.26.1 (`bqqlmd2tb`, which polls anaconda). The todo list carries the task state.

### T4. Wait for 0.26.1 to ship

The watcher already polls anaconda. Also read the install-loop session's release message in `engine/install-loop` for the commit and the image tag. Check that rc merged the released `feat/dev`, so the template and the driver fix `6a91ea81` are both in the package.
Removes: none. This task only waits.
Gotchas: install-loop said it ships only after engcyc run 4 comes back clean, so the wait may run many hours. Do not chase it with messages.

### T5. Verify the template on a fresh 0.26.1 install through the GUI and the API

Consumer layer, same method as the 0.26.0 fresh-install test:
1. Create a clean env `msm_gate_0261` from the hallamlab channel, with a fresh HOME.
2. Remove the local images, then pull them from quay.
3. GUI: start the GUI, open `geoquery_from_aoi_table`, and solve. Drive it through its HTTP backend or the browser tool, and record a screenshot.
4. API: run `geoquery_run.py --plan-only` on the MMAP KML, using the driver from `feat/dev`.
5. Smoke run: one AOI (aoi_6_quebec, already compared on 0.26.0) with 2 or 3 fast, stable sources, about 5 min.
6. Compare the smoke tables to the converted dataset.
7. Clean up the env, home, work and relay.

Removes: none.
Gotchas:
- The geoquery image is `geoquery:0.26.0`. Confirm `geoquery.env` in 0.26.1 still names an image that exists on quay.
- Choose smoke sources that answered in both the 0.7 and 0.26.0 runs.
- Known engine bugs a8a72167 (the probe timeout) and cebb1818 (the pool import) may show up. Note them and do not fix them here.

### T6. Pin and push the converted dataset, fix the geoquery README, and get readthedocs live

1. Run `dvc add` on the converted dataset and commit the pin on `geospatial_query/dev`. Push through the dvc tool.
2. Rewrite the Run section of `research/geospatial_query/README.md` on `feat/dev` and `feat/geoquery/release`:
   1. Install metasmith 0.26.1 from conda.
   2. Run `geoquery_run.py --aois <kml> --out <dir>`.
   3. Use `--plan-only` and `--sources`.

   Point at `--help` and at the new readthedocs page. Move the geoquery.oci CAUTION to a developer note.
3. In STACK, close R10 as "convert, do not rerun".
4. Push the branches.
5. With your approval, merge upstream PR #68 (its title becomes "Release 0.26.1"). Confirm readthedocs rebuilds `latest`.

Removes: the stale Run steps and the `msm/main` invocation.
Gotchas:
- `feat/dev` may have moved because of the 0.26.1 merge-back. Merge before pushing. Never force-push.
- Merging PR #68 publishes to hallamlab. It also carries every fork commit since 0.22.1, including the scrubbed history from 2026-11-22. Check the PR diff for private paths before merging.

### T7. Simplify

Run the `simplify` skill on the conversion script and any `publish()` refactor in the driver.

### T8. Debrief

Run the `debrief` skill. In the journal, name what to send Tomer: the driver link on `feat/dev`, the dataset path and pin, and the version to install (0.26.1).

## Still unsure about

- Do you want me to merge upstream PR #68 once 0.26.1 ships, so readthedocs goes live? Or does a hallamlab maintainer merge it?

## Callouts

- The 0.26.1 wait is open-ended and depends on install-loop's engcyc run 4.
- The converted dataset still carries the 0.7 values. Two sources have changed since (osm_overpass now answers, statcan was transient). The README states the run and image it came from, so this is honest without a rerun.
