# witness_sweep

The proved plan witness, held against the workflows this project actually runs rather than against
generated problems. Two questions per case: does `msm_solver` still solve it to a plan the witness
accepts, and does the witness reject a broken copy of that plan *by the clause that was broken*.

`src/metasmith/testing/witness_sweep.py` is the harness. This directory is the arms — where the real
workflows are found — plus the duplicated-work reproduction, which is a different question that
happens to need the same machinery.

    python research/metasmith/witness_sweep/run_sweep.py <arm> --out results/<arm>.json

`run_sweep.py --help` lists the arms. `driver` takes any module exposing `NAME` and `build_spec()`,
which is how a workflow on another branch is swept: stage it and its library read-only into a scratch
tree with `git show`, compile that tree's metadata, and point `--driver` at it. Nothing here ever
checks another branch out over the worktree.

`capture_plans.py` is a pytest plugin rather than an arm, because the real-library test corpus builds
its workflows inside session fixtures and has no module-level spec to import. It wraps
`Spec.SolveViews` — the one function every driver, template and test reaches the search through — so
the collection cannot disagree with what the tests actually plan:

    MSM_SWEEP_OUT=out.json pytest tests/metasmith_libraries -m "not slow" -p capture_plans

`duplicate_work.py` answers whether an underspecified workflow runs one tool twice, and what removes
it. It rebuilds `metagenomics_from_paired_reads`'s target list in memory rather than editing the
template, so the library on disk is untouched. **Its `unpinned` arm does not fit in 8 GB** — that is
the finding, not an accident of the box — so start from `--arm minimal`, which reproduces the same
shape in fifteen steps.

`tests/metasmith/solver/test_plan_witness_real.py` pins the eleven shipped templates in the fast
suite. Everything wider lives here, because a hundred-case sweep inside the suite is how a suite
stops being run.

The last full run is `data/metasmith/plans/07-the-witness-over-real-workflows.md`, with the machine
readable rows in `results/sweep.json`.
