# Test suite layout

## What goes in this file

Why the tree is shaped this way, and the traps in writing a test for it that reading the tests
will not tell you. What is recoverable by reading is not in here: which files exist, what each
one covers, what a lane costs, and which markers a directory carries are all in the tree and in
`conftest.py`, and a transcript of one goes stale without this file changing.

## The directory is the declaration

Each top-level directory owns one axis of concern, and `conftest.py` stamps that axis's markers
on everything under it. A file that lands under no axis **fails collection** rather than
quietly running in no gate — that is what makes directory-as-declaration a contract instead of a
convention. An explicit `@pytest.mark.X` is additive, never a replacement; adding `slow` to a
`fast` directory's file does not remove `fast`, so relocating the file or changing
`_DIR_MARKERS` is the only way to move an axis.

Pick the axis by what the test pins, not by what it uses. A test whose assertion is about
*cost* — a duration, an operation count, an O(n) claim — belongs in the perf axis; one that is
about correctness and merely happens to use a large fixture belongs with the behaviour it
pins. A GUI test that needs a docker daemon is an e2e test.

`python_solver` is the one marker that *removes* tests from every routine run, release
included: the rust engine is the shipped solver, so tests that need the python implementation
skip unless it is asked for. `--solver=` is unrelated and selects which implementation
everything else runs on.

## Writing one

Reuse the shared stimuli rather than defining ad-hoc transforms per test, and assert through
telemetry — the lineage trace and its walks — rather than grepping generated Nextflow text or
work-dir filenames. When moving or renaming, update consumers in the same commit; never leave
a stub file behind.

Set `PYTHONPATH` to this worktree's `src/`, and do not merely unset it: metasmith is not
installed into the environment, so the subprocess tests need it, and an ambient value resolves
the import to some other checkout.

## Traps

**The virtual runtime reimplements the cache-hit path from the store and never reads the
generated `workflow.nf`.** Anything about the *emitted text* therefore has to be pinned at
codegen level; a virtual run cannot see it.

**The deploy axis's source-pattern tests read `Agent.Deploy`'s text rather than running it**,
slicing from the method to the next one. `Deploy` is the last method in its module, so that
slice needs an end-of-file fallback or it comes out empty — and every assertion there pins an
*absence*, so it would pass on the empty slice.

**The bootstrap axis reaches no registry.** Some of it asserts the emitted command string and
some of it runs that string against stub binaries on a doctored `PATH`, reading their log —
so an assertion there can be about what the shell *did*, not only about what was generated.

**A lifecycle test that fails partway leaves live processes behind.** Everything it spawns goes
through the `spawner` fixture, which group-kills in teardown whatever the test asserted; a raw
`subprocess.Popen` there hands the next test a process it will find and mistake for its own.

**Prefer the cheapest runtime that can answer the question.** When an e2e test asserts only on
plan shape, channel wiring or DAG correctness, the contract runtime answers it; real execution
exists to verify the engine, not the planner.
