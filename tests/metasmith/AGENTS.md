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

## The group is a second declaration

A file's group says which feature it checks, and a merge check runs the smoke set plus the
groups a change touches. `_groups.py` holds the groups and the smoke set. It imports nothing
from metasmith, so `dev/metasmith.sh` reads it without collecting the suite. A group defaults
to the file's axis directory. `unit/` and `e2e/virtual/` mix features, so `OVERRIDES` names
each of their files. A file in no group **fails collection**, the same as a file in no axis.

The axis and the group answer different questions, so neither derives the other. The axis says
what a test needs and costs: a container, the host's Nextflow, a wall-clock budget. The group
says what it pins. A host-Nextflow grouping test belongs to `flow` but must stay out of the
cheap tier. A `unit/` file is cheap whatever feature it pins.

**CAUTION** A smoke entry is a node id, and a stale one makes `-ts` collect nothing under
xdist rather than fail. `unit/test_marker_taxonomy.py` collects every smoke id to catch a
rename.

## Writing one

Reuse the shared stimuli rather than defining ad-hoc transforms per test, and assert through
telemetry — the lineage trace and its walks — rather than grepping generated Nextflow text or
work-dir filenames. When moving or renaming, update consumers in the same commit; never leave
a stub file behind.

Run the suite through `dev/metasmith.sh -ts`, `-tm` or `-tr`. They pin `PYTHONPATH` to this
worktree's `src/`, which the subprocess tests need because metasmith is not installed into the
environment.

## Traps

**The virtual runtime calls the real key, probe and promote functions but never reads the
generated `workflow.nf`.** Per batch it runs `consumed_of`, `member_key`, `probe` and
`promote_members` from `caching/`, so a cache test there exercises the same decision the task
makes. What it cannot see is the Groovy: `Orchestrator._route`, the `_cached` twin, publish and
the trace TSV run only under real Nextflow. Anything about the *emitted text* has to be pinned at
codegen level. Grouping and release order run in `e2e/nextflow` on the host's Nextflow, with no
image. Its fixtures are hand-written in codegen's shape, so a codegen change that alters that
shape needs the fixtures changed with it.

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

**The docker lane execs into one container per xdist worker rather than starting one per run.**
Creating a container on altair's daemon takes 11-17 s, and an exec takes a fraction of a second.
The container mounts the worker's pytest tmp tree at the same path, so a path means the same
thing on both sides, and no test may assume a fixed in-container path such as `/ws`. Its
process runs as the host user, so its outputs are deletable without a chown. Killing the exec
client leaves Nextflow running inside, so `nxf_exec` wraps the command in an in-container
`timeout`.

Share a run between tests with a module- or class-scoped fixture, and put an
`xdist_group` mark on the module so `loadgroup` keeps the run on one worker.

**The image under test is built from the checkout.** `docker_builder` stamps the build hash,
builds from a staged copy under a file lock, and labels the image with whether its relays are
real or stubs. A stub-relay image is rebuilt once a real relay exists. The root conftest stamps
the hash before importing `metasmith.constants`, because `CONTAINER_TAG` is fixed at import.

**Prefer the cheapest runtime that can answer the question.** When an e2e test asserts only on
plan shape, channel wiring or DAG correctness, the contract runtime answers it; real execution
exists to verify the engine, not the planner.
