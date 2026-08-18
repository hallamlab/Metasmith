# The shared task index and the two silent truncations (2026-08)

Status: **closed.** The invariants it established are in `architecture.md`; the
things it found and did not fix are in `consolidation-followups.md`. This file
is the record of the mechanism and the measurements, which neither of those has
room for and which the tests cite by shape rather than by name.

Two scopes reported workflows that quietly did less work than they claimed —
`fabfos/bench-scales` (a nine-step benchmark that ran seven, submitted every one
at exit 0, and logged `Session await > all processes finished`) and
`lung-microbiome/run1` (two of 34 libraries with no megahit task at all, no work
dir, no failure, no trace row; and every restart a near-full re-run). They are
unrelated causes with overlapping symptoms.

Shipped on `feat/dev` as ten commits: T1 `d696742`+`bcac1be`, T2 `39e606d`+`4b7e1ea`,
T3 `1519555`+`5d679c3`, T4 `d7f44db`+`0fc55ac`, T5 `9a438a3`, T6 `471e786`.
Each bug-fixing task is a red commit carrying the test and its observed pre-fix
failure, then a green commit carrying the fix.

## The race

A process declaring N output tuples binds the **same** index map object to all N
of its output channels — identical `identityHashCode`, verified under real
Nextflow. Each output channel is a separate dataflow operator on its own thread.
`_debatch` deleted the two reserved bookkeeping keys from that map in place, once
per output stream, so N threads wrote one unsynchronised `LinkedHashMap`.

That does not fail loudly. It yields an **emptied** copy: the product reaches the
next `o.group` carrying nothing about where it came from, the join drops it, and
the branch of the workflow below it ceases to exist.

**Rate, measured on the shipped code path: ~0.07% of tasks at two outputs,
roughly ten times that at three.** Every corrupted copy lost its ancestry
entirely rather than partially, which is the shape both field reports described.
That rate is why it read as unreproducible — six runs on one cluster ran the
identical two-output step and only one lost it — and why the regression pin is a
20,000-round probe rather than a workflow: over the shipped strip it lost 26
lineage keys in 40,000 at two streams and 60 in 60,000 at three before the
change, and zero in both after.

**Neither reporter's inferred mechanism was right, and one of them refuted
themselves.** `bench-scales` suspected the grid-executor output-binding path for
a two-output process, and something specific to proteinbert's manifest shape;
it is neither — it is any of the 77 multi-product transforms in the shipped
library (228 declare products; exactly one of the 77, `logistics/dumpNcbiSra.py`,
puts them in separate groups). Their own "esm_c returns three and is fine" was
evidence against proteinbert being special, since three outputs is the
higher-risk case. `lung-microbiome` had the empty-list variant exactly right as a
hypothesis and could not recover the index that would have confirmed it.

**The sharing cannot be fixed from here** — it is Nextflow's output binding,
decided before any code in this repo runs. Corruption needs a shared object *and*
a writer; T1 removed the writer.

## Why nothing caught it

Every orchestrator e2e test declared exactly one output tuple, and the one
multi-product stimulus in the tree (`multi_slot_producer`) calls
`NewProductGroup` between slots — so the emitter wrote one *optional* output
tuple per branch and the call returned a single Channel. The multi-output
`ChannelOut` → `asStreams` → `_post` → `_debatch` path had no coverage at all,
and that is the shape 76 of the library's 77 multi-product transforms use.

## What each defect needed to be seen

Nine repro arms, one per way the defect can be observed. Listed because the
count is the argument: four defects, and the naive one-test-per-fix reading
misses four of these.

- **F1a/F1b — the race.** F1a asserts `stripReserved` does not mutate its
  argument: three lines, no threads, the whole defect as one property. F1b is the
  20,000-round probe. Pre-fix F1a lost both reserved keys from the caller's map.
- **F2a/F2b — the consumer guard.** Absent by-key and *empty-list* by-key need
  separate tests: a guard written only against null passes F2a and still misses
  the reported defect, because the loop over an empty list runs zero times and
  leaves no dispatch-log row at all. Pre-fix both exited 0 with zero items
  downstream.
- **F3a/F3b — the warm path.** A shard whose recorded index is present but empty
  renders to `[:]`, gets stamped with its own key, and reproduces the identical
  one-key symptom on a cache hit. F3a stops such a shard being trusted, F3b stops
  it being created. This is why T3 lands *after* T2: before the guard an empty
  replayed index was a silent drop, after it the run aborts, so demoting the
  shard is what keeps a warm run from failing on work it should recompute.
- **F4a/F4b/F4c — `scratch`.** F4a asserts the metadata file reaches the work
  directory; F4b asserts `scratch` off is unchanged and silent, which is the only
  way the fix can break anything (a self-copy error); F4c is the two-run cache
  test and the arm that matters long-term, because `$0` is absolute only by the
  launcher's convention and a future Nextflow could silently revert F4a's
  mechanism without breaking it.

## Deliberately not done

- **No close-time "this stream contributed nothing" assertion**, which
  `bench-scales` asked for. A stream that delivers no items is the legitimate
  optional-branch and all-ignored case; asserting there fails runs that are
  behaving correctly. The per-item guard catches the reported case strictly
  earlier.
- **`SIBLING` still only logs.** Its join key is an arbitrary member of an
  ancestor-set intersection, so an item can legitimately relate through a
  different one.
- **No deep copy of index values.** See `consolidation-followups.md`; it buys
  nothing against a live defect and would change the rendered index, which feeds
  `file_instance_id` and would orphan every existing cache shard.

## No run produced a wrong output

Every dispatch path was traced for what an emptied index does: `DESCENDANT_OF_BY`
dropped it, `SIBLING` dropped it, `PARENT_OF_BY`'s null join key never matched so
it dropped it, and `WILDCARD` has no by-key so it was unaffected. On the protocol
side `SourceOf` returns `None` or raises on ambiguity rather than guessing. The
failure mode is always work not done, never wrong work done — so the outputs both
reporting runs did produce are sound, and the nine-step replay of the failing
`workflow.nf` yields byte-identical published filenames before and after, which
is the check that keeps existing shards addressable.
