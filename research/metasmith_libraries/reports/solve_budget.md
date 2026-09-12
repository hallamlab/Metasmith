# Where the solve budget goes, on the post-fabfos library

Measured on the merged tree (`libraries/types` = `feat/monorepo` T7 + fabfos/dev
T3(a/1..5)), against a **5 second budget**: a plan a user is waiting on should
not cost more than that, so a solve that trips the alarm is a defect and not a
datum. Reproduce anything below with
`research/metasmith_libraries/probe_solve_budget.py` and
`research/metasmith_libraries/probe_ambiguity.py`.

Numbers are one host, one run each, wall clock around `Spec.Solve()` only.
Library load is reported separately and is ~0.5–1.0s throughout.

## The headline

**The compiled type surface is not the problem, and cutting it will not buy
anything.** The cost is *producer ambiguity*: one requirement with two reachable
producers, sitting above 20 of a template's 22 targets. Everything below is
the evidence for that, plus three other things found on the way that will bite
the monorepo integration independently.

## 1. The shipped solver engine cannot run in this repo

> **Fixed.** `src/metasmith/engine.dvc` is gone, each scope builds its own stage
> (sharing one cargo target dir via `MSM_SOLVER_TARGET_DIR`), the packaging guard
> checks the exec bit, and `-ud`/`-bs` ask the installed package inside the tagged
> image which backend it will use. Every measurement below was re-taken against
> the natively staged engine with no `--engine-dir` override and reproduced:
> n17 6.27s against 6.17s, n22 without spades 2.59s against 2.56s.

`src/metasmith/engine/` was DVC-tracked (`engine.dvc`, landed by *"DVC-track the
solver engine binaries"*). DVC materialises its outputs as read-only hardlinks
into the shared cache and **does not preserve the executable bit**, so all four
binaries check out `-r--r--r--`. `probe_engine` gets `Errno 13` at the handshake
and every solve in the repo silently reverts to the Python search.

    solver engine at [.../msm_solver.x86_64-linux] could not be run:
      [Errno 13] Permission denied
    solving with the python implementation because ... roughly 15x slower

`chmod +x` on a copy is all it takes — the binary is otherwise fine and
advertises `wire 2 / rng 1 / solve`. What makes this worth fixing rather than
noting: `setup.py` ships `engine/**` as package data and the docker image is
built from that sdist. Checked directly, with a 444 package-data file: the sdist
carries it through as 444 and the wheel normalises it to 644 — **neither is
executable**, so the fallback follows the artifact into the image, the conda
package, and every agent deployed from one. The mode is set once, in
`src/workflow_solver/dev.sh` at stage time, and a DVC checkout is now downstream
of it.

It also costs the budget outright. On the Python fallback the metagenomics
template misses its budget even with the ambiguity below removed:

| metagenomics | rust | python |
|---|---|---|
| 13 targets | 1.62s | 3.60s |
| 17 targets | 6.17s | >30s (killed) |
| 22 targets, spades/megahit fork removed | **2.56s** | **15.60s** |

`tests/metasmith/unit/test_solver_engine_packaging.py` opens with *"The three
ways the solver binary silently fails to ship"* and pins all three. This is a
fourth, and nothing in that file asserts the file is executable.

## 2. Producer ambiguity is what spends the budget

One file did this. Same template driver (`metagenomics_from_paired_reads.py` is
byte-identical across the merge), same 22 targets, same engine — the only
difference is the libraries:

| metagenomics, 22 targets, rust | before (`ddb203b`) | after fabfos T3(a) |
|---|---|---|
| solve | **2.88s** | **219.3s** (76x) |
| at 17 targets | 1.28s | 6.17s |
| library load | 0.66s | 0.93s |

It does still solve, and to the same shape (35 steps against 29) — this is a
budget failure, not a correctness one, which is why `build_templates.py` stayed
green through it.

`probe_ambiguity.py` over the two trees names the change exactly:
`sequences::assembly` went from 6 producers to 7, and the seventh is
`assembly/spades.py`.

`metagenomics_from_paired_reads` has 22 targets. On the Rust engine, after:

| targets | 10 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 22 |
|---|---|---|---|---|---|---|---|---|---|
| solve (s) | 1.53 | 1.70 | 1.62 | 3.72 | 2.46 | 2.75 | **6.17** | **31.2** | **219.3** |

The budget is gone by target 17 and the curve is exponential after it. The
template ships 22.

Masking a single transform puts it back:

| 22 targets | solve | verdict |
|---|---|---|
| as shipped | 219.3s | fail |
| `--exclude spades` | 2.56s | pass |
| `--exclude megahit` | 2.84s | pass |

Either one alone is fine; the pair is the bomb. `sequences::megahit_assembly`
and `sequences::spades_assembly` both `extends: assembly` and add only
`method: <name>`, so both satisfy every plain `sequences::assembly` demand — and
`probe_ambiguity.py` counts **13 transforms demanding `sequences::assembly`**
(prodigal, metabuli, assembly_stats, all three binners, aggregator, skani_dedup,
and four annotators). 20 of the template's 22 targets are downstream of that
fork — everything except `read_qc_stats` and `phyloflash_summary`, which come
off the reads — none of them pins which assembler, and the search has to carry
both choices through all of them.

With the fork removed the curve is flat, which is what says the target count was
never the problem either:

| targets, `--exclude spades` | 14 | 17 | 18 | 20 | 22 |
|---|---|---|---|---|---|
| solve (s) | 1.42 | 1.59 | 1.62 | 2.36 | 2.56 |

`spades.py` is new to this library set and cannot simply go:
`transforms/fabfos/resolve_inserts.py` requires `sequences::spades_assembly`
explicitly, alongside megahit's, and runs both per pool deliberately.

**The library-side shape of a fix already has an idiom here.**
`sequences::orfs` carries `granularity: whole` purely so `orf_chunk` stops being
a property-superset of it; `contig_batch` says the same thing and its comment
spells out the intent — *a distinct sibling, NOT a subtype, so the solver routes
batches only to batch-consuming tools*. Applying that to the assemblers would
make `spades_assembly` a sibling of `assembly` rather than a subtype, leaving
megahit the sole producer of plain `assembly` and leaving `resolve_inserts`
(which names `spades_assembly` outright) unaffected. What it costs is the
ability to plan a downstream metagenomics workflow *from* a SPAdes assembly,
which is a product decision and not a mechanical one.

The alternative that costs nothing in the library is pinning the assembler in
the template with `parents=`, the way the template already pins each binner for
its checkm/gtdbtk targets. That fixes the four shipped templates and leaves the
next author to rediscover this.

## 3. The type surface, measured, and it is not it

Landing fabfos took `transforms/metagenomics` from 369 to 522 compiled types
(+41%) and from 17 namespaces to 27 — ten of them (`buildlib` 27, `fabfos_data`
24, `raw` 22, `interm` 13, `fabfos` 12, `lookup` 6, `bench` 6, `ecspr` 5,
`evidence` 1, `algorithm` 1) belonging to a lane that library cannot reach.

Deleting all ten from all four libraries in the template's set (522 → 405 types,
−22%) changes:

| | as shipped | fabfos namespaces stripped |
|---|---|---|
| 17 targets | 6.17s | 5.94s |
| 22 targets | 219.3s | >30s, killed at the budget |
| library load | 0.93s | 0.76s |

So the surface is worth ~0.17s of load and nothing measurable in the search, and
stripping it does not rescue a single failing solve. Combined with the mask
(`--exclude spades --lib-root <stripped>`) the 22-target solve is 2.41s against
2.56s for the mask alone — the same 0.15s, and all of it load.

Trimming the surface is still defensible as hygiene: it is 117 types per library
that cannot participate in a plan, and it costs the load path. It is not a
performance fix, and this scope's founding premise — that the +40% type surface
is what took the solve curve exponential — does not survive measurement. The
confound named in the scope brief (spades landing in the same merge) turns out
to be the whole effect.

## 4. The fabfos annotation lane: 10s of hashing, 1s of solving

The lane a user actually runs (`fabfos.pipelines.annotation`, target
`annotation::gpr_table`, 9 steps) costs ~11s end to end before any work starts,
and the solve is not where it goes:

| stage | seconds |
|---|---|
| `build_inputs` (references present locally) | **10.0** |
| `build_inputs` (`verify_refs=False`, paths verbatim) | 0.12 |
| solve (rust) | 1.00 |
| solve (python) | 1.00 |

A leaf's `instance_id` is `blake3(file_bytes) ‖ relpath` when the file is present
at `AddItem` time, and this lane registers ~24 GB of reference databases
(uniref50 17 GB, kofam profiles 7.2 GB) on **every** plan. That is the price of
cross-run cache reentrancy, and it is being paid per-plan for inputs that never
change. The solve itself is a second, on either implementation — the engine does
not matter for this lane and never did.

Note the lane's solve is fine because it has almost no ambiguity: over
`functionalAnnotation + fabfos + logistics`, `probe_ambiguity.py` finds **1**
ambiguous requirement against the metagenomics set's 8.

## 5. Nothing anywhere enforces a budget

`build_templates.py` is the guardrail, and it asserts only that a template
*solves* — it waits however long that takes and reports success. All four still
solve, so it still passes; it just takes 3.7 minutes longer on the Rust engine
and considerably more than that on the Python fallback the repo currently runs.
That is how a 76x regression lands green. The perf suite declines wall-clock
assertions on purpose (*"timing thresholds on a developer laptop are a flake
generator"*) and settles performance claims against a recorded baseline instead,
which nothing runs on a merge.

A budget does not need to be a tight threshold to catch this class of defect:
the failures here are 5s against 30s+, not 5s against 6s.

## Where to fix, when the integration lands

1. ~~**Engine exec bit.**~~ Done — see the note in §1. The pin is removed, the
   guard checks the mode, and `test_solver_engine_packaging.py` now fails when
   nothing is staged or the staged binary cannot run.
2. **The assembler fork**, in `data_types/sequences.yml` — sibling-vs-subtype for
   `spades_assembly`, following the `contig_batch` / `orf_chunk` idiom — or
   per-target `parents=` pinning in the templates. First is a product decision;
   second is local and cheap.
3. ~~**Reference hashing**~~ Done. `fabfos.refs` builds a frozen
   `DataInstanceLibrary` whose ids come from each chunk's DVC md5 rather than
   from re-reading the bytes; `build_inputs` loads it instead of staging the
   references. 10.0s -> 0.23s, and the annotation task key is now stable across
   plans, which it was not before (the two directory references were minting a
   fresh uuid4 per build).
4. **A budget gate** on `build_templates.py`, so the next fork of this kind fails
   the build instead of being found by a user.
5. **Type-surface trimming** (`metasmith build`'s compilation scope) stays worth
   doing for load time and hygiene, at roughly 0.17s per solve — not as the fix
   for anything measured here.
