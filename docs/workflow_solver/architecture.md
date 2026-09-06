# workflow_solver — architecture

`msm_solver`, the Rust half of the metasmith plan solver. Rust source under
`src/workflow_solver/`; the built binaries are staged into `src/metasmith/engine/`.

## What goes in this file

How the Rust solver ships, what happens when it cannot, and the two contracts it still shares
with the Python half that drives it — the wire envelope and the RNG stream. The code is the
description of the search; what belongs here is what one file cannot see about another.

## How it ships, and why that differs from the relay

Unlike `msm_relay`, this binary runs **locally**, in whatever process is planning — CLI, GUI or
notebook. So it is **not in the docker image**: `dev/metasmith.sh -be` cross-builds the four
targets and stages them into the engine directory, from which they ship as package data in the
wheel and sdist.

**That staging directory is the only place a binary is looked for, in all three contexts.**
`PYTHONPATH=src` makes it the package's own `engine/`, which is where an installed wheel
resolves too — so nothing is added to `PATH` and there is one lookup rather than three. Run
`-bel` once and source runs use the engine.

**Absence is now a hard failure.** There is one solver, so a missing, unreadable or
version-mismatched binary raises an `EngineError` naming which of the three it was, rather than
falling back. That is a deliberate trade: the fallback was correct and roughly an order of
magnitude slower (1.1s against 7.5s on `metagenomics_from_paired_reads`), which made a checkout
that forgot to stage a binary *look* fine — and once, when DVC checked the binaries out mode 444,
every plan in every worktree took that path with nothing but a warning to say so. The guard on
every shipping build (`_assert_solver_engine`) is therefore load-bearing rather than advisory. It
also refuses a host-built `-bel` binary via a `BUILD_KIND` marker, since nothing about a Linux ELF
says musl versus the build machine's glibc.

The cost of the trade is that a host outside the four staged targets — x86_64 and arm64 crossed
with linux and darwin — cannot plan at all.

`solver_backend.py` holds the entrypoint and the refiner budget; `solver_engine.py` answers *is
this binary trustworthy* — where it lives, what it said about itself, whether its versions match.

## Two version constants, because they move for different reasons

`WIRE_VERSION` covers the envelope — field names, framing, request and reply shape.
`SOLVER_RNG_VERSION` covers the decision contract. Both are carried from the first commit rather
than added at the first break, and the scar tissue is `LIN_PAYLOAD_VERSION`: a *single* constant
covering two things that can move independently is how the last desync went unnoticed. The
binary also advertises `capabilities`; a capability it does not claim is simply unavailable.

`SOLVER_RNG_VERSION` is 3. It went to 3 when PUCT became the selection rule — the envelope did
not move, and that is exactly the independence the two constants exist to express.

## Plan-fingerprint parity has been re-established

Fingerprints pin the decision contract, so any search change re-pins them. They were held still
through the refiner repair and the PUCT adoption, because chasing parity between the two would have
re-pinned twice and confounded them. PUCT has now landed as the only rule, and
`tests/metasmith/solver/fingerprints.json` was re-recorded against it with `SOLVER_RNG_VERSION`
moving in the same commit. The suspension is over: a search change re-pins again.

What the fingerprints can and cannot do is worth stating, because the corpus they cover solves in
7–13 iterations against a budget of 256. They are a tripwire against a search that moves *quietly*.
They are not evidence about a search change, and a green re-pin says nothing about whether the
change was good. That question is `research/metasmith/solver_ratchet/`.

The soundness criterion is separate and did not move: a plan is judged by `solver_witness`, which is
proved to decide exactly the written specification. A search that returns a different plan is not a
regression. A search that returns a plan the witness rejects is.

**The gate runs `check`, not `audit`.** `solver_witness::check` is the function
`SolverProof.check_spec` is about. `solver_witness_audit::audit` re-implements the same judgement
as loops that can name a coordinate, and the two are tied only by a `debug_assert_eq!` that
`[profile.release]` compiles out — so a release binary that gated on the audit was gated by the
half nothing is proved about. `cmd_solve` and `cmd_check` both take their verdict from `check` and
call `audit` only to say which clause failed.

**CAUTION** The witness is not a complete oracle on its own. `msm_solver solve` runs the same check
before it emits, so an accepted plan is weak evidence. Two independent halves carry the guarantee:
`check_spec` agreeing on the same bytes, and the decoys. Two known holes let it accept what the
specification rejects. `same_slots` and `same_props` compare dense bit sets of a fixed width, so an
id at or beyond that width is invisible. `check_plan` records "equal to, not identical with" as a
note rather than a violation, which hides two steps producing signature-equal endpoints that
`rectify` then collapses onto one producer.

## The RNG stream is the one contract still written twice

`rng.rs` and `src/metasmith/models/solver_rng.py` are the two halves, and `rng.rs` is a
deliberately literal transcription — same rules, same order of operations, same shortcuts.
**Where a line looks like it could be simplified, the simplification is what would make the two
streams diverge.**

The python half outlived the python solver on purpose. It is no longer a second implementation of
anything; it is an executable statement of the stream that `test_rng_contract.py` drives the
binary against through `rng-trace`, which is the only cross-language differential left and the
only thing that would catch `rand_chacha` changing under us.

**Selection no longer samples.** PUCT ranks, `top_k` is 1, and a one-element choice consumes
nothing — so the solve seed does not reach the plan on any of 72 corpus and profile cases. The
stream is still drawn on ties and still has to agree, which is why the contract stays; but a test
that hoped to notice a PRNG swap by watching a plan move would now notice nothing.
`test_corpus_pin.py` pins the inertness instead, so putting sampling back is a decision rather
than a discovery.

The stream is ChaCha8 in its reference form: key = the seed as eight little-endian bytes followed
by 24 zero bytes, 96-bit zero nonce, block counter from 0, the sixteen words of each block
consumed in order. `ChaCha8Rng::from_seed` accepts exactly that; **`seed_from_u64` does not** —
it runs the seed through PCG first — and must never appear. The trait comes through
`rand_chacha`'s own re-export rather than a separate `rand_core` dependency, because two paths to
the trait means two resolvable versions of it. `rand_chacha` is used rather than `rand::StdRng`
precisely because its value stability is a documented guarantee of the crate.

`Log2` is probed as its own wire op because it is the one function both implementations delegate
to a C library rather than defining, so a musl-versus-glibc divergence surfaces as a failing test
naming the input rather than as a plan that is subtly wrong on one platform. `MSM_SOLVER_TRACE=1`
is read by the binary itself and narrates its decisions and frontier on stderr, which is how a
differential failure is localised to a single draw.

## Floats are load-bearing on the wire

`serde_json`'s default float parsing is documented as *best effort*: about twice as fast, and not
always the double the writer meant. It read `0.9999999999999999` as exactly `1.0`, and other
ordinary values a few ulps off. These numbers become scores, scores are compared, and comparisons
decide plans — so the **`float_roundtrip` feature is not optional here**, and `wire::tests` pins
it so dropping it fails `cargo test` rather than the corpus.

Non-finite floats are the one place the format is not plain JSON. `NaN` and `Infinity` are not
JSON, Python's `json` emits them anyway, and `serde_json` rejects them — and since NaN *ranking*
is part of the decision contract, the wire cannot quietly not support NaN. A score is therefore
either a JSON number or one of a fixed set of names, on both sides.

