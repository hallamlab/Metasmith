# workflow_solver — architecture

`msm_solver`, the Rust half of the metasmith plan solver. Rust source under
`src/workflow_solver/`; the built binaries are staged into `src/metasmith/engine/`.

## What goes in this file

How the Rust solver ships, and the contracts it shares with the Python implementation — the
version constants, the wire envelope, the decision rules that must agree bit for bit. The code
itself is the description of the search; what belongs here is what one side cannot see about
the other.

## How it ships, and why that differs from the relay

Unlike `msm_relay`, this binary runs **locally**, in whatever process is planning — CLI, GUI or
notebook. So it is **not in the docker image**: `dev/metasmith.sh -be` cross-builds the four
targets and stages them into the engine directory, from which they ship as package data in the
wheel and sdist.

**That staging directory is the only place a binary is looked for, in all three contexts.**
`PYTHONPATH=src` makes it the package's own `engine/`, which is where an installed wheel
resolves too — so nothing is added to `PATH` and there is one lookup rather than three. Run
`-bel` once and source runs use the engine.

**Presence means use it; absence means the Python solver runs instead.** That is recoverable but
not free: a wheel with no engine plans correctly and slowly, and nothing fails. Hence the guard
on every shipping build (`_assert_solver_engine`), which also refuses a host-built `-bel` binary
via a `BUILD_KIND` marker, since nothing about a Linux ELF says musl versus the build machine's
glibc.

## The engine is the default, and reversion is explicit

The Rust engine runs whenever a usable binary is staged, with nothing set and nothing passed.
The Python solver is a **reversion**, reachable only by saying so — `_set_solver_class`,
`UsePythonSolver()`, or `pytest --solver=python` — and never by an environment variable. An
*unasked-for* fallback warns once per process, because the two produce the same plans and differ
by roughly an order of magnitude in wall clock (1.1s against 7.5s on
`metagenomics_from_paired_reads`), so a checkout that forgot to stage a binary is correct and
slow, which is exactly the defect nobody notices.

Selection is an object rather than an environment read because the callers sit on core execution
paths: a planning call must be able to state which implementation it wants and put the previous
choice back afterwards. A differential test needs a reference, and a test whose subject is the
Python implementation stops testing anything the moment the search runs elsewhere. An environment
variable cannot be scoped, cannot be restored, and is invisible at the call site.

`solver_backend.py` answers *which implementation runs* and is the only one of the pair anything
outside the solver should import; `solver_engine.py` separately answers *is this binary
trustworthy* — where it lives, what it said about itself, whether its versions match.

## Two version constants, because they move for different reasons

`WIRE_VERSION` covers the envelope — field names, framing, request and reply shape.
`SOLVER_RNG_VERSION` covers the decision contract. Both are carried from the first commit rather
than added at the first break, and the scar tissue is `LIN_PAYLOAD_VERSION`: a *single* constant
covering two things that can move independently is how the last desync went unnoticed. The
binary also advertises `capabilities`, and the Python side falls back for anything not
advertised, so the port could land one piece at a time with the delivery path proven first.

## Plan-fingerprint parity is suspended, and the witness replaces it

Plan fingerprints normally pin the decision contract, so any search change re-pins them. The refiner
repair and the PUCT adoption both move every plan. Chasing parity between them re-pins twice and
confounds the two, so fingerprints are held still until PUCT lands. Re-establish them once, then.

In the interval a plan is judged by `solver_witness`, which is proved to decide exactly the written
specification. A refiner that returns a different plan is not a regression. A refiner that returns a
plan the witness rejects is.

**CAUTION** This does not suspend `SOLVER_RNG_VERSION`. That constant does a different job. It stops
a stale binary and a new Python planning differently while both advertise the same version. Bump it
whenever the decision contract moves, parity suspended or not.

**CAUTION** The witness is not a complete oracle on its own. `msm_solver solve` runs the same audit
before it emits, so an accepted plan is weak evidence. Two independent halves carry the guarantee:
`check_spec` agreeing on the same bytes, and the decoys. Two known holes let it accept what the
specification rejects. `same_slots` and `same_props` compare dense bit sets of a fixed width, so an
id at or beyond that width is invisible. `check_plan` records "equal to, not identical with" as a
note rather than a violation, which hides two steps producing signature-equal endpoints that
`rectify` then collapses onto one producer.

## The decision contract is one specification written twice

`rng.rs` and `src/metasmith/models/solver_rng.py` are the two halves, and `rng.rs` is a
deliberately literal transcription — same rules, same order of operations, same shortcuts.
**Where a line looks like it could be simplified, the simplification is what would make the two
streams diverge.**

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

