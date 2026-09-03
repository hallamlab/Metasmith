# Extracting the plan witness to Lean 4

## Purpose & Contents

How the Rust witness becomes Lean, and which Rust shapes survive the trip. Holds
the toolchain pins and the extraction failures that are worth knowing before
writing code intended to be extracted.

The specification itself is `docs/metasmith/solver-spec.md`. The checker is
`src/solver_witness/`.

## Usage

    ./dev.sh -b                      # build the image (slow: ~45 min cold)
    ./dev.sh --versions              # report the pins actually inside it
    ./dev.sh -x src/solver_witness   # extract that crate to Lean
    ./dev.sh -s '<cmd>'              # run a command in the image

Output lands in `$MSM_WITNESS_OUT_DIR`, default
`~/.cache/metasmith/witness-lean`. It is a build product and is not committed.

## Three pins that move together

    aeneas   453b09f9  ->  charon-pin names the charon commit
    charon   fea3fc68  ->  rust-toolchain names the rustc nightly
    rustc    nightly-2026-08-18

Bump in that order, then copy the nightly into
`src/solver_witness/rust-toolchain`. Charon reads rustc's internals, so the
crate being extracted must be compiled by the nightly charon was built against.
The image installs rustup with **no default toolchain** deliberately, so nothing
picks one by accident — the crate's own `rust-toolchain` is what selects it.

`./dev.sh --versions` reads the pins out of the built image rather than out of
this file. Trust that over this paragraph.

## What survives extraction, and what does not

`charon cargo --preset=aeneas` is required. Without the preset Aeneas refuses
the LLBC with a message naming the missing option, which is the one easy error
here.

Measured on the current witness: **35 of 37 functions translate.** `check`
itself comes out clean, in the `Result` monad — so `check_sound` must be stated
over a monadic value rather than a bare `Bool`.

Three shapes do not survive. All three are avoidable, and knowing them is the
point of this file.

- **A `&mut` collection mutated inside a *nested* loop.** Both hard failures are
  `v.violations.push(...)` under two levels of `while`, and Aeneas reports
  `Could not match the contexts` — its loop fixed point cannot unify the borrow
  context across the two depths. One loop is fine; two is not.
- **`&'static str`.** `Clause::name` returns a string literal and fails for that
  reason alone. Strings have no model here.
- **Anything the above two reach.** A function whose body fails becomes `sorry`,
  and functions it calls are emitted as `axiom`. That is quiet: the file still
  compiles, and a proof built on it would prove nothing. **Grep the output for
  `axiom` and `sorry` before believing an extraction succeeded.**

`-loops-to-rec` does not help with the nested-loop failure. It was tried.

## The consequence for how the witness is written

Judgement and reporting have to be separate functions. The predicate the proof
is about should return a plain `bool` and mutate nothing; collecting *which*
clause failed, with positions, belongs in a wrapper that is never extracted.
The current `src/lib.rs` interleaves them, which is why two of its functions are
`sorry` — that split is the outstanding work before `check_sound` can be stated
against a real body rather than an axiom.
