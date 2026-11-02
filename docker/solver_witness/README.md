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
    ./dev.sh -x src/solver_witness   # extract that crate to Lean, then gate it
    ./dev.sh --lean-check            # build the whole Lean tree and adjudicate it
    ./dev.sh --lean-build MOD        # build one module, for writing a proof
    ./dev.sh -s '<cmd>'              # run a command in the image

Output lands in `$MSM_WITNESS_OUT_DIR`, default `~/.cache/metasmith/witness-lean`. The Lean project
lives in `$MSM_WITNESS_LEAN_HOME`. Both are build products and neither is committed.

## Three pins that move together

    aeneas   453b09f9  ->  charon-pin names the charon commit
    charon   fea3fc68  ->  rust-toolchain names the rustc nightly
    rustc    nightly-2026-08-18

Bump in that order, then copy the nightly into `src/solver_witness/rust-toolchain`. Charon reads
rustc's internals, so the crate being extracted must be compiled by the nightly charon was built
against. The image installs rustup with **no default toolchain** deliberately, so nothing picks one
by accident. `./dev.sh --versions` reads the pins out of the built image rather than out of this
file. Trust that over this paragraph.

Aeneas selects the Lean toolchain too, currently 4.31.0.

## What survives extraction

`charon cargo --preset=aeneas` is required. Without the preset Aeneas refuses the LLBC with a
message naming the missing option.

The current witness extracts with **no holes**: no `sorry`, no axiomatised crate function, no
external axiom. Five Rust shapes do not survive, each found by hitting it, and
`docs/metasmith/solver-spec.md` lists them. A function that hits one comes back as a hole while the
Lean still compiles, which is why `--gate` exists.

**CAUTION** Do not use `-xd`. `-decreases-clauses` produces Lean that cannot compile on this pin, in
three separate ways, and the belief that the proof needed it was wrong — a `partial_fixpoint`
carries its own unfolding equation. `docs/metasmith/solver-spec.md` records the detail so nobody
re-attempts it. The flag stays only to keep that finding reproducible.

## Two gates, and why neither trusts an exit code

`--gate` adjudicates an extraction. Aeneas emits a hole rather than failing, and the Lean still
compiles, so nothing downstream notices that the function a proof is about was never translated.

`--lean-check` adjudicates the proof tree. It checks four things, and every one of them was driven
to failure before being trusted:

1. The build status, captured rather than piped. A pipeline reports `tail`'s status.
2. Any declaration using `sorry`, taken from Lean's own per-declaration warning. A text search
   cannot tell a hole from prose about holes.
3. Any `axiom` added in a hand-written file.
4. `#print axioms` over every audited obligation, which is the only check that reports what a proof
   *depends* on rather than what it contains.

**CAUTION** Every grep over the build log passes `-a`. `lake` writes bytes that make `grep` call the
log binary, and a binary `grep` matches nothing — which in a gate reads as "no holes found".

The fourth check is not theoretical. The Aeneas standard library ships two `sorry`s, in
`core.slice.Slice.get_unchecked` and in that function's spec lemma. The witness does not reach them,
and the axiom audit is what says so.
