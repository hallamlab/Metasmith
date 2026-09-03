# Rust -> LLBC -> Lean 4, for certifying the solver's plan witness.
#
# Charon lowers rustc's MIR to LLBC; Aeneas turns LLBC into pure Lean 4. Lean is
# Aeneas's most mature backend, which is the whole reason this pipeline is the
# one chosen -- hax's primary backend is F* and its Lean support trails.
#
# Every version here is pinned to a commit rather than a branch. The three pins
# are coupled and cannot be bumped independently: aeneas names the charon commit
# it works with in `./charon-pin`, and charon names the rustc nightly it builds
# against in `./rust-toolchain`. Bump aeneas, then read its charon-pin, then read
# that charon's rust-toolchain -- in that order, or the build fails deep in
# rustc-dev with an error that names none of this.
#
# Built as its own image rather than over `joseluisq/rust-linux-darwin-builder`
# (which `src/workflow_solver/dev.sh` uses for the shipping cross build). That
# tag carries an osxcross toolchain and layering a plain rust image over it is
# the 0.18.4 stub-relay bug; the two images have nothing to share anyway.

FROM debian:bookworm-slim

# `opam` comes from apt rather than from opam.ocaml.org/install.sh. The install
# script takes its install directory and its `--version` in an argument order
# that is easy to get wrong, and getting it wrong makes it print its usage and
# exit 2, which reads as a network failure in the build log and is not one.
# Debian's 2.1.x is old but creates a 5.x switch perfectly well.
#
# `libgmp-dev` is zarith's depext, and opam reports a missing depext as
# "Package conflict! No solution found" for the whole switch rather than naming
# the header. zarith is what aeneas's arbitrary-precision arithmetic rests on.
#
# `bubblewrap` is opam's sandbox and opam init fails without it. `libclang-dev`
# is for charon's bindgen. `m4` is an opam build dep that nothing declares.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl git m4 unzip pkg-config \
        libssl-dev libclang-dev bubblewrap rsync opam \
        libgmp-dev libffi-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    OPAMROOT=/opt/opam \
    ELAN_HOME=/opt/elan
ENV PATH=$CARGO_HOME/bin:$ELAN_HOME/bin:/opt/aeneas/bin:$PATH

# No default toolchain on purpose: charon's own `rust-toolchain` file selects
# nightly-2026-08-18 plus rustc-dev/llvm-tools/rust-src, and rustup honours that
# file when it runs inside the charon checkout. Installing a default here would
# only download a second toolchain nobody uses.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --no-modify-path --default-toolchain none

# Lean, via elan. `stable` resolves at build time and is recorded in the image
# by `elan show`; the Lean version only has to be able to compile what Aeneas
# emits, and Aeneas tracks a recent Lean itself.
RUN curl --proto '=https' --tlsv1.2 -sSf \
      https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
      | sh -s -- -y --no-modify-path --default-toolchain stable

# OCaml 5 for Aeneas. Kept in its own layer because the opam switch is the
# slowest step that never changes.
ARG OCAML_VERSION=5.3.0
RUN opam init --disable-sandboxing --bare -y \
    && opam switch create ${OCAML_VERSION} \
    && opam install -y \
        calendar core_unix domainslib easy_logging menhir ocamlformat.0.27.0 \
        ocamlgraph odoc ppx_deriving ppx_deriving_yojson progress unionFind \
        visitors yojson zarith

# The two coupled pins. AENEAS_REF names the charon commit in its charon-pin
# file; CHARON_REF below must equal it, and is repeated here only so a mismatch
# is a loud build failure rather than a silent version skew.
ARG AENEAS_REF=453b09f98f2b593c0544a8ad654b77e2a3bc621a
ARG CHARON_REF=fea3fc68d445181cf4ce094855a43a17192a2b12

RUN git clone https://github.com/AeneasVerif/aeneas /opt/aeneas \
    && cd /opt/aeneas \
    && git checkout ${AENEAS_REF} \
    && pinned="$(grep -v '^#' charon-pin | tr -d '[:space:]')" \
    && if [ "$pinned" != "${CHARON_REF}" ]; then \
         echo "charon-pin is $pinned but CHARON_REF is ${CHARON_REF}" >&2; exit 1; \
       fi

# `make setup-charon` clones charon at the pinned commit and builds it, which is
# what pulls the nightly. Separate layer: this is the step most likely to fail
# on a network hiccup and the most expensive to repeat.
RUN cd /opt/aeneas && eval $(opam env) && make setup-charon

RUN cd /opt/aeneas && eval $(opam env) && make

# `opam env` is not inherited by `docker run`, so bake it in rather than making
# every caller remember to eval it. Charon builds into the checkout rather than
# onto the PATH, so name that directory too.
ENV OPAM_SWITCH_PREFIX=/opt/opam/${OCAML_VERSION}
ENV PATH=$OPAM_SWITCH_PREFIX/bin:/opt/aeneas/charon/bin:$PATH

# Resolve `stable` now, so the toolchain is *in* the image. elan otherwise
# downloads Lean on first use, and containers here run with `--rm`, so that
# download would repeat on every single invocation.
RUN lean --version && lake --version

WORKDIR /root/src
