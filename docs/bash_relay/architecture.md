# bash_relay — architecture

`msm_relay`, the small Rust binary metasmith drives a remote host through. Rust source under
`src/bash_relay/`; the built binaries are **baked into the agent container image**, which is what
distinguishes it from `workflow_solver` — that one runs locally and ships as package data.

## What it is for

Everything metasmith does on an agent goes through one long-lived bash subprocess and returns
when a marker line comes back. The relay owns that: it forks a watcher, holds a workspace, runs
commands under a remote shell, and reports status back through files in an I/O directory rather
than over the connection. That is why the calling side bounds its wait on **silence rather than
elapsed time**, and why the shell has to be a pty — details on the client half are in
`docs/metasmith/architecture.md`.

`DeployFromContainer` extracts the binary out of the image and verifies its **magic bytes and
size** before copying it, so a stub or corrupted relay fails with a precise error instead of a
bare missing-file assertion several steps later.

## Cross-compilation, and the stub-relay bug

Four targets: `x86_64`/`aarch64` × `linux-musl`/`apple-darwin`. `dev/metasmith.sh -br` builds
them; `-brc` fetches the build image.

**The build image is upstream and must be pulled, never built over.** It carries the osxcross
toolchain the two darwin targets link against. Building the local `Dockerfile` over that tag
replaces it with a plain rust image, whereupon both darwin targets fail with
`cc: unrecognized command-line option '-framework'` — and leave the previous stub binaries in
`target/` for the image build to bake. That is the 0.18.4 stub-relay bug: one release shipped
with three of four relay binaries replaced by 28-byte `echo 'stub relay'` stubs.

The guards that followed: `_assert_real_relays` runs on `-ud`/`-bs` against the tagged image and
refuses on a wrong-magic or under-100 KB slot. **Note the gap it leaves** — `-bd` does *not* run
it, so a failed `-br` still produces a stub-bearing image locally and is only caught later.
Treat a non-zero `-br` as fatal rather than continuing.

## Release ordering

Relays are built **before** the image that bakes them. `RELEASE_PROTOCOL.md` holds the full
sequence, and that order is load-bearing rather than stylistic.
