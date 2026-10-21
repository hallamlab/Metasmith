# bash_relay — architecture

`msm_relay`, the small Rust binary metasmith drives a remote host through. Rust source under
`src/bash_relay/`; the built binaries are **baked into the agent container image**, which is what
distinguishes it from `workflow_solver` — that one runs locally and ships as package data.

## What goes in this file

What the relay is for and the protocol the calling side depends on: how a command is handed
over, how completion is observed, and what fails silently when the two halves stop agreeing.
Not an inventory of its subcommands — `--help` is that.

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

## The job protocol

A job is a set of files in the I/O directory sharing one id. The client writes the script under
`.compile` and **renames** it to `.start` — the watcher dispatches any `.start` it sees, so a
script it could observe half-written would be run half-written. Before that rename the client
also writes `.run` (`$METASMITH_RUN`, when the caller has one). The watcher then writes `.pid`,
and the launcher writes `.done` with the exit code.

Two invariants the two halves must both honour:

**`.pid` holds a process group, not a bare pid.** The launcher runs each job under `set -m`, so
the recorded number leads a group containing the tool the job started. Signalling the number
alone stops the job's shell and orphans the tool. Every stop path — the client's, the watcher's,
`kill-run` — signals the group, TERM then KILL.

**A watcher stops on `msm_relay stop`, or when `active` stops naming it.** It `setsid`s out of
the login shell's process group and points its stdio at `/dev/null`, so a dropped connection
neither signals it nor fails its next write. A shutdown that cannot kill a job keeps that job's
records rather than deleting them — they are the only trace of what is still running.

`msm_relay kill-run <token>` stops every job whose `.run` matches, and `status` names the run
each job belongs to. The I/O directory is per host and shared by every run on it, so the token
is the only thing that scopes a stop to one run. The bounce inverts ownership — a bounced job
runs as a child of the watcher, so Nextflow's process tree holds only the client — and the token
is what closes that gap. The watcher never infers a dead requester from a pid: a requester
inside a container reports a pid from its own namespace, which on the host names nothing.

## Who owns a workspace

A workspace has at most one watcher, and two records decide which one.

- **The start lock.** `start` holds an exclusive `flock` across its check, setup, fork and
  publish. The lock file sits beside the node-local workspace target under `$TMPDIR`, never in
  the agent home. `flock` on Lustre is not reliable, and the race the lock closes is between two
  starts on one node. Each login node has its own `/tmp`, so a lock there cannot order starts
  on different nodes, and does not need to: each node has its own workspace.
- **The owner record.** `active` holds the owning watcher's pid and its pid namespace. A start
  leaves a live owner alone, even a silent one. A watcher exits when `active` names another pid.
  Its exit guard then leaves the successor's jobs and files alone.

**CAUTION** A pid from another pid namespace names nothing on this host. A relay client inside a
container reads the host watcher's pid and cannot probe it, so a record from another namespace
counts as alive. Probing it marked every bounced job's watcher dead in the docker lane.

**CAUTION** A `start` that cannot see its watcher come up exits non-zero after 30 s and prints
the tail of `main.log`. Before that bound, a lost race left the start parent polling forever,
and the client's 60 s silence timeout reported only that the relay produced no output.

**The relay carries the engine version.** `--version` prints it and the watcher records it.
`start` replaces an idle watcher of another version, and `Agent.Deploy` re-extracts a home relay
whose version differs. The swap renames over the binary, so a running watcher cannot make the
copy fail with `ETXTBSY`. Without this, an upgrade of metasmith never reached an existing agent
home.

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
