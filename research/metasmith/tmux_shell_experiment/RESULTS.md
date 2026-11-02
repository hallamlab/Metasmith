# Is tmux a more robust LiveShell? — verdict

**Short answer: no — not for metasmith's load-bearing case.** A full-parity
tmux-backed shell matches LiveShell on every *local* path (often that's the easy
part), but tmux's pty-centric architecture actively *breaks* the one thing that
drove LiveShell's complexity in the first place: transparently crossing into a
persistent `ssh` sub-shell.

Run yourself:
```
mamba run -n msm python main/tmux_shell_experiment/compare_shells.py
LIVESHELL_REMOTE_HOST=<host> mamba run -n msm python main/tmux_shell_experiment/compare_shells.py
```
Mechanical data: `RESULTS.generated.md` (regenerated each run). Curated analysis: this file.

## What was built

- `tmux_shell.py` — `TmuxShell`, exposing the LiveShell surface the call sites use
  (`Exec` / `ExecAsync` / `AwaitDone` / `SubShell` / `RegisterOn*` / `Dispose`,
  context manager, `ShellResult`). tmux owns a private-socket session; input via
  `send-keys`; stdout/stderr split via two FIFOs (so `.out` vs `.err` is
  preserved); local completion via a durable control FIFO carrying
  `\x1eDONE <token> <nonce> <rc>\x1e`; the ssh sub-shell path falls back to an
  in-band sentinel scraped off stdout.
- `scenarios.py` — 23 shell-agnostic scenarios ported from `tests/test_live_shell.py`
  (the goals that drove LiveShell's hardening) plus ssh boundary + robustness stress.
- `compare_shells.py` — runs every scenario against both shells under a per-scenario
  watchdog (a real wedge is recorded as HANG, not a hang of the run).

## Head-to-head (tmux 3.2a, ssh host = `mira`)

| Shell | PASS | FAIL | HANG | ERROR |
|---|---|---|---|---|
| **LiveShell** | **23** | 0 | 0 | 0 |
| **TmuxShell** | 22 | **1** | 0 | 0 |

- **All 19 local scenarios: identical PASS** on both — exit-code fidelity,
  sentinel-collision immunity, stderr-only/mixed streams, no-CPU-poll
  (0.001s CPU over a 3s wait), FD-leak-freedom, async batch drain-on-last,
  incremental streaming callbacks, nested-bash sub-shell round-trip / two-level /
  exception-pop / chatty-exit / 10× repeated crossing.
- **One-shot ssh (`ssh host cmd`): PASS** on both — rc=0, rc=1, and the
  `ssh mkdir && rsync` compound. A one-shot ssh requests no remote pty, so nothing
  mangles the marker.
- **Persistent ssh sub-shell (`ssh host` → exec → exit): LiveShell PASS (0.54s),
  TmuxShell FAIL (timed out, 40.9s).** Same command, opposite outcome.

## Why TmuxShell fails the persistent ssh case — the headline

tmux drives input by typing into a real pane **pty**. So `ssh host` (no command),
seeing a tty on stdin, allocates a **remote** pty and starts an *interactive*
remote shell. That remote shell applies full terminal processing to everything —
bracketed-paste (`\x1b[?2004h`), ANSI-colored prompt, command echo, and, fatally,
it eats/translates the `\x1e` (RS) control bytes our completion marker is built
from. The marker arrives corrupted, never matches, and `AwaitDone` times out:

```
r1.out = ['\x1b[?2004l\rremote_pavilion',
          'SUB <token> <nonce> 0']   # <- the \x1e brackets are GONE
r1.exit_code = None                  # marker never matched -> wedge
```

**LiveShell sidesteps this for free.** Its bash stdin is a `subprocess.PIPE`, not
a tty, so `ssh host` stays *non-interactive*: no remote pty, no prompt, no echo,
no terminal processing — the raw RS marker bytes round-trip untouched. This is the
same property that defeated the earlier out-of-band fd-5 channel (commit
`7a8008f`): a side channel can't cross ssh, so the marker has to be plain bytes
that survive transit — and tmux's pty corrupts exactly those bytes.

**The fix exists but is a footgun.** Forcing `ssh -T host` (no remote pty) makes
TmuxShell pass *and* fast — 0.6s vs 24.9s in a direct A/B — because the remote
shell goes non-interactive, like LiveShell's. But every caller must remember `-T`
on every ssh/docker/sub-shell entry; LiveShell needs nothing. tmux turns a
free property into a per-call obligation.

## Other findings

- **Latency:** TmuxShell adds ~20ms/command (multiple `send-keys` subprocess
  round-trips per Exec) vs LiveShell's sub-millisecond pipe writes. Negligible for
  deploy/transfer/nextflow-launch; visible only in tight loops.
- **`tmux wait-for` is not usable for completion.** It is local-server-only (can't
  cross ssh, same wall as above) and does not queue a signal sent before a waiter
  arrives — racy for the async fan-out idiom. The prototype uses a control FIFO
  instead; that FIFO is itself an out-of-band side channel that *also* can't cross
  ssh — which is exactly why the sub-shell path needs in-band markers regardless of
  shell. tmux removes the in-band marker only for the local case LiveShell already
  handles fine.
- **The one genuine tmux upside:** the pane process is owned by a detached tmux
  server, so it survives the Python process dying and can be re-attached
  (`tmux -L <sock> attach`) — a real plus for hours-long `RunWorkflow` runs and
  live debugging. LiveShell's bash dies with its parent. This does not require
  replacing LiveShell, though.

## Recommendation

**Keep LiveShell.** Its in-band-marker-over-pipe-stdin design is not incidental
complexity — it is the minimal mechanism that makes completion survive the ssh /
docker / nested-bash boundary, which is metasmith's actual deployment reality. A
tmux rewrite would re-introduce that same in-band marker for the boundary case
*and* add a pty that corrupts it by default, a binary dependency (tmux is in the
Docker image but not in `envs/base.yml`), and ~20ms/command — to gain nothing on
the local path that already works.

If detached-survivability or live-attach is wanted for long workflow runs, pursue
it narrowly — e.g. launch only the detached `nextflow` step inside a tmux/`screen`
session — rather than replacing the shell abstraction. That keeps the boundary
crossing on the proven LiveShell path.
