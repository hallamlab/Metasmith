"""
LiveShell unit tests covering the goals of the robust-upgrade plan
(plan: read-awm-inbodx-for-dynamic-brooks.md). One test per goal G1-G8.

These tests exercise the wrapper's surface contract — exit-code capture,
sentinel-collision immunity, blocking AwaitDone, lifecycle leak-freedom,
callback isolation, and bounded buffering — without depending on
external infrastructure (no docker, no relay, no network).
"""

from __future__ import annotations
import os
import resource
import subprocess
import time
from contextlib import contextmanager

import pytest

# conftest.py inserts src/ on sys.path
from metasmith.coms.terminals import LiveShell, ShellResult, TerminalProcess


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------

def _fd_count():
    """Count of open FDs in this process. Used for leak detection."""
    try:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))
    except FileNotFoundError:
        pytest.skip("/proc/self/fd not available")


def _child_bash_count():
    """How many bash children does this process have right now?"""
    try:
        out = subprocess.check_output(
            ["pgrep", "-P", str(os.getpid()), "bash"],
            stderr=subprocess.DEVNULL,
        ).decode()
        return len([l for l in out.splitlines() if l.strip()])
    except subprocess.CalledProcessError:
        return 0


@contextmanager
def _short_init():
    """Cap the LiveShell init timeout so partial-init tests don't drag."""
    orig = LiveShell._INIT_TIMEOUT
    LiveShell._INIT_TIMEOUT = 1.0
    try:
        yield
    finally:
        LiveShell._INIT_TIMEOUT = orig


# ----------------------------------------------------------------------
# G1 — exit codes are first-class
# ----------------------------------------------------------------------

def test_g1_exit_code_zero():
    with LiveShell() as sh:
        res = sh.Exec("true", history=True)
        assert res.exit_code == 0


def test_g1_exit_code_nonzero():
    with LiveShell() as sh:
        res = sh.Exec("false", history=True)
        assert res.exit_code == 1


def test_g1_exit_code_arbitrary():
    # (exit N) uses a subshell so the wrapper bash stays alive.
    with LiveShell() as sh:
        for code in (2, 42, 124, 127, 255):
            res = sh.Exec(f"(exit {code})", history=True)
            assert res.exit_code == code, f"expected {code}, got {res.exit_code}"


# ----------------------------------------------------------------------
# G2 — sentinel collision immunity
# ----------------------------------------------------------------------

def test_g2_user_output_matching_frame_is_not_stripped():
    """
    A user command that prints a JSON line shaped exactly like a completion
    frame must still appear verbatim in ShellResult.out — no in-band
    stripping. Same line was the previous design's classic foot-gun.
    """
    with LiveShell() as sh:
        fake_frame = '{"id":"anything","exit":99}'
        res = sh.Exec(f"""echo '{fake_frame}'; echo data""", history=True)
        assert res.exit_code == 0
        assert fake_frame in res.out, f"frame line was stripped: {res.out!r}"
        assert "data" in res.out


def test_g2_user_can_echo_legacy_marker():
    """
    Old in-band marker ("done_<random>.<hash>") echoed by user code
    must pass through untouched (no MARK in the wrapper anymore).
    """
    with LiveShell() as sh:
        res = sh.Exec('echo "done_anyhash.somevalue"; echo trailer', history=True)
        assert res.exit_code == 0
        assert "done_anyhash.somevalue" in res.out
        assert "trailer" in res.out


# ----------------------------------------------------------------------
# G3 — completion works regardless of which user stream is touched
# ----------------------------------------------------------------------

def test_g3_stderr_only_command_does_not_hang():
    with LiveShell() as sh:
        res = sh.Exec("echo hi >&2", history=True, timeout=5)
        assert res.exit_code == 0
        assert res.out == []
        assert "hi" in res.err


def test_g3_silent_command_returns_promptly():
    with LiveShell() as sh:
        t0 = time.monotonic()
        res = sh.Exec("true", history=True, timeout=5)
        dt = time.monotonic() - t0
        assert res.exit_code == 0
        assert dt < 2.0, f"silent command took {dt:.2f}s — should be subsecond"


def test_g3_mixed_streams():
    with LiveShell() as sh:
        res = sh.Exec("echo o; echo e >&2; echo o2; echo e2 >&2", history=True)
        assert res.exit_code == 0
        assert set(res.out) >= {"o", "o2"}
        assert set(res.err) >= {"e", "e2"}


# ----------------------------------------------------------------------
# G4 — AwaitDone does not CPU-poll
# ----------------------------------------------------------------------

def test_g4_long_wait_does_not_burn_cpu():
    """
    `sleep 3` should consume effectively zero CPU in the parent
    Python process: the wrapper sleeps in Condition.wait_for, not in
    a poll loop. Allow generous headroom for thread overhead.
    """
    with LiveShell() as sh:
        # Warm up so reader threads are running and stable.
        sh.Exec("true", history=True)
        u0 = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        s0 = resource.getrusage(resource.RUSAGE_SELF).ru_stime
        t0 = time.monotonic()
        res = sh.Exec("sleep 3", history=True, timeout=10)
        wall = time.monotonic() - t0
        u1 = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        s1 = resource.getrusage(resource.RUSAGE_SELF).ru_stime
    cpu = (u1 - u0) + (s1 - s0)
    assert res.exit_code == 0
    assert wall >= 2.8, f"sleep 3 returned too fast: {wall:.2f}s"
    # Polling design burns 100% of one core; the new design should be
    # near-zero. 0.5s budget allows for thread / reader overhead.
    assert cpu < 0.5, f"cpu={cpu:.2f}s during 3s wait — looks like polling"


# ----------------------------------------------------------------------
# G5 — TerminalProcess / LiveShell leak-free on partial init
# ----------------------------------------------------------------------

def test_g5_clean_lifecycle_no_fd_leak():
    fd0 = _fd_count()
    bash0 = _child_bash_count()
    for _ in range(5):
        with LiveShell() as sh:
            sh.Exec("echo x", history=True)
    # Reaper / FD release is synchronous in Dispose; allow tiny grace
    time.sleep(0.1)
    fd1 = _fd_count()
    bash1 = _child_bash_count()
    assert abs(fd1 - fd0) <= 2, f"fd leak: {fd0} -> {fd1}"
    assert bash1 <= bash0, f"bash child leak: {bash0} -> {bash1}"


def test_g5_partial_init_failure_does_not_leak():
    """
    If TerminalProcess.__init__ raises after pty.openpty() has allocated
    FDs, the cleanup path must release them. Force the failure by
    overriding subprocess.Popen to raise.
    """
    import metasmith.coms.terminals as tmod
    fd0 = _fd_count()
    orig_popen = subprocess.Popen
    def boom(*a, **kw):
        raise RuntimeError("simulated subprocess failure")
    tmod.subprocess.Popen = boom
    try:
        with pytest.raises(RuntimeError, match="simulated"):
            TerminalProcess()
    finally:
        tmod.subprocess.Popen = orig_popen
    time.sleep(0.1)
    fd1 = _fd_count()
    assert abs(fd1 - fd0) <= 2, f"partial-init leaked FDs: {fd0} -> {fd1}"


# ----------------------------------------------------------------------
# G7 — a raising callback does not stop subsequent delivery
# ----------------------------------------------------------------------

def test_g7_raising_callback_isolated():
    delivered = []
    fail_every_other = {"n": 0}
    def bad_cb(line):
        fail_every_other["n"] += 1
        if fail_every_other["n"] % 2 == 1:
            raise RuntimeError("intentional callback failure")
    def good_cb(line):
        delivered.append(line)
    with LiveShell() as sh:
        sh.RegisterOnOut(bad_cb)
        sh.RegisterOnOut(good_cb)
        sh.Exec("for i in 1 2 3 4 5 6; do echo line_$i; done")
        # let the reader thread drain
        time.sleep(0.3)
    assert len(delivered) >= 6, f"good callback missed lines: {delivered}"
    assert any("line_1" in s for s in delivered)
    assert any("line_6" in s for s in delivered)


# ----------------------------------------------------------------------
# G8 — bounded buffer on pathological no-newline output
# ----------------------------------------------------------------------

def test_g8_oversized_line_bounded():
    """
    A single ~5 MB write with no newline must not blow up memory. The
    NonBlockingReader caps its incomplete-line buffer at MAX_LINE_BYTES
    and emits one Log.Warn. We assert: (a) the wrapper survives,
    (b) RSS growth is bounded.
    """
    with LiveShell() as sh:
        sh.Exec("true", history=True)  # warm
        rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KB on linux
        # 5 MB of 'a' followed by a newline
        cmd = (
            "python3 -c \"import sys; sys.stdout.write('a'*5_000_000); "
            "sys.stdout.write('\\nDONE\\n'); sys.stdout.flush()\""
        )
        res = sh.Exec(cmd, history=True, timeout=15)
        rss1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert res.exit_code == 0
    # Allow 50 MB headroom — 5MB raw + decoding + test scaffolding. Without
    # the cap, multiple seconds of accumulation could easily exceed 100 MB.
    growth_mb = (rss1 - rss0) / 1024
    assert growth_mb < 50, f"RSS grew {growth_mb:.1f} MB — buffer cap not enforced"
    # We don't assert exact content because the cap drops bytes from the head;
    # the trailing DONE marker is what matters.
    assert any("DONE" in line for line in res.out), f"DONE marker missing: tail={res.out[-3:]!r}"


# ----------------------------------------------------------------------
# ExecAsync + AwaitDone(_hash) — the supported async path
# (4 batch sites in remote.py: local/globus/ssh/http transfer fan-out)
# ----------------------------------------------------------------------

def test_exec_async_await_done_returns_exit_code():
    with LiveShell() as sh:
        h = sh.ExecAsync("sleep 0.1; (exit 7)")
        rc = sh.AwaitDone(_hash=h, timeout=5)
        assert rc == 7, rc
        # AwaitDone reaps internal state — long-lived shells don't leak.
        assert h not in sh._results
        assert h not in sh._pending


def test_batched_exec_async_drains_via_last_hash():
    """
    The remote.py transfer-fanout pattern: enqueue N commands on one shell,
    then wait on the LAST hash. Because bash is sequential, the last frame
    arriving means all prior commands also completed.

    Regression guard for the (now-removed) bare-AwaitDone() semantic, which
    only waited for one random pending hash and left the rest in flight.
    """
    with LiveShell() as sh:
        # 5 commands with descending sleeps. If we only waited for the first,
        # we'd return after ~0.1s with 4 still in flight.
        sleeps = [0.5, 0.4, 0.3, 0.2, 0.1]
        last_hash = None
        t0 = time.monotonic()
        for s in sleeps:
            last_hash = sh.ExecAsync(f"sleep {s}")
        assert last_hash is not None
        rc = sh.AwaitDone(_hash=last_hash, timeout=10)
        wall = time.monotonic() - t0
    assert rc == 0
    # Total sequential wall is ~1.5s; assert we actually waited for the batch
    assert wall >= 1.4, f"AwaitDone(last_hash) returned too fast: {wall:.2f}s — batch not drained"


def test_await_done_requires_hash():
    """The bare AwaitDone() form is gone — it had no coherent semantic."""
    with LiveShell() as sh:
        sh.ExecAsync("sleep 0.05")
        with pytest.raises(TypeError):
            sh.AwaitDone()  # type: ignore[call-arg]


def test_batch_strict_ordering_invariant():
    """
    The wait-on-last idiom rests on a strict-ordering invariant: when
    `AwaitDone(_hash=last)` returns, every PRIOR hash in the batch must
    also be fully synced.

    The invariant holds because each of the three pipes (ctl/stdout/stderr)
    is read by a single thread in OS-preserved order, and bash writes
    those pipes in command order. We stress this with 50 commands of
    varied timings and assert the invariant directly.
    """
    import random
    random.seed(42)
    with LiveShell() as sh:
        N = 50
        hashes = []
        for i in range(N):
            # Mix command shapes to perturb scheduling: silent, stdout,
            # stderr, mixed, sleep, nonzero exit, fast.
            shape = i % 7
            if shape == 0:   cmd = "true"
            elif shape == 1: cmd = f"echo o_{i}"
            elif shape == 2: cmd = f"echo e_{i} 1>&2"
            elif shape == 3: cmd = f"echo o_{i}; echo e_{i} 1>&2"
            elif shape == 4: cmd = f"sleep 0.0{random.randint(1, 5)}"
            elif shape == 5: cmd = f"(exit {i % 7})"
            else:            cmd = f"echo o_{i}; echo e_{i} 1>&2; (exit {i % 3})"
            hashes.append(sh.ExecAsync(cmd))

        rc = sh.AwaitDone(_hash=hashes[-1], timeout=20)
        # Last hash was reaped by AwaitDone; every prior hash must still be
        # fully synced (not yet reaped). If even one isn't, the strict
        # ordering invariant is broken and the wait-on-last idiom is unsafe.
        not_synced_prior = [h for h in hashes[:-1] if not sh._is_fully_synced(h)]
        assert not not_synced_prior, (
            f"strict-ordering INVARIANT BROKEN: {len(not_synced_prior)}/{N-1} "
            f"prior hashes not fully synced after AwaitDone(_hash=last). "
            f"wait-on-last idiom is unsafe. examples: {not_synced_prior[:5]}"
        )


def test_quiet_mode_does_not_pollute_outer_callbacks():
    outer = []
    with LiveShell() as sh:
        sh.RegisterOnOut(outer.append)
        # quiet mode: outer should NOT see the inner echo
        sh.Exec("echo inner", history=True, quiet=True)
        # post-quiet: outer should see the next echo
        sh.Exec("echo after", history=True)
        time.sleep(0.2)
    assert "inner" not in outer
    assert any("after" in s for s in outer)


# ----------------------------------------------------------------------
# SubShell + quiescence (Approach D follow-up — robust shell-boundary
# crossing via SubShell context manager, no ssh needed for these tests)
# ----------------------------------------------------------------------

def test_subshell_nested_bash_round_trip():
    """Enter nested local bash, run a command, leave, continue locally."""
    with LiveShell() as sh:
        assert sh._depth == 0
        r = sh.Exec("echo top", history=True, timeout=5)
        assert r.exit_code == 0 and r.out == ["top"]
        with sh.SubShell("bash"):
            assert sh._depth == 1
            r = sh.Exec("echo nested", history=True, timeout=5)
            assert r.exit_code == 0 and r.out == ["nested"]
        assert sh._depth == 0
        r = sh.Exec("echo back", history=True, timeout=5)
        assert r.exit_code == 0 and r.out == ["back"]


def test_subshell_two_levels_deep():
    """Two-level nesting (bash → bash) pops cleanly back to the root shell."""
    with LiveShell() as sh:
        # SHLVL increments for each nested bash invocation, so it's a stable
        # signal that we are actually at the depth we think we are.
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        root_lvl = int(r.out[0])
        with sh.SubShell("bash"):
            r = sh.Exec("echo $SHLVL", history=True, timeout=5)
            assert int(r.out[0]) == root_lvl + 1
            with sh.SubShell("bash"):
                r = sh.Exec("echo $SHLVL", history=True, timeout=5)
                assert int(r.out[0]) == root_lvl + 2
                assert sh._depth == 2
            r = sh.Exec("echo $SHLVL", history=True, timeout=5)
            assert int(r.out[0]) == root_lvl + 1
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        assert int(r.out[0]) == root_lvl
        assert sh._depth == 0


def test_subshell_pops_on_exception_in_body():
    """Exception inside the with body still pops the sub-shell on exit."""
    with LiveShell() as sh:
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        root_lvl = int(r.out[0])
        with pytest.raises(RuntimeError):
            with sh.SubShell("bash"):
                raise RuntimeError("kaboom")
        assert sh._depth == 0
        # Back at root shell — SHLVL should match pre-entry value.
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        assert int(r.out[0]) == root_lvl


def test_subshell_pop_within_time_bounds():
    """Default pop completes within ~1s for a silent nested bash."""
    with LiveShell() as sh:
        t0 = time.monotonic()
        with sh.SubShell("bash"):
            pass
        elapsed = time.monotonic() - t0
        # 150ms floor + 3*50ms samples ≈ 250-350ms in the silent case;
        # add headroom for CI jitter but cap well below 5s timeout.
        assert elapsed < 1.5, f"pop took {elapsed:.3f}s, expected < 1.5s"


def test_subshell_pop_retry_path():
    """Force the first marker write to be dropped; retry must recover."""
    LiveShell._pop_drop_first_marker = True
    try:
        with LiveShell() as sh:
            with sh.SubShell("bash"):
                r = sh.Exec("echo inside", history=True, timeout=5)
                assert r.out == ["inside"]
            # If pop's retry didn't fire, this Exec would wedge.
            r = sh.Exec("echo recovered", history=True, timeout=5)
            assert r.exit_code == 0 and r.out == ["recovered"]
    finally:
        LiveShell._pop_drop_first_marker = False


def test_pop_quiescence_under_chatty_output():
    """A sub-shell that prints noise on exit shouldn't trip premature pop."""
    with LiveShell() as sh:
        # Nested bash that prints a multi-line message via PROMPT_COMMAND-
        # equivalent: install an EXIT trap that emits chatter, then exit.
        # inherit_stdin=True mirrors SubShell.__enter__: the nested bash
        # needs the real stdin pipe so subsequent Exec writes reach it.
        sh.Exec("bash", timeout=5, inherit_stdin=True)
        # Inside nested bash, install the trap and then leave via _pop.
        sh.Exec("trap 'for i in 1 2 3 4 5; do echo bye_$i; done' EXIT", timeout=5)
        rc = sh._pop(quiescence_ms=150, idle_samples=3, timeout=5.0, retries=1)
        assert rc is not None
        # Back at root shell.
        r = sh.Exec("echo back", history=True, timeout=5)
        assert r.exit_code == 0 and r.out == ["back"]


# ----------------------------------------------------------------------
# G9 — init timeout measures inactivity, not total runtime
# ----------------------------------------------------------------------

def _rearm_init(sh):
    """Reset the init handshake bookkeeping so _wait_for_init can be driven
    again under test-controlled timing."""
    with sh._cond:
        sh._pending.add(sh._INIT_NONCE)
        sh._sync_received[sh._INIT_NONCE] = set()
        sh._results.pop(sh._INIT_NONCE, None)
        sh._last_byte_time = time.monotonic()


def test_g9_init_timeout_tracks_activity_not_total_time():
    """
    Init must NOT time out while bytes keep flowing, even once total elapsed
    time exceeds the idle window. Proves the window resets on activity (it is
    an *idle* window) rather than being a fixed total-time ceiling.

    We drive _wait_for_init against a real (alive) bash with a tiny idle
    window, while a feeder thread keeps bumping _last_byte_time — the same
    signal _make_tee stamps on every incoming chunk — for roughly twice the
    window before delivering the markers.
    """
    import threading

    with LiveShell() as sh:
        sh._INIT_TIMEOUT = 0.3        # idle window
        sh._INIT_POLL_INTERVAL = 0.01
        _rearm_init(sh)

        active_for = 0.6              # ~2x the idle window
        def feeder():
            t_end = time.monotonic() + active_for
            while time.monotonic() < t_end:
                with sh._cond:
                    sh._last_byte_time = time.monotonic()  # simulate a chunk
                time.sleep(0.02)
            # Activity stops; deliver both init markers so the wait succeeds.
            with sh._cond:
                sh._results[sh._INIT_NONCE] = 0
                sh._sync_received[sh._INIT_NONCE] = {"out", "err"}
                sh._cond.notify_all()

        th = threading.Thread(target=feeder)
        t0 = time.monotonic()
        th.start()
        sh._wait_for_init()           # must not raise despite elapsed > window
        elapsed = time.monotonic() - t0
        th.join(timeout=2)

    # Survived well past the 0.3s idle window because activity kept resetting
    # it — a total-time ceiling would have raised at 0.3s.
    assert elapsed >= 0.5, (
        f"init returned after only {elapsed:.2f}s — too early to prove the "
        f"idle window survived sustained activity"
    )


def test_g9_init_fails_fast_when_bash_dead():
    """
    If bash exits before responding, init must fail within ~a poll interval —
    not after the full idle window — and the error must name the exit code.
    This is the alive-vs-dead distinction the old fixed-deadline wait could
    not make.
    """
    with LiveShell() as sh:
        sh._INIT_TIMEOUT = 30.0       # long idle window: must NOT be reached
        sh._INIT_POLL_INTERVAL = 0.01
        _rearm_init(sh)

        # Kill the underlying bash so IsAlive() -> False and ExitCode() is set.
        sh._shell._console.kill()
        sh._shell._console.wait(timeout=2)

        t0 = time.monotonic()
        with pytest.raises(RuntimeError, match="bash exited"):
            sh._wait_for_init()
        dt = time.monotonic() - t0

    assert dt < 1.0, (
        f"dead-bash init took {dt:.2f}s; should fail fast, far below the "
        f"30s idle window"
    )


def test_last_byte_time_updates_on_output():
    """The quiescence timestamp moves forward when bytes arrive."""
    with LiveShell() as sh:
        t_before = sh._last_byte_time
        time.sleep(0.05)
        sh.Exec("echo tick", history=True, timeout=5)
        assert sh._last_byte_time > t_before


# ----------------------------------------------------------------------
# stdin-isolation (the </dev/null wrap): user cmds get EOF from /dev/null
# instead of stealing bash's stdin pipe. Mutation gate: reverting the wrap
# in ExecAsync makes test_exec_stdin_reading_cmd_does_not_steal_marker
# and the network-gated ssh tests time out.
# ----------------------------------------------------------------------

def test_exec_stdin_reading_cmd_does_not_steal_marker():
    """`cat` would consume the marker emission line off bash's stdin pipe
    without the </dev/null wrap. With the wrap it gets EOF and exits."""
    with LiveShell() as sh:
        r = sh.Exec("cat", timeout=5)
        assert r.exit_code == 0
        # Next Exec must still work — proves the first one didn't break
        # the marker protocol or leave bytes stranded on the pipe.
        r2 = sh.Exec("echo hi", history=True, timeout=5)
        assert r2.exit_code == 0
        assert r2.out == ["hi"]


def test_exec_explicit_stdin_redirect_is_honored():
    """`cat < file` overrides the outer </dev/null and reads the file."""
    with LiveShell() as sh:
        r = sh.Exec("cat < /etc/hostname", history=True, timeout=5)
        assert r.exit_code == 0
        assert len(r.out) >= 1 and r.out[0] != ""


def test_exec_multiline_body_preserves_env_mutation():
    """The brace group must preserve env mutations across Execs (current
    shell, not a subshell). Multi-line bodies must parse cleanly under
    the `{\\n ... \\n} </dev/null` wrap regardless of trailing whitespace."""
    import textwrap
    with LiveShell() as sh:
        cmd = textwrap.dedent("""
            x=42
            echo "x is $x"
        """)
        r = sh.Exec(cmd, history=True, timeout=5)
        assert r.exit_code == 0
        assert "x is 42" in r.out
        # Brace-group scope preserves env in the parent shell.
        r2 = sh.Exec("echo $x", history=True, timeout=5)
        assert r2.exit_code == 0
        assert r2.out == ["42"]


def test_exec_inherit_stdin_passes_through_to_child():
    """With inherit_stdin=True the user cmd inherits bash's stdin pipe.
    Verified by entering nested bash and running a command in it — the
    SubShell mechanism this enables. Mirrors test_pop_quiescence path."""
    with LiveShell() as sh:
        sh.Exec("bash", timeout=5, inherit_stdin=True)
        r = sh.Exec("echo from-nested", history=True, timeout=5)
        assert r.exit_code == 0
        assert r.out == ["from-nested"]
        # Pop back out cleanly so the LiveShell disposes cleanly.
        sh._pop(quiescence_ms=150, idle_samples=3, timeout=5.0, retries=1)


# ----------------------------------------------------------------------
# Network-gated: real-traffic confirmation against an ssh-reachable host.
# Run with: LIVESHELL_REMOTE_HOST=sockeye pytest -m network
# ----------------------------------------------------------------------

_REMOTE_HOST = os.environ.get("LIVESHELL_REMOTE_HOST", "")


@pytest.mark.network
@pytest.mark.skipif(not _REMOTE_HOST, reason="set LIVESHELL_REMOTE_HOST to an ssh-reachable host")
def test_exec_ssh_one_shot_returns_rc_zero():
    """Exec("ssh host true") must return rc=0, not None. Pre-fix this
    wedged because ssh consumed the marker line from bash's stdin."""
    with LiveShell() as sh:
        r = sh.Exec(f"ssh {_REMOTE_HOST} true", timeout=30)
        assert r.exit_code == 0


@pytest.mark.network
@pytest.mark.skipif(not _REMOTE_HOST, reason="set LIVESHELL_REMOTE_HOST to an ssh-reachable host")
def test_exec_ssh_one_shot_returns_rc_nonzero():
    """ssh propagates the remote command's exit code."""
    with LiveShell() as sh:
        r = sh.Exec(f"ssh {_REMOTE_HOST} false", timeout=30)
        assert r.exit_code == 1


@pytest.mark.network
@pytest.mark.skipif(not _REMOTE_HOST, reason="set LIVESHELL_REMOTE_HOST to an ssh-reachable host")
def test_exec_ssh_then_rsync_compound():
    """The exact rsync.py:55 pattern: `ssh host mkdir && rsync src host:dst`.
    Verifies the compound runs and the destination file exists remotely."""
    with LiveShell() as sh:
        cmd = (
            f"ssh {_REMOTE_HOST} 'mkdir -p /tmp/lstest_msm' && "
            f"rsync /etc/hostname {_REMOTE_HOST}:/tmp/lstest_msm/probe.txt"
        )
        r = sh.Exec(cmd, timeout=60)
        assert r.exit_code == 0
        # Confirm the file landed.
        check = sh.Exec(
            f"ssh {_REMOTE_HOST} 'cat /tmp/lstest_msm/probe.txt'",
            history=True, timeout=30,
        )
        assert check.exit_code == 0
        assert len(check.out) > 0
