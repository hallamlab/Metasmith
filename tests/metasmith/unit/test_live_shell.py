from __future__ import annotations
import os
import resource
import subprocess
import time
from contextlib import contextmanager

import pytest

from metasmith.coms import terminals
from metasmith.coms.terminals import LiveShell, ShellResult, TerminalProcess


def _fd_count():
    try:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))
    except FileNotFoundError:
        pytest.skip("/proc/self/fd not available")


def _child_bash_count():
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
    orig = LiveShell._INIT_TIMEOUT
    LiveShell._INIT_TIMEOUT = 1.0
    try:
        yield
    finally:
        LiveShell._INIT_TIMEOUT = orig


def test_g1_exit_code_zero():
    with LiveShell() as sh:
        res = sh.Exec("true", history=True)
        assert res.exit_code == 0


def test_g1_exit_code_nonzero():
    with LiveShell() as sh:
        res = sh.Exec("false", history=True)
        assert res.exit_code == 1


def test_g1_exit_code_arbitrary():
    with LiveShell() as sh:
        for code in (2, 42, 124, 127, 255):
            res = sh.Exec(f"(exit {code})", history=True)
            assert res.exit_code == code, f"expected {code}, got {res.exit_code}"


def test_g2_user_output_matching_frame_is_not_stripped():
    with LiveShell() as sh:
        fake_frame = '{"id":"anything","exit":99}'
        res = sh.Exec(f"""echo '{fake_frame}'; echo data""", history=True)
        assert res.exit_code == 0
        assert fake_frame in res.out, f"frame line was stripped: {res.out!r}"
        assert "data" in res.out


def test_g2_user_can_echo_legacy_marker():
    with LiveShell() as sh:
        res = sh.Exec('echo "done_anyhash.somevalue"; echo trailer', history=True)
        assert res.exit_code == 0
        assert "done_anyhash.somevalue" in res.out
        assert "trailer" in res.out


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


def test_g4_long_wait_does_not_burn_cpu():
    with LiveShell() as sh:
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
    assert cpu < 0.5, f"cpu={cpu:.2f}s during 3s wait — looks like polling"


def test_g5_clean_lifecycle_no_fd_leak():
    fd0 = _fd_count()
    bash0 = _child_bash_count()
    for _ in range(5):
        with LiveShell() as sh:
            sh.Exec("echo x", history=True)
    time.sleep(0.1)
    fd1 = _fd_count()
    bash1 = _child_bash_count()
    assert abs(fd1 - fd0) <= 2, f"fd leak: {fd0} -> {fd1}"
    assert bash1 <= bash0, f"bash child leak: {bash0} -> {bash1}"


def test_g5_partial_init_failure_does_not_leak():
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
        time.sleep(0.3)
    assert len(delivered) >= 6, f"good callback missed lines: {delivered}"
    assert any("line_1" in s for s in delivered)
    assert any("line_6" in s for s in delivered)


def test_g8_oversized_line_bounded():
    with LiveShell() as sh:
        sh.Exec("true", history=True)
        rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        cmd = (
            "python3 -c \"import sys; sys.stdout.write('a'*5_000_000); "
            "sys.stdout.write('\\nDONE\\n'); sys.stdout.flush()\""
        )
        res = sh.Exec(cmd, history=True, timeout=15)
        rss1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert res.exit_code == 0
    growth_mb = (rss1 - rss0) / 1024
    assert growth_mb < 50, f"RSS grew {growth_mb:.1f} MB — buffer cap not enforced"
    assert any("DONE" in line for line in res.out), f"DONE marker missing: tail={res.out[-3:]!r}"


def test_exec_async_await_done_returns_exit_code():
    with LiveShell() as sh:
        h = sh.ExecAsync("sleep 0.1; (exit 7)")
        rc = sh.AwaitDone(_hash=h, timeout=5)
        assert rc == 7, rc
        assert h not in sh._results
        assert h not in sh._pending


def test_batched_exec_async_drains_via_last_hash():
    with LiveShell() as sh:
        sleeps = [0.5, 0.4, 0.3, 0.2, 0.1]
        last_hash = None
        t0 = time.monotonic()
        for s in sleeps:
            last_hash = sh.ExecAsync(f"sleep {s}")
        assert last_hash is not None
        rc = sh.AwaitDone(_hash=last_hash, timeout=10)
        wall = time.monotonic() - t0
    assert rc == 0
    assert wall >= 1.4, f"AwaitDone(last_hash) returned too fast: {wall:.2f}s — batch not drained"


def test_await_done_requires_hash():
    with LiveShell() as sh:
        sh.ExecAsync("sleep 0.05")
        with pytest.raises(TypeError):
            sh.AwaitDone()  # type: ignore[call-arg]


def test_batch_strict_ordering_invariant():
    import random
    random.seed(42)
    with LiveShell() as sh:
        N = 50
        hashes = []
        for i in range(N):
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
        sh.Exec("echo inner", history=True, quiet=True)
        sh.Exec("echo after", history=True)
        time.sleep(0.2)
    assert "inner" not in outer
    assert any("after" in s for s in outer)


def test_subshell_nested_bash_round_trip():
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
    with LiveShell() as sh:
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
    with LiveShell() as sh:
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        root_lvl = int(r.out[0])
        with pytest.raises(RuntimeError):
            with sh.SubShell("bash"):
                raise RuntimeError("kaboom")
        assert sh._depth == 0
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        assert int(r.out[0]) == root_lvl


def test_subshell_pop_within_time_bounds():
    with LiveShell() as sh:
        t0 = time.monotonic()
        with sh.SubShell("bash"):
            pass
        elapsed = time.monotonic() - t0
        assert elapsed < 1.5, f"pop took {elapsed:.3f}s, expected < 1.5s"


def test_subshell_pop_retry_path():
    LiveShell._pop_drop_first_marker = True
    try:
        with LiveShell() as sh:
            with sh.SubShell("bash"):
                r = sh.Exec("echo inside", history=True, timeout=5)
                assert r.out == ["inside"]
            r = sh.Exec("echo recovered", history=True, timeout=5)
            assert r.exit_code == 0 and r.out == ["recovered"]
    finally:
        LiveShell._pop_drop_first_marker = False


def test_pop_quiescence_under_chatty_output():
    with LiveShell() as sh:
        sh.Exec("bash", timeout=5, inherit_stdin=True)
        sh.Exec("trap 'for i in 1 2 3 4 5; do echo bye_$i; done' EXIT", timeout=5)
        rc = sh._pop(quiescence_ms=150, idle_samples=3, timeout=5.0, retries=1)
        assert rc is not None
        r = sh.Exec("echo back", history=True, timeout=5)
        assert r.exit_code == 0 and r.out == ["back"]


@contextmanager
def _spawn_override(delay, inner="exec bash"):
    real = terminals.subprocess
    class _Shim:
        PIPE = subprocess.PIPE
        TimeoutExpired = subprocess.TimeoutExpired
        def Popen(self, args, **kw):
            if args == ["bash"]:
                args = ["bash", "-c", f"sleep {delay}; {inner}"]
            return subprocess.Popen(args, **kw)
    terminals.subprocess = _Shim()
    try:
        yield
    finally:
        terminals.subprocess = real


def test_g9_slow_but_alive_bash_still_inits():
    delay = 6.0
    t0 = time.monotonic()
    with _spawn_override(delay):
        with LiveShell() as sh:
            dt = time.monotonic() - t0
            r = sh.Exec("echo alive", history=True, timeout=10)
    assert r.exit_code == 0 and r.out == ["alive"], r.out
    assert dt >= delay - 0.5, (
        f"init returned in {dt:.2f}s; expected to wait ~{delay}s for the real "
        f"marker — a short deadline would have killed this live bash"
    )


def test_g9_dead_bash_fails_fast_with_rc():
    t0 = time.monotonic()
    with _spawn_override(0.5, inner="exit 7"):
        with pytest.raises(RuntimeError, match=r"bash exited \(rc=7\)"):
            LiveShell()
    dt = time.monotonic() - t0
    assert dt < 3.0, f"dead-bash init took {dt:.2f}s; should fail fast"


def test_g9_wedged_alive_bash_hits_backstop(monkeypatch):
    monkeypatch.setattr(LiveShell, "_INIT_TIMEOUT", 0.4)
    monkeypatch.setattr(LiveShell, "_INIT_POLL_INTERVAL", 0.01)
    t0 = time.monotonic()
    with _spawn_override(0, inner="exec sleep 30"):
        with pytest.raises(RuntimeError, match="unresponsive"):
            LiveShell()
    dt = time.monotonic() - t0
    assert dt >= 0.4, f"backstop fired too early ({dt:.2f}s)"
    assert dt < 2.0, f"backstop fired too late ({dt:.2f}s)"


def test_g9_command_path_timeout_none_survives_silence():
    with LiveShell() as sh:
        res = sh.Exec("sleep 2", history=True, timeout=None)
    assert res.exit_code == 0


class TestIdleTimeout:
    def test_a_silent_command_is_given_up_on(self):
        t0 = time.monotonic()
        with LiveShell() as sh:
            with pytest.raises(TimeoutError, match="no output"):
                sh.Exec("sleep 30", idle_timeout=0.6)
        dt = time.monotonic() - t0
        assert dt < 5, f"gave up too late ({dt:.2f}s)"

    def test_the_message_names_the_step(self):
        with LiveShell() as sh:
            with pytest.raises(TimeoutError, match="compiling the workflow"):
                sh.Exec("sleep 30", idle_timeout=0.4, what="compiling the workflow")

    def test_a_chattering_command_is_not(self):
        with LiveShell() as sh:
            res = sh.Exec(
                "for i in 1 2 3 4 5 6; do echo tick; sleep 0.2; done",
                history=True, idle_timeout=1.0,
            )
        assert res.exit_code == 0
        assert res.out.count("tick") == 6

    def test_carriage_returns_with_no_newline_count_as_alive(self):
        with LiveShell() as sh:
            res = sh.Exec(
                r"for i in 1 2 3 4 5 6; do printf 'pct %s\r' $i; sleep 0.2; done; echo",
                history=True, idle_timeout=1.0,
            )
        assert res.exit_code == 0

    def test_the_defaults_come_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("METASMITH_IDLE_TIMEOUT", "42")
        assert terminals._env_seconds("METASMITH_IDLE_TIMEOUT", 300) == 42

    def test_an_unreadable_override_falls_back_rather_than_dying(self, monkeypatch):
        monkeypatch.setenv("METASMITH_IDLE_TIMEOUT", "five minutes")
        assert terminals._env_seconds("METASMITH_IDLE_TIMEOUT", 300) == 300

    def test_a_shell_idle_before_the_command_is_not_a_silent_command(self):
        with LiveShell() as sh:
            sh.Exec("echo hello", history=True)
            time.sleep(0.8)
            res = sh.Exec("sleep 0.4; echo done", history=True, idle_timeout=0.6)
        assert res.exit_code == 0


@pytest.mark.slow
def test_g9_init_survives_reader_starvation_under_load():
    import threading

    stop = threading.Event()
    def gil_hog():
        x = 0
        while not stop.is_set():
            for _ in range(200000):
                x = (x * 1103515245 + 12345) & 0x7fffffff

    n_hogs = max(8, (os.cpu_count() or 4))
    hogs = [threading.Thread(target=gil_hog, daemon=True) for _ in range(n_hogs)]
    for h in hogs:
        h.start()
    time.sleep(0.3)

    failures = []
    try:
        for i in range(8):
            try:
                with LiveShell() as sh:
                    r = sh.Exec("echo ok", history=True, timeout=30)
                    assert r.out == ["ok"], r.out
            except Exception as e:
                failures.append((i, repr(e)))
    finally:
        stop.set()
        for h in hogs:
            h.join(timeout=2)

    assert not failures, f"init false-negatives under load: {failures}"


def test_last_byte_time_updates_on_output():
    with LiveShell() as sh:
        t_before = sh._last_byte_time
        time.sleep(0.05)
        sh.Exec("echo tick", history=True, timeout=5)
        assert sh._last_byte_time > t_before


def test_exec_stdin_reading_cmd_does_not_steal_marker():
    with LiveShell() as sh:
        r = sh.Exec("cat", timeout=5)
        assert r.exit_code == 0
        r2 = sh.Exec("echo hi", history=True, timeout=5)
        assert r2.exit_code == 0
        assert r2.out == ["hi"]


def test_exec_explicit_stdin_redirect_is_honored():
    with LiveShell() as sh:
        r = sh.Exec("cat < /etc/hostname", history=True, timeout=5)
        assert r.exit_code == 0
        assert len(r.out) >= 1 and r.out[0] != ""


def test_exec_multiline_body_preserves_env_mutation():
    import textwrap
    with LiveShell() as sh:
        cmd = textwrap.dedent("""
            x=42
            echo "x is $x"
        """)
        r = sh.Exec(cmd, history=True, timeout=5)
        assert r.exit_code == 0
        assert "x is 42" in r.out
        r2 = sh.Exec("echo $x", history=True, timeout=5)
        assert r2.exit_code == 0
        assert r2.out == ["42"]


def test_exec_inherit_stdin_passes_through_to_child():
    with LiveShell() as sh:
        sh.Exec("bash", timeout=5, inherit_stdin=True)
        r = sh.Exec("echo from-nested", history=True, timeout=5)
        assert r.exit_code == 0
        assert r.out == ["from-nested"]
        sh._pop(quiescence_ms=150, idle_samples=3, timeout=5.0, retries=1)


_REMOTE_HOST = os.environ.get("LIVESHELL_REMOTE_HOST", "")


@pytest.mark.network
@pytest.mark.skipif(not _REMOTE_HOST, reason="set LIVESHELL_REMOTE_HOST to an ssh-reachable host")
def test_exec_ssh_one_shot_returns_rc_zero():
    with LiveShell() as sh:
        r = sh.Exec(f"ssh {_REMOTE_HOST} true", timeout=30)
        assert r.exit_code == 0


@pytest.mark.network
@pytest.mark.skipif(not _REMOTE_HOST, reason="set LIVESHELL_REMOTE_HOST to an ssh-reachable host")
def test_exec_ssh_one_shot_returns_rc_nonzero():
    with LiveShell() as sh:
        r = sh.Exec(f"ssh {_REMOTE_HOST} false", timeout=30)
        assert r.exit_code == 1


@pytest.mark.network
@pytest.mark.skipif(not _REMOTE_HOST, reason="set LIVESHELL_REMOTE_HOST to an ssh-reachable host")
def test_exec_ssh_then_rsync_compound():
    with LiveShell() as sh:
        cmd = (
            f"ssh {_REMOTE_HOST} 'mkdir -p /tmp/lstest_msm' && "
            f"rsync /etc/hostname {_REMOTE_HOST}:/tmp/lstest_msm/probe.txt"
        )
        r = sh.Exec(cmd, timeout=60)
        assert r.exit_code == 0
        check = sh.Exec(
            f"ssh {_REMOTE_HOST} 'cat /tmp/lstest_msm/probe.txt'",
            history=True, timeout=30,
        )
        assert check.exit_code == 0
        assert len(check.out) > 0


def test_a_comment_is_a_legal_command():
    with LiveShell() as sh:
        r = sh.Exec("#!/bin/bash", timeout=10, history=True)
        assert r.exit_code == 0
        assert not any("syntax error" in ln for ln in r.err), r.err
        after = sh.Exec("echo still-here", timeout=10, history=True)
        assert "still-here" in after.out


def test_an_empty_command_is_a_legal_command():
    with LiveShell() as sh:
        assert sh.Exec("", timeout=10).exit_code == 0
        assert sh.Exec("   \n  ", timeout=10).exit_code == 0


def test_a_comment_does_not_mask_the_next_status():
    with LiveShell() as sh:
        assert sh.Exec("# a note\nfalse", timeout=10).exit_code == 1
        assert sh.Exec("# a note\ntrue", timeout=10).exit_code == 0


def test_a_dead_shell_does_not_wait_forever():
    with LiveShell() as sh:
        sh.Exec("echo alive", timeout=10)
        sh._shell._console.kill()
        start = time.monotonic()
        try:
            sh.Exec("echo after")
        except (BrokenPipeError, ConnectionError):
            pass
        assert time.monotonic() - start < 30
