"""
Shell-agnostic scenario suite for the LiveShell vs TmuxShell head-to-head.

Each Scenario.fn takes a `make_shell` factory (a 0-arg callable returning a
fresh shell whose object supports the LiveShell surface) and an optional ssh
`host`, and returns (passed: bool, detail: str). Scenarios own their shell
lifecycle so one failure cannot poison the next. The harness (compare_shells.py)
runs each fn under a watchdog so a real wedge is recorded as HANG, not a hang of
the whole run.

Cases are ported from tests/test_live_shell.py (the goals that drove LiveShell's
hardening) plus ssh boundary cases and robustness stress, with an emphasis on
the wedge-prone paths.
"""

from __future__ import annotations
import os
import resource
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class Scenario:
    name: str
    fn: Callable[..., tuple[bool, str]]
    needs_ssh: bool = False
    # generous per-scenario wall ceiling; the harness records HANG past this.
    timeout: float = 30.0


def _fd_count() -> int:
    return len(os.listdir(f"/proc/{os.getpid()}/fd"))


# ----------------------------------------------------------------------
# exit-code fidelity (G1)
# ----------------------------------------------------------------------

def s_exit_zero(make_shell, host=None):
    with make_shell() as sh:
        rc = sh.Exec("true", history=True, timeout=5).exit_code
    return rc == 0, f"rc={rc} (want 0)"


def s_exit_nonzero(make_shell, host=None):
    with make_shell() as sh:
        rc = sh.Exec("false", history=True, timeout=5).exit_code
    return rc == 1, f"rc={rc} (want 1)"


def s_exit_arbitrary(make_shell, host=None):
    want = [2, 42, 124, 127, 255]
    got = []
    with make_shell() as sh:
        for c in want:
            got.append(sh.Exec(f"(exit {c})", history=True, timeout=5).exit_code)
    return got == want, f"got={got} want={want}"


# ----------------------------------------------------------------------
# sentinel-collision immunity (G2)
# ----------------------------------------------------------------------

def s_collision_frame(make_shell, host=None):
    with make_shell() as sh:
        fake = '{"id":"anything","exit":99}'
        r = sh.Exec(f"echo '{fake}'; echo data", history=True, timeout=5)
    ok = r.exit_code == 0 and fake in r.out and "data" in r.out
    return ok, f"rc={r.exit_code} fake_present={fake in r.out} out={r.out!r}"


def s_collision_legacy_marker(make_shell, host=None):
    with make_shell() as sh:
        r = sh.Exec('echo "done_anyhash.somevalue"; echo trailer', history=True, timeout=5)
    ok = r.exit_code == 0 and "done_anyhash.somevalue" in r.out and "trailer" in r.out
    return ok, f"rc={r.exit_code} out={r.out!r}"


# ----------------------------------------------------------------------
# stream completeness (G3)
# ----------------------------------------------------------------------

def s_stderr_only_no_hang(make_shell, host=None):
    with make_shell() as sh:
        r = sh.Exec("echo hi >&2", history=True, timeout=5)
    ok = r.exit_code == 0 and r.out == [] and "hi" in r.err
    return ok, f"rc={r.exit_code} out={r.out!r} err={r.err!r}"


def s_silent_prompt(make_shell, host=None):
    with make_shell() as sh:
        t0 = time.monotonic()
        r = sh.Exec("true", history=True, timeout=5)
        dt = time.monotonic() - t0
    ok = r.exit_code == 0 and dt < 2.0
    return ok, f"rc={r.exit_code} latency={dt:.3f}s (want <2s)"


def s_mixed_streams(make_shell, host=None):
    with make_shell() as sh:
        r = sh.Exec("echo o; echo e >&2; echo o2; echo e2 >&2", history=True, timeout=5)
    ok = (r.exit_code == 0
          and {"o", "o2"} <= set(r.out)
          and {"e", "e2"} <= set(r.err))
    return ok, f"rc={r.exit_code} out={r.out!r} err={r.err!r}"


# ----------------------------------------------------------------------
# no CPU poll on long wait (G4)
# ----------------------------------------------------------------------

def s_no_cpu_poll(make_shell, host=None):
    with make_shell() as sh:
        sh.Exec("true", timeout=5)
        u0 = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        s0 = resource.getrusage(resource.RUSAGE_SELF).ru_stime
        t0 = time.monotonic()
        r = sh.Exec("sleep 3", history=True, timeout=10)
        wall = time.monotonic() - t0
        u1 = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        s1 = resource.getrusage(resource.RUSAGE_SELF).ru_stime
    cpu = (u1 - u0) + (s1 - s0)
    ok = r.exit_code == 0 and wall >= 2.8 and cpu < 0.5
    return ok, f"rc={r.exit_code} wall={wall:.2f}s cpu={cpu:.3f}s (want cpu<0.5)"


# ----------------------------------------------------------------------
# leak-freedom over N create/dispose (G5)
# ----------------------------------------------------------------------

def s_no_fd_leak(make_shell, host=None):
    fd0 = _fd_count()
    for _ in range(5):
        with make_shell() as sh:
            sh.Exec("echo x", history=True, timeout=5)
    time.sleep(0.2)
    fd1 = _fd_count()
    ok = abs(fd1 - fd0) <= 2
    return ok, f"fd {fd0} -> {fd1} (want delta<=2)"


# ----------------------------------------------------------------------
# async batch drain-on-last (the remote.py transfer fan-out idiom)
# ----------------------------------------------------------------------

def s_async_single(make_shell, host=None):
    with make_shell() as sh:
        h = sh.ExecAsync("sleep 0.1; (exit 7)")
        rc = sh.AwaitDone(_hash=h, timeout=5)
    return rc == 7, f"rc={rc} (want 7)"


def s_async_batch_last(make_shell, host=None):
    with make_shell() as sh:
        last = None
        t0 = time.monotonic()
        for s in (0.5, 0.4, 0.3, 0.2, 0.1):
            last = sh.ExecAsync(f"sleep {s}")
        rc = sh.AwaitDone(_hash=last, timeout=10)
        wall = time.monotonic() - t0
    ok = rc == 0 and wall >= 1.4
    return ok, f"rc={rc} wall={wall:.2f}s (want >=1.4s — batch drained)"


# ----------------------------------------------------------------------
# streaming callbacks fire DURING a long command
# ----------------------------------------------------------------------

def s_streaming_incremental(make_shell, host=None):
    arrivals = []
    with make_shell() as sh:
        sh.RegisterOnOut(lambda line: arrivals.append((line, time.monotonic())))
        t0 = time.monotonic()
        sh.Exec("for i in 1 2 3; do echo line_$i; sleep 0.3; done", timeout=10)
    lines = [a[0] for a in arrivals if a[0].startswith("line_")]
    times = [a[1] for a in arrivals if a[0].startswith("line_")]
    spread = (max(times) - min(times)) if len(times) >= 2 else 0.0
    ok = len(lines) >= 3 and spread >= 0.4
    return ok, f"lines={len(lines)} spread={spread:.2f}s (want >=3 lines, incremental)"


# ----------------------------------------------------------------------
# sub-shell crossing — nested bash (no network needed)
# ----------------------------------------------------------------------

def s_subshell_roundtrip(make_shell, host=None):
    with make_shell() as sh:
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        root = int(r.out[0])
        with sh.SubShell("bash"):
            r = sh.Exec("echo $SHLVL", history=True, timeout=5)
            inner = int(r.out[0])
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        back = int(r.out[0])
    ok = inner == root + 1 and back == root
    return ok, f"root={root} inner={inner} back={back}"


def s_subshell_two_levels(make_shell, host=None):
    with make_shell() as sh:
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        root = int(r.out[0])
        with sh.SubShell("bash"):
            with sh.SubShell("bash"):
                r = sh.Exec("echo $SHLVL", history=True, timeout=5)
                deep = int(r.out[0])
            r = sh.Exec("echo $SHLVL", history=True, timeout=5)
            mid = int(r.out[0])
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        back = int(r.out[0])
    ok = deep == root + 2 and mid == root + 1 and back == root
    return ok, f"root={root} deep={deep} mid={mid} back={back}"


def s_subshell_exception_pops(make_shell, host=None):
    with make_shell() as sh:
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        root = int(r.out[0])
        try:
            with sh.SubShell("bash"):
                raise RuntimeError("kaboom")
        except RuntimeError:
            pass
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        back = int(r.out[0])
    ok = back == root
    return ok, f"root={root} back={back} (want equal — popped on exception)"


def s_subshell_mixed_streams(make_shell, host=None):
    with make_shell() as sh:
        with sh.SubShell("bash"):
            r = sh.Exec("echo o_in; echo e_in >&2", history=True, timeout=5)
    ok = "o_in" in r.out and "e_in" in r.err and r.exit_code == 0
    return ok, f"rc={r.exit_code} out={r.out!r} err={r.err!r}"


# ----------------------------------------------------------------------
# robustness stress
# ----------------------------------------------------------------------

def s_subshell_repeated_crossing(make_shell, host=None):
    """10 enter/exit cycles on one shell — the path most prone to drift."""
    n = 10
    with make_shell() as sh:
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        root = int(r.out[0])
        for i in range(n):
            with sh.SubShell("bash"):
                r = sh.Exec("echo $SHLVL", history=True, timeout=5)
                if int(r.out[0]) != root + 1:
                    return False, f"cycle {i}: inner SHLVL={r.out} want {root+1}"
        r = sh.Exec("echo $SHLVL", history=True, timeout=5)
        back = int(r.out[0])
    ok = back == root
    return ok, f"{n} cycles ok, back={back} root={root}"


def s_subshell_chatty_exit(make_shell, host=None):
    """Sub-shell prints noise on EXIT — must not desync the parent."""
    with make_shell() as sh:
        with sh.SubShell("bash"):
            sh.Exec("trap 'for i in 1 2 3 4 5; do echo bye_$i; done' EXIT",
                    history=True, timeout=5)
        r = sh.Exec("echo back", history=True, timeout=5)
    ok = r.exit_code == 0 and r.out == ["back"]
    return ok, f"rc={r.exit_code} out={r.out!r} (want ['back'])"


# ----------------------------------------------------------------------
# ssh boundary cases (needs_ssh) — the crux of the experiment
# ----------------------------------------------------------------------

def s_ssh_oneshot_rc0(make_shell, host=None):
    with make_shell() as sh:
        rc = sh.Exec(f"ssh {host} true", timeout=30).exit_code
    return rc == 0, f"rc={rc} (want 0)"


def s_ssh_oneshot_rc1(make_shell, host=None):
    with make_shell() as sh:
        rc = sh.Exec(f"ssh {host} false", timeout=30).exit_code
    return rc == 1, f"rc={rc} (want 1)"


def s_ssh_rsync_compound(make_shell, host=None):
    with make_shell() as sh:
        cmd = (f"ssh {host} 'mkdir -p /tmp/lstest_msm' && "
               f"rsync /etc/hostname {host}:/tmp/lstest_msm/probe.txt")
        rc = sh.Exec(cmd, timeout=60).exit_code
        chk = sh.Exec(f"ssh {host} 'cat /tmp/lstest_msm/probe.txt'",
                      history=True, timeout=30)
    ok = rc == 0 and chk.exit_code == 0 and len(chk.out) > 0
    return ok, f"compound_rc={rc} check_rc={chk.exit_code} out={chk.out!r}"


def s_ssh_persistent_subshell(make_shell, host=None):
    """Enter a persistent ssh sub-shell, run several remote commands, pop back."""
    with make_shell() as sh:
        with sh.SubShell(f"ssh {host}"):
            r1 = sh.Exec("echo remote_$(hostname)", history=True, timeout=20)
            r2 = sh.Exec("false", history=True, timeout=20)
        back = sh.Exec("echo local_back", history=True, timeout=20)
    ok = (r1.exit_code == 0 and any("remote_" in x for x in r1.out)
          and r2.exit_code == 1
          and back.exit_code == 0 and back.out == ["local_back"])
    return ok, f"r1={r1.out!r}/{r1.exit_code} r2_rc={r2.exit_code} back={back.out!r}"


ALL: list[Scenario] = [
    Scenario("exit_zero", s_exit_zero),
    Scenario("exit_nonzero", s_exit_nonzero),
    Scenario("exit_arbitrary", s_exit_arbitrary),
    Scenario("collision_frame", s_collision_frame),
    Scenario("collision_legacy_marker", s_collision_legacy_marker),
    Scenario("stderr_only_no_hang", s_stderr_only_no_hang),
    Scenario("silent_prompt", s_silent_prompt),
    Scenario("mixed_streams", s_mixed_streams),
    Scenario("no_cpu_poll", s_no_cpu_poll, timeout=20),
    Scenario("no_fd_leak", s_no_fd_leak),
    Scenario("async_single", s_async_single),
    Scenario("async_batch_last", s_async_batch_last),
    Scenario("streaming_incremental", s_streaming_incremental),
    Scenario("subshell_roundtrip", s_subshell_roundtrip),
    Scenario("subshell_two_levels", s_subshell_two_levels),
    Scenario("subshell_exception_pops", s_subshell_exception_pops),
    Scenario("subshell_mixed_streams", s_subshell_mixed_streams),
    Scenario("subshell_repeated_crossing", s_subshell_repeated_crossing),
    Scenario("subshell_chatty_exit", s_subshell_chatty_exit),
    Scenario("ssh_oneshot_rc0", s_ssh_oneshot_rc0, needs_ssh=True),
    Scenario("ssh_oneshot_rc1", s_ssh_oneshot_rc1, needs_ssh=True),
    Scenario("ssh_rsync_compound", s_ssh_rsync_compound, needs_ssh=True, timeout=90),
    Scenario("ssh_persistent_subshell", s_ssh_persistent_subshell, needs_ssh=True, timeout=60),
]
