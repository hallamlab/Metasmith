"""The relay's job protocol, against the real binary.

src/bash_relay carries no unit tests: the behaviours that matter here -- what a
stop reaches, what a dead requester leaves behind, what survives a SIGHUP -- are
only observable from outside the process. Each test pins one of them.
"""
import os
import platform
import signal
import subprocess
from pathlib import Path

import pytest

from _procs import alive, group_alive, pids_matching, wait_until

pytestmark = pytest.mark.requires_relay

_TRIPLES = {
    ("x86_64", "Linux"): "x86_64-unknown-linux-musl",
    ("aarch64", "Linux"): "aarch64-unknown-linux-musl",
    ("arm64", "Linux"): "aarch64-unknown-linux-musl",
    ("x86_64", "Darwin"): "x86_64-apple-darwin",
    ("arm64", "Darwin"): "aarch64-apple-darwin",
    ("aarch64", "Darwin"): "aarch64-apple-darwin",
}

_REPO = Path(__file__).resolve().parents[3]


def _relay_binary() -> Path | None:
    triple = _TRIPLES.get((platform.machine(), platform.system()))
    if triple is None:
        return None
    path = _REPO/"src/bash_relay/target"/triple/"release/msm_relay"
    return path if path.is_file() else None


@pytest.fixture(scope="module")
def relay_bin() -> Path:
    path = _relay_binary()
    if path is None:
        pytest.skip("no msm_relay built for this platform; run ./dev/metasmith.sh -br")
    return path


def _status(relay_bin: Path, io: Path) -> str:
    return subprocess.run(
        [str(relay_bin), "--io", str(io), "status"],
        capture_output=True, text=True, timeout=30,
    ).stdout


def _relay_alive(relay_bin: Path, io: Path) -> bool:
    return "alive: true" in _status(relay_bin, io)


@pytest.fixture
def relay(relay_bin, tmp_path, spawner):
    # Started through a shell that outlives it, so a test can signal the process
    # group the relay was launched from without signalling the test runner.
    io = tmp_path/"relay_io"
    holder = spawner(f'"{relay_bin}" --io "{io}" start --local\nsleep 600\n')
    assert wait_until(lambda: _relay_alive(relay_bin, io), timeout=30), "relay never came up"
    yield io, holder
    subprocess.run(
        [str(relay_bin), "--io", str(io), "stop"], capture_output=True, timeout=30,
    )


def _bounce(spawner, relay_bin, io, cmd: str, run_token: str | None = None):
    env = dict(os.environ)
    if run_token is not None:
        env["METASMITH_RUN"] = run_token
    else:
        env.pop("METASMITH_RUN", None)
    return spawner(f'"{relay_bin}" --io "{io}" bounce "{cmd}"\n', env=env)


def job_pids(needle: str) -> list[int]:
    # The bounce client's own cmdline carries the command it asked for, so a
    # naive /proc scan finds the client as well as the job. The client is not
    # part of the job and does not die with it.
    return [
        p for p in pids_matching(needle)
        if "msm_relay" not in _cmdline(p)
    ]


def _cmdline(pid: int) -> str:
    try:
        return (Path(f"/proc/{pid}/cmdline")).read_bytes().replace(b"\0", b" ").decode()
    except OSError:
        return ""


def test_stop_kills_the_job_and_its_grandchildren(relay, relay_bin, spawner):
    io, _ = relay
    _bounce(spawner, relay_bin, io, "sleep 911")
    assert wait_until(lambda: job_pids("sleep 911")), "bounced job never started"
    job = job_pids("sleep 911")

    subprocess.run([str(relay_bin), "--io", str(io), "stop"], capture_output=True, timeout=30)

    assert wait_until(lambda: not any(alive(p) for p in job)), (
        "msm_relay stop left the bounced job's tool running"
    )


def test_job_is_reclaimed_when_its_requester_dies(relay, relay_bin, spawner):
    # The reported bug, pinned: nextflow sees only the bounce client, so killing
    # nextflow kills clients and leaves the tools running. The watcher notices
    # the requester is gone and stops the job it stands in for.
    io, _ = relay
    client = _bounce(spawner, relay_bin, io, "sleep 912")
    assert wait_until(lambda: job_pids("sleep 912")), "bounced job never started"
    job = job_pids("sleep 912")

    os.killpg(client.pid, signal.SIGKILL)
    client.wait(timeout=10)

    assert wait_until(lambda: not any(alive(p) for p in job), timeout=30), (
        "the watcher did not reclaim a job whose requester was killed"
    )


def test_kill_run_stops_one_run_and_leaves_the_other(relay, relay_bin, spawner):
    io, _ = relay
    _bounce(spawner, relay_bin, io, "sleep 913", run_token="RUNA.t")
    _bounce(spawner, relay_bin, io, "sleep 914", run_token="RUNB.t")
    assert wait_until(lambda: job_pids("sleep 913") and job_pids("sleep 914"))
    doomed, spared = job_pids("sleep 913"), job_pids("sleep 914")

    subprocess.run(
        [str(relay_bin), "--io", str(io), "kill-run", "RUNA.t"],
        capture_output=True, text=True, timeout=30,
    )

    assert wait_until(lambda: not any(alive(p) for p in doomed))
    assert all(alive(p) for p in spared), "kill-run reached a second run's jobs"


def test_status_names_the_run_each_job_belongs_to(relay, relay_bin, spawner):
    io, _ = relay
    _bounce(spawner, relay_bin, io, "sleep 915", run_token="RUNC.t")
    assert wait_until(lambda: "run=RUNC.t" in _status(relay_bin, io), timeout=30), (
        f"status did not name the run: {_status(relay_bin, io)!r}"
    )


def test_watcher_survives_a_sighup_to_the_group_it_started_in(relay, relay_bin, spawner):
    # The disconnect policy, as a test: a dropped ssh connection HUPs the login
    # shell's process group, and that must not stop a run.
    io, holder = relay
    _bounce(spawner, relay_bin, io, "sleep 916")
    assert wait_until(lambda: job_pids("sleep 916"))
    job = job_pids("sleep 916")

    os.killpg(holder.pid, signal.SIGHUP)
    holder.wait(timeout=10)

    assert wait_until(lambda: _relay_alive(relay_bin, io), timeout=10), (
        "a SIGHUP to the starting group stopped the watcher"
    )
    assert all(alive(p) for p in job), "a SIGHUP to the starting group stopped a job"


def _foreign_pgid() -> int | None:
    # A process group we can see but not signal: EPERM, not ESRCH. That is the
    # only way a job survives try_kill_jobs, and it is the case whose record
    # must not be wiped.
    me = os.getuid()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if entry.stat().st_uid == me:
                continue
            pgid = os.getpgid(int(entry.name))
        except (OSError, ProcessLookupError):
            continue
        if pgid > 1 and group_alive(pgid):
            try:
                os.killpg(pgid, 0)
                os.kill(pgid, 0)
            except PermissionError:
                return pgid
            except (OSError, ProcessLookupError):
                continue
    return None


def test_wipe_keeps_the_record_of_a_job_it_could_not_kill(relay_bin, tmp_path):
    pgid = _foreign_pgid()
    if pgid is None:
        pytest.skip("no process group visible-but-unsignallable from this uid")
    io = tmp_path/"relay_io"
    io.mkdir()
    stale = io/"deadbeef.pid"
    stale.write_text(f"{pgid}\n")

    # Not capture_output: `start` daemonizes, and the watcher holds the pipe
    # open for as long as it runs, so a capturing run would wait on it forever.
    subprocess.run(
        [str(relay_bin), "--io", str(io), "start", "--local"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60,
    )
    try:
        assert wait_until(lambda: (io/"active").exists(), timeout=30)
        assert stale.exists(), (
            "the startup wipe deleted the record of a job it failed to kill, "
            "erasing the only trace of what is still running"
        )
    finally:
        subprocess.run(
            [str(relay_bin), "--io", str(io), "stop"], capture_output=True, timeout=30,
        )
