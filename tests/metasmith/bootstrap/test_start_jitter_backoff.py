from pathlib import Path
import subprocess

_SH = Path(__file__).parent / "start_jitter_backoff.sh"


def _run(*args: str) -> int:
    r = subprocess.run(["/bin/bash", str(_SH), *args], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, f"{args} -> rc {r.returncode}: {r.stderr}"
    return int(r.stdout.strip())


def test_window_scales_and_caps() -> None:
    assert _run("window", "1") == 0, "single task must not jitter"
    assert _run("window", "0") == 0
    assert _run("window", "3") == 9, "small array -> small window (efficiency)"
    assert _run("window", "10") == 30
    assert _run("window", "100") == 300, "100-way -> ~5 min spread"
    assert _run("window", "1000") == 300, "window is capped at 300s"
    prev = 0
    for c in (1, 2, 5, 20, 50, 100, 500):
        w = _run("window", str(c))
        assert w >= prev
        prev = w


def test_delay_within_window() -> None:
    for c in (3, 10, 100, 1000):
        w = _run("window", str(c))
        for _ in range(20):
            d = _run("delay", str(c))
            assert 0 <= d <= w, f"delay {d} out of [0,{w}] for count={c}"
    assert _run("delay", "1") == 0, "single task delay is 0"


def test_backoff_cap_exponential_but_bounded() -> None:
    assert _run("backoff_cap", "1") == 2
    assert _run("backoff_cap", "2") == 4
    assert _run("backoff_cap", "3") == 8
    assert _run("backoff_cap", "4") == 16
    assert _run("backoff_cap", "10") == 60, "backoff is bounded (never unbounded)"
