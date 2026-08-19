from __future__ import annotations
import os
import sys
import threading
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_HERE))

from metasmith.coms.terminals import LiveShell  # noqa: E402
from tmux_shell import TmuxShell  # noqa: E402
import scenarios  # noqa: E402


PASS, FAIL, HANG, ERROR, SKIP = "PASS", "FAIL", "HANG", "ERROR", "SKIP"


def run_one(fn, make_shell, host, timeout):
    result = {}

    def worker():
        t0 = time.monotonic()
        try:
            ok, detail = fn(make_shell, host)
            result["status"] = PASS if ok else FAIL
            result["detail"] = detail
        except Exception as e:
            result["status"] = ERROR
            result["detail"] = f"{type(e).__name__}: {e}"
        finally:
            result["sec"] = time.monotonic() - t0

    th = threading.Thread(target=worker, daemon=True)
    t0 = time.monotonic()
    th.start()
    th.join(timeout)
    if th.is_alive():
        return HANG, f"no return within {timeout:.0f}s", time.monotonic() - t0
    return result.get("status", ERROR), result.get("detail", "no result"), result.get("sec", 0.0)


def main():
    host = os.environ.get("LIVESHELL_REMOTE_HOST", "").strip()
    shells = {
        "LiveShell": lambda: LiveShell(),
        "TmuxShell": lambda: TmuxShell(),
    }

    rows = []
    for sc in scenarios.ALL:
        rowcells = {}
        for shell_name, factory in shells.items():
            if sc.needs_ssh and not host:
                rowcells[shell_name] = (SKIP, "LIVESHELL_REMOTE_HOST unset", 0.0)
                continue
            status, detail, sec = run_one(sc.fn, factory, host or None, sc.timeout)
            rowcells[shell_name] = (status, detail, sec)
            print(f"[{shell_name:9}] {sc.name:28} {status:5} {sec:6.2f}s  {detail}")
        rows.append((sc, rowcells))
        print()

    write_results(rows, host)


def _cell(c):
    status, detail, sec = c
    return status, f"{status} ({sec:.2f}s)" if status not in (SKIP,) else status, detail


def write_results(rows, host):
    out = _HERE / "RESULTS.generated.md"
    lines = []
    lines.append("# LiveShell vs TmuxShell — head-to-head results\n")
    lines.append(f"_ssh host: `{host or '(none — ssh scenarios skipped)'}`_  ")
    lines.append(f"_tmux: `{_tmux_version()}`_\n")

    def tally(shell_name):
        c = {PASS: 0, FAIL: 0, HANG: 0, ERROR: 0, SKIP: 0}
        for _, cells in rows:
            c[cells[shell_name][0]] += 1
        return c

    lt, tt = tally("LiveShell"), tally("TmuxShell")
    lines.append("## Tally\n")
    lines.append("| Shell | PASS | FAIL | HANG | ERROR | SKIP |")
    lines.append("|---|---|---|---|---|---|")
    lines.append(f"| LiveShell | {lt[PASS]} | {lt[FAIL]} | {lt[HANG]} | {lt[ERROR]} | {lt[SKIP]} |")
    lines.append(f"| TmuxShell | {tt[PASS]} | {tt[FAIL]} | {tt[HANG]} | {tt[ERROR]} | {tt[SKIP]} |")
    lines.append("")

    lines.append("## Per-scenario\n")
    lines.append("| Scenario | ssh? | LiveShell | TmuxShell |")
    lines.append("|---|---|---|---|")
    for sc, cells in rows:
        l = cells["LiveShell"]; t = cells["TmuxShell"]
        ls = l[0] if l[0] == SKIP else f"{l[0]} ({l[2]:.2f}s)"
        ts = t[0] if t[0] == SKIP else f"{t[0]} ({t[2]:.2f}s)"
        lines.append(f"| {sc.name} | {'yes' if sc.needs_ssh else ''} | {ls} | {ts} |")
    lines.append("")

    lines.append("## Failure / hang details\n")
    any_detail = False
    for sc, cells in rows:
        for shell_name in ("LiveShell", "TmuxShell"):
            status, detail, sec = cells[shell_name]
            if status in (FAIL, HANG, ERROR):
                any_detail = True
                lines.append(f"- **{sc.name}** / {shell_name}: {status} — {detail}")
    if not any_detail:
        lines.append("_None — all executed scenarios passed._")
    lines.append("")

    out.write_text("\n".join(lines))
    print(f"\nwrote {out}")


def _tmux_version():
    import subprocess
    try:
        return subprocess.run(["tmux", "-V"], capture_output=True, text=True).stdout.strip()
    except Exception:
        return "unknown"


if __name__ == "__main__":
    main()
