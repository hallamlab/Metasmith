#!/usr/bin/env python3
"""Race fir against sockeye per ORF set; first finisher wins, the other is cancelled.

Both sites run the same nine-step graph over the same seven ORF sets. Whichever driver
exits 0 first for a set has already verified its targets landed and pulled the results
back, so that exit IS the win condition -- there is nothing left for the loser to
contribute. Cancelling it frees a GPU reservation that the remaining sets are queued
behind, which is the whole point of racing on a cluster whose GPU partition is the
bottleneck.

A non-zero exit is not a loss: the other site keeps running, since a failure on one
site says nothing about the other.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path("/home/tony/agentic_workspace/projects/metasmith/fabfos/bench-eydallin")
LOGDIR = Path(os.environ.get("FABFOS_RACE_LOGS", REPO / "data" / "scratch" / "race" / "logs"))
EXITS = LOGDIR / "_exits.txt"

sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))
from _driver import fir_agent, sockeye_agent                          # noqa: E402

SETS = {
    "e_coli_dh1": "NC_017638.1",
    "e_coli_k12": "NC_000913.3",
    "e_coli_dh10b": "NC_010473.1",
    "e_coli_epi300": "CP189566.1",
    "e_coli_bw25113": "CP193896.1",
    "e_coli_w3110": "CP165600.1",
    "eydallin_clones": "eydallin_clones",
}

WORK = {
    "fir": lambda stem: REPO / "data" / "scratch" / f"clone_gpr_fir_{stem}",
    "sockeye": lambda stem: REPO / "data" / "scratch" / f"race_sockeye_{stem}",
}
AGENT = {"fir": fir_agent, "sockeye": sockeye_agent}
OTHER = {"fir": "sockeye", "sockeye": "fir"}


def say(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def read_exits() -> dict[tuple[str, str], int]:
    """(name, site) -> exit code, from the line each driver appends when it returns."""
    out: dict[tuple[str, str], int] = {}
    if not EXITS.exists():
        return out
    for line in EXITS.read_text().splitlines():
        parts = line.split()
        if len(parts) != 3 or parts[1] != "exited":
            continue
        tag, code = parts[0], int(parts[2])
        name, site = (tag.split("__", 1) + ["fir"])[:2]
        if name in SETS:
            out[(name, site)] = code
    return out


def kill_local(name: str, site: str) -> None:
    stem = SETS[name]
    pattern = f"clone_gpr_on_hpc.py .*{stem}\\.faa .*--site {site}"
    subprocess.run(["pkill", "-f", pattern], capture_output=True)


def cancel_remote(name: str, site: str) -> None:
    key_file = WORK[site](SETS[name]) / "RUN_KEY"
    if not key_file.exists():
        say(f"    {site}/{name}: no RUN_KEY, nothing staged to cancel")
        return
    key = key_file.read_text().strip()
    agent = AGENT[site]()
    try:
        agent.CancelWorkflow(key)
        say(f"    {site}/{name}: cancelled {key}")
    except Exception as e:                                            # noqa: BLE001
        say(f"    {site}/{name}: cancel({key}) raised {e!r}")
    try:
        agent.ReapWorkflow(key)
        say(f"    {site}/{name}: reaped {key}")
    except Exception as e:                                            # noqa: BLE001
        say(f"    {site}/{name}: reap({key}) raised {e!r}")


def main() -> int:
    settled: set[str] = set()
    say(f"racing {len(SETS)} ORF sets, fir vs sockeye")
    while len(settled) < len(SETS):
        exits = read_exits()
        for name in SETS:
            if name in settled:
                continue
            winner = next((s for s in ("fir", "sockeye")
                           if exits.get((name, s)) == 0), None)
            if winner:
                settled.add(name)
                loser = OTHER[winner]
                say(f"{name}: {winner} WON -- cancelling {loser}")
                kill_local(name, loser)
                cancel_remote(name, loser)
                continue
            if all(exits.get((name, s), 0) not in (0, None) for s in ("fir", "sockeye")) \
                    and (name, "fir") in exits and (name, "sockeye") in exits:
                settled.add(name)
                say(f"{name}: BOTH SITES FAILED "
                    f"(fir={exits[(name, 'fir')]}, sockeye={exits[(name, 'sockeye')]})")
        time.sleep(60)
    say("all sets settled")
    return 0


if __name__ == "__main__":
    sys.exit(main())
