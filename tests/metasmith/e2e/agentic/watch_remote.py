"""Poll a first-install run's footprint on the cluster, and stop the run when it crosses a file or byte cap."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path

HOST = "nibi"
REMOTE_ROOT = "/scratch/phyberos/msm_e2e"

# One find walk feeds every per-directory row: Lustre stats are the cost, so walk once.
PROBE = r'''
R=$1; MIN=$2
if [ -d "$R" ]; then
  timeout 900 find "$R" -mindepth 1 -printf '%P\t%s\n' 2>/dev/null | awk -F'\t' -v min="$MIN" -v root="$R" '
    { n++; s+=$2; k=split($1, a, "/"); p="";
      for (i=1; i<=k && i<=4; i++) { p = (i==1 ? a[1] : p "/" a[i]); c[p]++; b[p]+=$2; d[p]=i } }
    END { printf "total\t%s\t%d\t%d\t\n", root, n, s;
          for (p in c) if (d[p]==1 || c[p]>=min) printf "dir\t%s\t%d\t%d\t\n", p, c[p], b[p] }'
  echo "walk_exit	$R	0	0	${PIPESTATUS[0]}"
  find "$R" -name '*.sif' -printf 'sif\t%p\t1\t%s\t\n' 2>/dev/null | head -50
else
  echo "total	$R	0	0	absent"
fi
for d in "$HOME/.apptainer" "$HOME/.singularity" "${APPTAINER_CACHEDIR:-}"; do
  [ -n "$d" ] && [ -d "$d" ] && echo "apptainer	$d	$(du -s --inodes "$d" | cut -f1)	$(du -sb "$d" | cut -f1)	"
done
squeue -u "$USER" -h -o '%i|%j|%T|%Z' | while IFS='|' read -r id name st wd; do
  case "$wd" in "$R"*) echo "job	$id	0	0	$name $st";; esac
done
diskusage_report 2>/dev/null | grep -E '^ */(home|scratch)' | sed 's/^ *//' | while read -r l; do echo "quota	-	0	0	$l"; done
for p in $(ps -u "$USER" -o pid=); do
  case "$(readlink /proc/$p/cwd 2>/dev/null)" in "$R"*) echo "proc	$(hostname):$p	0	0	$(tr '\0' ' ' < /proc/$p/cmdline | cut -c1-120)";; esac
done
'''

STOP = r'''
R=$1
for id in $(squeue -u "$USER" -h -o '%i|%Z' | awk -F'|' -v r="$R" 'index($2, r)==1 {print $1}'); do scancel "$id" && echo "scancel $id"; done
# Container steps run with cwd /ws, so match the run dir in cwd or argv, then take every descendant.
roots=$(for p in $(ps -u "$USER" -o pid=); do
  [ "$p" = "$$" ] && continue
  { readlink /proc/$p/cwd; tr '\0' ' ' < /proc/$p/cmdline; } 2>/dev/null | grep -q "$R" && echo $p
done | tr '\n' ' ')
ps -u "$USER" -o pid=,ppid= | awk -v roots=" $roots " '
  { parent[$1]=$2 }
  END { for (p in parent) { q=p; while (q in parent) { if (index(roots, " " q " ")) { print p; break } q=parent[q] } } }' |
  while read -r p; do kill "$p" 2>/dev/null && echo "kill $(hostname):$p"; done
'''


def ssh_script(script: str, *args: str, timeout: float) -> str:
    r = subprocess.run(["ssh", HOST, "bash", "-s", "--", *args], input=script,
                       capture_output=True, text=True, timeout=timeout)
    if r.returncode == 255:
        raise RuntimeError(f"ssh {HOST} failed: {r.stderr.strip()}")
    return r.stdout


def sample(remote_dir: str, min_dir_files: int) -> list[dict]:
    now = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    rows = []
    for line in ssh_script(PROBE, remote_dir, str(min_dir_files), timeout=1200).splitlines():
        kind, path, inodes, nbytes, note = (line.split("\t") + [""] * 5)[:5]
        rows.append(dict(ts=now, kind=kind, path=path, inodes=int(inodes or 0),
                         bytes=int(nbytes or 0), note=note))
    return rows


def stop_run(remote_dir: str, local_dir: Path, reason: str, total: dict) -> None:
    actions = ssh_script(STOP, remote_dir, timeout=120).splitlines()
    # The runner, this watcher and every process the agent detached share one
    # systemd scope, so the record is written before the stop that ends us.
    unit = f"{local_dir.name.replace('run-', 'msm-e2e-run-')}.scope"
    record = dict(ts=dt.datetime.now().astimezone().isoformat(timespec="seconds"),
                  reason=reason, total=total, actions=actions + [f"systemctl stop {unit}"])
    (local_dir / "STOP.json").write_text(json.dumps(record, indent=2))
    print(json.dumps(record), flush=True)
    subprocess.run(["systemctl", "--user", "stop", unit])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", type=int, help="run number N; sets --remote-dir and --local-dir")
    ap.add_argument("--remote-dir")
    ap.add_argument("--local-dir", type=Path)
    ap.add_argument("--max-files", type=int, default=600_000)
    ap.add_argument("--max-gb", type=float, default=600.0)
    ap.add_argument("--interval", type=float, default=300.0)
    ap.add_argument("--min-dir-files", type=int, default=5000, help="report a subdirectory below depth 1 only past this many entries")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args(argv)
    remote_dir = args.remote_dir or f"{REMOTE_ROOT}/run-{args.run}"
    local_dir = args.local_dir or Path.home() / "msm_e2e" / f"run-{args.run}"
    local_dir.mkdir(parents=True, exist_ok=True)
    csv_path = local_dir / "watch.csv"
    fields = ["ts", "kind", "path", "inodes", "bytes", "note"]
    new = not csv_path.exists()
    peak = 0

    while True:
        try:
            rows = sample(remote_dir, args.min_dir_files)
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            rows = [dict(ts=dt.datetime.now().astimezone().isoformat(timespec="seconds"),
                         kind="error", path=remote_dir, inodes=0, bytes=0, note=str(e)[:200])]
        with csv_path.open("a", newline="") as fp:
            w = csv.DictWriter(fp, fieldnames=fields)
            if new:
                w.writeheader()
                new = False
            w.writerows(rows)
        total = next((r for r in rows if r["kind"] == "total"), None)
        if total:
            peak = max(peak, total["inodes"])
            jobs = sum(r["kind"] == "job" for r in rows)
            print(f"{total['ts']} files={total['inodes']} gb={total['bytes'] / 1e9:.1f} peak={peak} jobs={jobs}", flush=True)
            if total["inodes"] >= args.max_files or total["bytes"] >= args.max_gb * 1e9:
                stop_run(remote_dir, local_dir, f"cap crossed: files {total['inodes']} >= {args.max_files} "
                         f"or bytes {total['bytes']} >= {args.max_gb} GB", total)
                return 2
        else:
            print(f"{rows[0]['ts']} {rows[0]['kind']}: {rows[0]['note']}", flush=True)
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
