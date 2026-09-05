"""The ratchet harness: build, gate, measure, rank, log.

`score` and `submit` are thin wrappers over this. Everything a worker needs to do
happens here so that a worker never touches cargo, docker, git or pytest.

Two gates, in this order, because the first is cheap and catches the mistake that
would invalidate every number after it:

  default-path -- with MSM_SOLVER_POLICY unset the reply must be byte-identical to
      the recorded baseline on every payload. A change that moves the shipped rule
      is not a candidate policy, it is a regression.
  witness -- every complete plan the candidate emits is adjudicated by the
      REFERENCE binary's `check`, not the candidate's own. A worker that breaks the
      witness must not also get to be its own judge.
"""
import argparse, json, os, subprocess, sys, time
from pathlib import Path

RATCHET = Path(os.environ.get("RATCHET", "/home/tony/.claude/jobs/0f8a7346/tmp/ratchet"))
PAYLOADS = RATCHET / "payloads"
REF_BIN = RATCHET / "ref" / "msm_solver"
LOG = RATCHET / "log"
CARGO_ENV = {
    "CARGO_PROFILE_RELEASE_LTO": "false",
    "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "16",
    "CARGO_PROFILE_RELEASE_STRIP": "none",
    "CARGO_PROFILE_RELEASE_INCREMENTAL": "true",
}


def worktree():
    return Path(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                               capture_output=True, text=True, check=True).stdout.strip())


def worker_name(root):
    return root.name


def build(root, target_dir):
    """Compile msm_solver. Returns (binary_path, seconds) or raises SystemExit."""
    env = dict(os.environ)
    env.update(CARGO_ENV)
    env["CARGO_TARGET_DIR"] = str(target_dir)
    t0 = time.monotonic()
    p = subprocess.run(["mamba", "run", "-n", "msmrust", "cargo", "build", "--release"],
                       cwd=root / "src" / "workflow_solver",
                       capture_output=True, text=True, env=env)
    dt = time.monotonic() - t0
    if p.returncode != 0:
        print("BUILD FAILED\n", file=sys.stderr)
        # Verbatim. A summarised compiler error costs the worker a round trip.
        sys.stderr.write(p.stderr)
        raise SystemExit(2)
    return target_dir / "release" / "msm_solver", dt


def run_set(binary, which, policy, puct, out, replies):
    cmd = [sys.executable, str(Path(__file__).parent / "runner.py"),
           "--bin", str(binary), "--payloads", str(PAYLOADS), "--set", which,
           "--out", str(out), "--replies", str(replies)]
    if policy:
        cmd += ["--policy", policy]
    if puct:
        cmd += ["--puct", puct]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        print("RUN FAILED\n" + p.stderr, file=sys.stderr)
        raise SystemExit(3)
    return json.loads(Path(out).read_text())["rows"]


def gate_default(rows, base):
    bad = [k for k in base if k in rows and rows[k]["digest"] != base[k]["digest"]]
    return bad


def gate_witness(rows, replies, which):
    """Adjudicate every complete plan with the reference binary's witness."""
    manifest = json.loads((PAYLOADS / "manifest.json").read_text())[which]
    files = {e["name"]: e for e in manifest}
    bad = []
    for name, row in rows.items():
        if not row.get("complete"):
            continue
        rep = Path(replies) / f"{name}.reply.json"
        if not rep.exists():
            bad.append((name, "no reply"))
            continue
        ent = files[name]
        req = json.loads((PAYLOADS / ent["file"]).read_text())
        req["max_iter"] = ent["iter"]
        req["max_refine"] = ent["refine"]
        blob = json.dumps({"request": req, "reply": json.loads(rep.read_text())}).encode()
        p = subprocess.run([str(REF_BIN), "check"], input=blob, capture_output=True)
        if p.returncode != 0:
            bad.append((name, p.stderr.decode()[-200:]))
            continue
        v = json.loads(p.stdout)
        if not v.get("ok"):
            cl = ",".join(x["clause"] for x in v.get("violations", [])[:3])
            bad.append((name, f"witness rejected: {cl}"))
    return bad


def summarise(rows, ref=None):
    """Solved count, plus steps/iterations over cases solved by BOTH arms.

    Comparing totals over all cases would let an arm look good by failing a case
    whose plan is long, so the quality columns are restricted to the joint set.
    """
    solved = {k for k, v in rows.items() if v.get("complete")}
    joint = solved if ref is None else solved & {k for k, v in ref.items() if v.get("complete")}
    return {
        "solved": len(solved),
        "total": len(rows),
        "steps": sum(rows[k]["steps"] for k in joint),
        "iters": sum(rows[k]["iterations"] or 0 for k in joint),
        "joint": len(joint),
        "wall": round(sum(v["wall"] for v in rows.values()), 2),
    }


def verdict(cur, base):
    """WIN / TIE / LOSS. Solved is a gate; steps then iterations break the tie."""
    c, b = cur["solved"], base["solved"]
    if c != b:
        return "WIN" if c > b else "LOSS"
    if cur["steps"] != base["steps"]:
        return "WIN" if cur["steps"] < base["steps"] else "LOSS"
    if cur["iters"] != base["iters"]:
        return "WIN" if cur["iters"] < base["iters"] else "LOSS"
    return "TIE"


def table(cur_rows, base_rows, head_rows):
    out = []
    cs = summarise(cur_rows, base_rows)
    bs = summarise(base_rows, base_rows)
    out.append(f"{'':14s} {'solved':>12s} {'steps':>10s} {'iters':>10s}   (steps/iters over cases both solved)")
    out.append(f"{'baseline':14s} {bs['solved']:>7d}/{bs['total']:<4d} {bs['steps']:>10d} {bs['iters']:>10d}")
    out.append(f"{'candidate':14s} {cs['solved']:>7d}/{cs['total']:<4d} {cs['steps']:>10d} {cs['iters']:>10d}"
               f"   {verdict(cs, bs)} vs baseline")
    if head_rows:
        hs = summarise(head_rows, base_rows)
        cs_h = summarise(cur_rows, head_rows)
        hs_h = summarise(head_rows, head_rows)
        out.append(f"{'ratchet head':14s} {hs['solved']:>7d}/{hs['total']:<4d} {hs['steps']:>10d} {hs['iters']:>10d}")
        out.append(f"{'':14s} {'':12s} {'':10s} {'':10s}   {verdict(cs_h, hs_h)} vs head")
    return "\n".join(out)


def movers(cur_rows, ref_rows, limit=12):
    """Which payloads actually moved. A worker cannot steer without this."""
    lines = []
    for k in sorted(cur_rows):
        c, r = cur_rows[k], ref_rows.get(k)
        if not r or c["digest"] == r["digest"]:
            continue
        if c.get("complete") and not r.get("complete"):
            lines.append(f"  + {k:16s} SOLVED (was unsolved)  steps={c['steps']} its={c['iterations']}")
        elif r.get("complete") and not c.get("complete"):
            lines.append(f"  - {k:16s} LOST (was solved at {r['steps']} steps)")
        elif c.get("complete"):
            ds, di = c["steps"] - r["steps"], (c["iterations"] or 0) - (r["iterations"] or 0)
            if ds or di:
                lines.append(f"    {k:16s} steps {r['steps']:>3d} -> {c['steps']:<3d} ({ds:+d})   its {r['iterations']:>4d} -> {c['iterations']:<4d} ({di:+d})")
    n = len(lines)
    return "\n".join(lines[:limit]) + (f"\n  ... and {n-limit} more" if n > limit else ""), n


def log_append(worker, record):
    LOG.mkdir(parents=True, exist_ok=True)
    import fcntl
    with open(LOG / "findings.md", "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        fh.write(record["md"])
        fh.flush()
        fcntl.flock(fh, fcntl.LOCK_UN)
    with open(LOG / f"{worker}.jsonl", "a") as fh:
        fh.write(json.dumps({k: v for k, v in record.items() if k != "md"}) + "\n")
