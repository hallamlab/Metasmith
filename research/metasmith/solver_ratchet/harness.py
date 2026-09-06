"""The ratchet harness: build, gate, measure, rank, log.

`score` and `submit` are thin wrappers over this. Everything a worker needs to do
happens here so that a worker never touches cargo, docker, git or pytest.

Two gates, in this order, because the first is cheap and catches the mistake that
would invalidate every number after it:

  adjudicator -- the two witness crates must be byte-identical to what the Lean
      proof was last run against. A round that changes the judge has not measured
      anything. This replaced a default-path gate that became vacuous when PUCT
      stopped being opt-in.
  witness -- every complete plan the candidate emits is adjudicated by the
      REFERENCE binary's `check`, not the candidate's own. A worker that breaks the
      witness must not also get to be its own judge.
"""
import argparse, hashlib, json, os, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
#: Everything the harness reads is committed beside it, so a later session gets a
#: working harness from the checkout alone. The original run kept these in a job
#: directory, which is why the first attempt to re-run it found no payloads.
RATCHET = Path(os.environ.get("RATCHET", HERE))
PAYLOADS = RATCHET / "payloads"
RESULTS = RATCHET / "results"
LOG = Path(os.environ.get("RATCHET_LOG", RESULTS))

#: The binary that adjudicates. It used to be a pristine pre-change build, so a
#: candidate could not be its own judge. That is now `witness_unchanged` instead:
#: a selection round never touches the witness crates, and checking that they are
#: byte-identical to what was proved is a stronger guarantee than a binary whose
#: provenance nobody can see. Point this at another build to override.
REF_BIN = Path(os.environ.get(
    "RATCHET_REF_BIN",
    HERE.parents[2] / "src" / "metasmith" / "engine" / "msm_solver.x86_64-linux",
))

#: sha256 over the two witness crates' sources, recorded when the proof was last
#: adjudicated by `docker/solver_witness/dev.sh --lean-check`.
WITNESS_DIGEST_FILE = RESULTS / "witness-digest.txt"

#: Build and reply scratch. Outside the checkout on purpose -- an earlier version
#: defaulted it beside the harness and `git add -A` swept 81 reply files in.
SCRATCH = Path(os.environ.get(
    "RATCHET_SCRATCH", Path(os.environ.get("TMPDIR", "/tmp")) / "msm-ratchet"))
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


def witness_digest(root):
    """sha256 over every source file of the two witness crates, in path order."""
    h = hashlib.sha256()
    for crate in ("solver_witness", "solver_witness_audit"):
        base = root / "src" / crate
        for f in sorted(base.rglob("*")):
            if not f.is_file() or "target" in f.parts:
                continue
            h.update(str(f.relative_to(base)).encode())
            h.update(f.read_bytes())
    return h.hexdigest()


def witness_unchanged(root):
    """Has the adjudicator moved? Returns None when it has not, else a message.

    This replaced the old first gate. That gate said "with the policy env var
    unset the replies are byte-identical to the baseline", which was the right
    shape while PUCT was opt-in and became vacuous the moment it shipped as the
    only rule. What still needs guarding is that the thing doing the judging is
    the thing that was proved.
    """
    if not WITNESS_DIGEST_FILE.exists():
        return f"no recorded witness digest at {WITNESS_DIGEST_FILE}"
    want = WITNESS_DIGEST_FILE.read_text().split()[0]
    got = witness_digest(root)
    if got != want:
        return (f"the witness crates changed ({got[:12]} vs recorded {want[:12]})."
                " Re-run docker/solver_witness/dev.sh --lean-check and record the"
                " new digest before trusting any verdict below.")
    return None


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


def family(name):
    """Real workflows versus generated stress cases.

    `ladder-*` are rungs of a real metagenomics workflow from 3 to 68 targets and
    the four named arms are real metagenomics plans; `sink-*` are generated
    pathologies. They are not interchangeable, and round 1 found a config that
    solves one more generated case by lengthening every real plan. Keeping the
    columns apart is what makes that visible instead of averaged away.
    """
    if name.startswith("sink"):
        return "sink"
    return "real"


def summarise(rows, ref=None):
    """Solved count, plus steps/iterations over cases solved by BOTH arms.

    Comparing totals over all cases would let an arm look good by failing a case
    whose plan is long, so the quality columns are restricted to the joint set.
    """
    solved = {k for k, v in rows.items() if v.get("complete")}
    joint = solved if ref is None else solved & {k for k, v in ref.items() if v.get("complete")}
    real = [k for k in joint if family(k) == "real"]
    real_all = [k for k in rows if family(k) == "real"]
    return {
        "solved": len(solved),
        "total": len(rows),
        "steps": sum(rows[k]["steps"] for k in joint),
        "iters": sum(rows[k]["iterations"] or 0 for k in joint),
        "joint": len(joint),
        "real_solved": sum(1 for k in real_all if rows[k].get("complete")),
        "real_total": len(real_all),
        "real_steps": sum(rows[k]["steps"] for k in real),
        "sink_solved": len(solved) - sum(1 for k in real_all if rows[k].get("complete")),
        "wall": round(sum(v["wall"] for v in rows.values()), 2),
    }


def verdict(cur, base):
    """WIN / TIE / LOSS.

    A regression on real-workflow plan length is a LOSS however many generated
    cases it buys. Round 1 measured exactly that trade -- `epsilon_milli=200`
    solves one more `sink` case and adds 9 steps to the ladder and 6 to the
    metagenomics arms -- and the real workflows are what ships. Solved count is
    the next key, then total steps, then iterations.
    """
    if cur["real_solved"] < base["real_solved"]:
        return "LOSS"
    if cur["real_steps"] > base["real_steps"]:
        return "LOSS"
    if cur["real_steps"] < base["real_steps"]:
        return "WIN"
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
    def row(tag, d, suffix=""):
        return (f"{tag:14s} {d['solved']:>3d}/{d['total']:<3d} {d['real_solved']:>3d}/{d['real_total']:<3d}"
                f" {d['real_steps']:>10d} {d['steps']:>8d} {d['iters']:>9d}{suffix}")
    out.append(f"{'':14s} {'solved':>7s} {'real':>7s} {'REAL steps':>10s} {'steps':>8s} {'iters':>9s}")
    out.append(row("baseline", bs))
    out.append(row("candidate", cs, f"   {verdict(cs, bs)} vs baseline"))
    if head_rows:
        hs = summarise(head_rows, base_rows)
        cs_h = summarise(cur_rows, head_rows)
        hs_h = summarise(head_rows, head_rows)
        out.append(row("ratchet head", hs))
        out.append(f"{'':14s} {'':7s} {'':7s} {'':10s} {'':8s} {'':9s}   {verdict(cs_h, hs_h)} vs head")
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
