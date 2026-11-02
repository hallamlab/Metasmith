"""Run each SMETANA protocol far enough to catch name errors: a stub context, no tool, no cluster.

A bare import would not have caught the `out` shadowing that killed every wave-5 task in ~10 s --
UnboundLocalError only fires when protocol() runs. This walks each real function body with
ExecWithEnv stubbed out, and checks what the body actually produced.

Run from the repository root:  PYTHONPATH=src python research/metasmith_benchmark/drivers/protocol_smoke.py
"""
import importlib.util
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Absolute, because each protocol runs inside its own temp cwd: a relative path resolves against the
# repo root before the chdir and against the temp dir after it.
REPO = Path(__file__).resolve().parents[3]
BENCH = REPO / "research/metasmith_benchmark/library/transforms/bench"
MEDIA_DB = REPO / "research/metasmith_benchmark/drivers/refs/metagem_media_db.tsv"
sys.path.insert(0, str(REPO / "src"))

MEDIA = ["M1", "M2", "M3", "M4", "M5", "M7", "M8", "M9", "M10", "M11",
         "M13", "M14", "M15A", "M15B", "M16"]
DETAILED_HEADER = "community\tmedium\treceiver\tdonor\tcompound\tscs\tmus\tmps\tsmetana"


class StubUnit:
    def __init__(self, p):
        self.local = Path(p)
        self.container = Path("/ws") / Path(p).name


class StubCtx:
    """One stub for every lane. `inputs` maps a slot index to a path; group_files feeds InputGroup."""

    def __init__(self, tmp, inputs=None, group_files=(), on_exec=None, expect_in_cmd=()):
        self.params = {"cpus": 16}
        self.tmp = Path(tmp)
        self.execs = 0
        self._inputs = inputs or {}
        self._group = list(group_files)
        self._on_exec = on_exec
        self._expect = list(expect_in_cmd)
        self.outputs = []

    def Input(self, slot):
        # Slots are distinguished by identity, so the seed table is keyed by the slot object.
        p = self._inputs.get(id(slot))
        if p is None:
            p = self.tmp / "in.txt"
            p.write_text("stub\n")
        return StubUnit(p)

    def Output(self, slot, i=None):
        p = self.tmp / ("out.tsv" if i is None else f"out_{i}.tsv")
        self.outputs.append(p)
        return StubUnit(p)

    def InputGroup(self, slot):
        for p in self._group:
            yield StubUnit(p)

    def ExecWithEnv(self, **kw):
        self.execs += 1
        cmd = kw.get("cmd", "")
        for needle in self._expect:
            assert needle in cmd, f"{needle!r} did not render into the command"
        if self._on_exec:
            self._on_exec(cmd)

    def LocalShell(self, cmd):
        subprocess.run(cmd, shell=True, check=True)


def load(name):
    spec = importlib.util.spec_from_file_location(name, BENCH / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run(name, build_ctx, check):
    """Execute one protocol in its own temp cwd and report what it produced."""
    try:
        mod = load(name)
    except Exception as e:
        print(f"{name}: IMPORT FAILED {type(e).__name__}: {e}")
        return False
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        cwd = os.getcwd()
        os.chdir(td)
        try:
            ctx = build_ctx(tmp, mod)
            res = mod.protocol(ctx)
            assert res.success, "protocol reported success=False"
            print(f"{name}: PROTOCOL OK  {check(ctx, mod)}")
            return True
        except Exception as e:
            print(f"{name}: PROTOCOL FAILED {type(e).__name__}: {e}")
            return False
        finally:
            os.chdir(cwd)


def media_rows(medium):
    """The real metaGEM rows for one medium, header included -- what the split writes."""
    lines = MEDIA_DB.read_text().splitlines()
    body = [l for l in lines[1:] if l.split("\t", 1)[0] == medium]
    assert body, f"the real media db has no rows for {medium}"
    return lines[0] + "\n" + "\n".join(body) + "\n"


# ---- stage 1: split -------------------------------------------------------
def _split_ctx(tmp, mod):
    db = tmp / "media_db.tsv"
    db.write_text(MEDIA_DB.read_text())
    return StubCtx(tmp, inputs={id(mod.media): db})


def _split_check(ctx, mod):
    written = [p for p in ctx.outputs if p.exists()]
    assert len(written) == 15, f"{len(written)} media written, expected 15"
    seen = []
    for p in written:
        lines = p.read_text().splitlines()
        assert lines[0].startswith("medium\t"), f"{p.name} lost its header"
        ids = {l.split("\t", 1)[0] for l in lines[1:] if l.strip()}
        assert len(ids) == 1, f"{p.name} mixes media {sorted(ids)}"
        seen.append(ids.pop())
    assert sorted(seen) == sorted(MEDIA), f"split wrote {sorted(seen)}"
    return f"15 single-medium tables, ids match metaGEM's fifteen"


# ---- stage 2: one medium --------------------------------------------------
def _medium_ctx_for(solver):
    def build(tmp, mod):
        med = tmp / "medium.tsv"
        med.write_text(media_rows("M3"))
        gems = []
        for n in ("a", "b"):
            p = tmp / f"{n}.xml"
            p.write_text("<sbml/>")
            gems.append(p)

        def on_exec(cmd):
            Path("out_detailed.tsv").write_text(
                DETAILED_HEADER + "\n" + "c\tM3\tr\td\tcpd\t1\t1\t1\t1\n")

        expect = ["-m M3", f"--solver {solver}", "--detailed", "test -f out_detailed.tsv"]
        if solver == "cplex":
            # `set -u` aborts on an unset PYTHONPATH, so the default-expansion form is load-bearing.
            expect.append("${PYTHONPATH:-}")
        return StubCtx(tmp, inputs={id(mod.medium): med}, group_files=gems, on_exec=on_exec,
                       expect_in_cmd=expect)
    return build


_medium_ctx = _medium_ctx_for("scip")


def _medium_check(ctx, mod):
    out = ctx.outputs[-1]
    rows = out.read_text().splitlines()
    assert len(rows) == 2, f"expected header + 1 row, got {len(rows)}"
    assert rows[1].split("\t")[1] == "M3", "the medium column is not M3"
    return f"execs={ctx.execs}  rows={len(rows) - 1}  medium read from table content"


# ---- stage 3: merge -------------------------------------------------------
def _merge_files(tmp, media, empty=()):
    out = []
    for m in media:
        p = tmp / f"t_{m}.tsv"
        if m in empty:
            p.write_text(DETAILED_HEADER + "\n")
        else:
            p.write_text(DETAILED_HEADER + "\n" + f"c\t{m}\tr\td\tcpd\t1\t1\t1\t1\n")
        out.append(p)
    return out


def _merge_ctx(tmp, mod):
    # Reversed, because grouped slots arrive in arbitrary order and the merge must order them itself.
    return StubCtx(tmp, group_files=_merge_files(tmp, list(reversed(MEDIA)), empty={"M5"}))


def _merge_check(ctx, mod):
    out = ctx.outputs[-1]
    lines = out.read_text().splitlines()
    assert lines[0] == DETAILED_HEADER, "merged table lost its header"
    body = lines[1:]
    assert len(body) == 14, f"expected 14 rows (M5 is zero-score), got {len(body)}"
    order = [l.split("\t")[1] for l in body]
    assert order == sorted(order), f"rows are not ordered by medium: {order}"
    assert len(set(order)) == 14, "a medium appears twice"
    return f"15 media -> 1 header + {len(body)} rows, M5 header-only, deterministic order"


def _merge_short_ctx(tmp, mod):
    return StubCtx(tmp, group_files=_merge_files(tmp, MEDIA[:-1]))


# ---- both lanes -----------------------------------------------------------
# E5 scores on SCIP and roots at an assembly; E4 scores on CPLEX and roots at a metaGEM sample. The
# protocol bodies are otherwise identical, so both trios run through the same three checks.
TRIOS = [("smetana", "scip"), ("smetana_cplex", "cplex")]

if __name__ == "__main__":
    ok = []
    for prefix, solver in TRIOS:
        print(f"-- {prefix} lane ({solver}) --")
        ok.append(run(f"{prefix}_split_media", _split_ctx, _split_check))
        ok.append(run(f"{prefix}_medium", _medium_ctx_for(solver), _medium_check))
        ok.append(run(f"{prefix}_merge", _merge_ctx, _merge_check))

    # A short group must FAIL loudly: retry-then-ignore drops a medium's task and the lane still
    # reports complete, so a merge that quietly emitted 14 media is the silent scientific error.
    print("-- negative control: 14 of 15 media --")
    short_failed = not run("smetana_merge", _merge_short_ctx, lambda c, m: "")
    print(f"short group rejected: {short_failed}")
    ok.append(short_failed)

    print(f"\n{sum(ok)}/{len(ok)} checks passed")
    sys.exit(0 if all(ok) else 1)
