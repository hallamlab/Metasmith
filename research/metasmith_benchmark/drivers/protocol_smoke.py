"""Run each SMETANA protocol far enough to catch name errors: a stub context, no tool, no cluster.

A bare import would not have caught the `out` shadowing -- UnboundLocalError only fires when
protocol() runs. This walks the real function body with ExecWithEnv stubbed out.
"""
import sys, types, importlib.util
from pathlib import Path
import tempfile, os

BENCH = Path("research/metasmith_benchmark/library/transforms/bench")
sys.path.insert(0, "src")

class StubUnit:
    def __init__(self, p): self.local = Path(p); self.container = Path("/ws") / Path(p).name
class StubCtx:
    def __init__(self, tmp):
        self.params = {"cpus": 16}
        self.tmp = tmp
        self.execs = 0
    def Input(self, slot): return StubUnit(self.tmp / "in.txt")
    def Output(self, slot): return StubUnit(self.tmp / "out.tsv")
    def InputGroup(self, slot):
        for n in ("a", "b"):
            p = self.tmp / f"{n}.xml"; p.write_text("<sbml/>"); yield StubUnit(p)
    def ExecWithEnv(self, **kw):
        self.execs += 1
        cmd = kw.get("cmd", "")
        assert "for m in M1 M2" in cmd, "media loop did not render"
        assert 'wait "$p"' in cmd, "wait block did not render"
        # emulate the tool writing one table per medium
        for m in ("M1", "M2"):
            (Path.cwd() / f"m_{m}_detailed.tsv").write_text("community\tmedium\tx\nc\t%s\t1\n" % m)

for name in ("smetana", "smetana_cplex"):
    spec = importlib.util.spec_from_file_location(name, BENCH / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        print(f"{name}: IMPORT FAILED {type(e).__name__}: {e}"); continue
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td); cwd = os.getcwd(); os.chdir(td)
        try:
            ctx = StubCtx(tmp)
            res = mod.protocol(ctx)
            rows = (tmp / "out.tsv").read_text().splitlines()
            print(f"{name}: PROTOCOL OK  execs={ctx.execs}  stacked_lines={len(rows)}  header={rows[0][:24]!r}")
        except Exception as e:
            print(f"{name}: PROTOCOL FAILED {type(e).__name__}: {e}")
        finally:
            os.chdir(cwd)
