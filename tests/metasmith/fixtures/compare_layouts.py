#!/usr/bin/env python3
import random
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[3] / "src"))
sys.path.insert(0, str(HERE.parents[3]))

import metasmith.models.dag_layout as dag_layout  # noqa: E402
from metasmith.models.dag_colour import SCHEMES  # noqa: E402
from metasmith.models.dag_layout import measure  # noqa: E402
from metasmith.models.dag_renderer import LabelMode  # noqa: E402
from tests.metasmith.fixtures import load_dag  # noqa: E402

_DIMS = re.compile(r'width="(\d+)" height="(\d+)"')

VARIANTS = {
    "column": dict(label_mode=LabelMode.COLUMN),
    "beside": dict(label_mode=LabelMode.BESIDE),
}


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "dag_comparison")
    out.mkdir(parents=True, exist_ok=True)

    rows = []
    for name, kwargs in VARIANTS.items():
        r = load_dag(**kwargs)
        svg = r.to_svg()
        (out / f"{name}.svg").write_text(svg)
        w, h = (int(v) for v in _DIMS.search(svg).groups())
        lay = r.layout()
        rows.append((name, w, h, lay.width, lay.height,
                     len(r.to_text().rstrip("\n").splitlines())))
        try:
            r.render(out / f"{name}.png")
        except RuntimeError as e:
            print(f"  (no raster for {name}: {e})", file=sys.stderr)

    for scheme in SCHEMES:
        (out / f"colour-{scheme}.svg").write_text(load_dag(colour=scheme).to_svg())

    (out / "rails.txt").write_text(load_dag().to_text())

    head = f"{'variant':<10} {'svg w':>7} {'svg h':>7} {'lanes':>6} {'rows':>6} {'text':>6}"
    print(head)
    print("-" * len(head))
    for name, w, h, lanes, nrows, lines in rows:
        print(f"{name:<10} {w:>7} {h:>7} {lanes:>6} {nrows:>6} {lines:>6}")
    narrow = min(rows, key=lambda r: r[1])
    wide = max(rows, key=lambda r: r[1])
    print(f"\n{narrow[0]} is {wide[1] / narrow[1]:.1f}x narrower than {wide[0]}")
    print(f"cost: {measure(load_dag().layout())}")
    print(f"wrote {out}/ (+{len(SCHEMES)} colour schemes)")
    return 0


def _random_dag(rng: random.Random, n: int):
    names = [f"{i} n" for i in range(n)]
    edges = []
    for i in range(1, n):
        parents = rng.sample(range(i), min(i, rng.choice([1, 1, 1, 2, 2, 3])))
        edges += [(names[j], names[i]) for j in parents]
    return {x: rng.choice(["T", "D"]) for x in names}, edges


def corpus(count: int) -> int:
    real = dag_layout._motifs
    print(f"{'motifs':<8} {'rail':>8} {'lanes':>7} {'crossings':>10}"
          f" {'congruent':>11} {'graphs w/ a class':>18} {'secs':>7}")
    for label, off in (("off", True), ("on", False)):
        dag_layout._motifs = (lambda *a, **k: ()) if off else real
        total = dict(rail=0, lanes=0, cross=0, cong=0, reps=0, some=0)
        start = time.perf_counter()
        for seed in range(count):
            rng = random.Random(seed)
            kinds, edges = _random_dag(rng, rng.randint(12, 60))
            m = measure(dag_layout.layout(kinds, edges))
            total["rail"] += m.rail_rows
            total["lanes"] += m.lanes
            total["cross"] += m.crossings
            total["cong"] += m.congruent
            total["reps"] += m.repeats
            total["some"] += m.repeats > 0
        secs = time.perf_counter() - start
        print(f"{label:<8} {total['rail']:>8} {total['lanes']:>7}"
              f" {total['cross']:>10} {total['cong']:>7}/{total['reps']:<3}"
              f" {total['some']:>18} {secs:>7.2f}")
    dag_layout._motifs = real
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--corpus":
        raise SystemExit(corpus(int(sys.argv[2]) if len(sys.argv) > 2 else 300))
    raise SystemExit(main())
