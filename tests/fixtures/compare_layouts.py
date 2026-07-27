#!/usr/bin/env python3
"""Render the stress fixture every way and print the sizes side by side.

    python tests/fixtures/compare_layouts.py [OUT_DIR]

Writes an SVG (and a PNG where `neato` is on PATH) per variant and a table of
their dimensions, so the choice between label placements is a number rather
than an impression. Run by hand; nothing in the suite depends on it.
"""
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[2] / "src"))
sys.path.insert(0, str(HERE.parents[2]))

from metasmith.models.dag_renderer import LabelMode  # noqa: E402
from tests.fixtures import load_dag  # noqa: E402

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

    (out / "rails.txt").write_text(load_dag().to_text())

    head = f"{'variant':<10} {'svg w':>7} {'svg h':>7} {'lanes':>6} {'rows':>6} {'text':>6}"
    print(head)
    print("-" * len(head))
    for name, w, h, lanes, nrows, lines in rows:
        print(f"{name:<10} {w:>7} {h:>7} {lanes:>6} {nrows:>6} {lines:>6}")
    narrow = min(rows, key=lambda r: r[1])
    wide = max(rows, key=lambda r: r[1])
    print(f"\n{narrow[0]} is {wide[1] / narrow[1]:.1f}x narrower than {wide[0]}")
    print(f"wrote {out}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
