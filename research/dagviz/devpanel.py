#!/usr/bin/env python3
"""Every case through the release engine and the working tree, one page.

    python research/dagviz/devpanel.py          # writes research/dagviz/dev.html

Rows are cases, columns are engines. The SVGs are inlined rather than
published beside the page, so regenerating is one write and one publish and
there is no file set to keep in step while we are still changing our minds.

Add a case in `cases.py`, a question in `questions.py`. Nothing here knows
about either beyond iterating them.
"""

import sys
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import metasmith.models.dag_draw as D  # noqa: E402
import metasmith.models.dag_layout as L  # noqa: E402
import metasmith.models.dag_renderer as DR  # noqa: E402
from cases import CASES  # noqa: E402
from metasmith.models.dag_renderer import DagRenderer, Label, THEMES  # noqa: E402
from baseline import dag_draw as release_draw, dag_layout as release_layout  # noqa: E402
from questions import QUESTIONS  # noqa: E402

_LAYOUT, _RENDER = DR.layout, DR.render_svg

ENGINES = [
    ("release", "v0.23: a lane per edge, then repacked"),
    ("solved", "a step's outputs under it, rows solved for total edge length;"
               " columns for crossings, then horizontal length"),
]


@contextmanager
def one_row_pitch():
    """Both drawers at the release row pitch, so a case is one height in both
    columns. Release needs the taller pitch: its jogs sit a tenth of a gap
    from the rows, inside the markers at the working tree's pitch."""
    grids = [(m, m._grid) for m in (D, release_draw)]

    def pinned(orig):
        def _grid(lay, **k):
            g, drawn = orig(lay, **k)
            pitch = max(2.4 * g.font_size,
                        (g.lane_pitch + g.marker_d) / (1 - 2 * release_draw.BAND))
            return replace(g, row_pitch=pitch,
                           height=2 * g.margin + lay.height * pitch), drawn
        return _grid

    for m, orig in grids:
        m._grid = pinned(orig)
    try:
        yield
    finally:
        for m, orig in grids:
            m._grid = orig


def build(nodes, edges, theme: str) -> DagRenderer:
    r = DagRenderer(colour="none", theme=theme, background=False)
    for kind, name, *label in nodes:
        r.add_node(kind, name, *label or [Label(name=name)])
    for a, b in edges:
        r.add_edge(a, b)
    return r


def _svgs(nodes, edges) -> list[str]:
    return [build(nodes, edges, t).to_svg().split("?>", 1)[-1].strip()
            for t in ("light", "dark")]


def _metric(m: L.Metrics, seconds: float) -> str:
    proof = "optimal" if m.optimal else "unproven"
    columns = "optimal" if m.optimal_columns else "unproven"
    return (f"{m.length} length · {m.width} columns · {m.crossings} crossings"
            f" · {m.hlen} horizontal · rows {proof} · columns {columns}"
            f" · {seconds:.2f}s")


def _release_hlen(lay) -> int:
    return round(sum(
        abs(x1 - x0)
        for e in lay.edges if not e.back
        for (y0, x0), (y1, x1) in zip(e.points, e.points[1:]) if y0 == y1
    ))


def cell(nodes, edges) -> tuple[str, str, str]:
    graph = build(nodes, edges, "light")._graph()
    t = time.perf_counter()
    lay = L.layout(*graph, blocks=DR.step_blocks(*graph))
    seconds = time.perf_counter() - t
    return (*_svgs(nodes, edges), _metric(L.measure(lay), seconds))


def release_cell(nodes, edges) -> tuple[str, str, str]:
    """The release engine whole: its lanes and its drawing, not just its lanes.

    `baseline/` is b7c2a2e8, whose lane assignment is the release branch's
    line for line; the two differ only in how disjoint components are ordered.
    Its crossings count lane swaps between rows, not bars over runs.
    """
    DR.layout = lambda nodes, edges, order=None, blocks=None: release_layout.layout(
        nodes, edges, order)
    DR.render_svg = lambda *a, kinds=None, **k: release_draw.render_svg(*a, **k)
    try:
        svgs = _svgs(nodes, edges)
        t = time.perf_counter()
        lay = release_layout.layout(*build(nodes, edges, "light")._graph())
        seconds = time.perf_counter() - t
        m = release_layout.measure(lay)
    finally:
        DR.layout, DR.render_svg = _LAYOUT, _RENDER
    return (*svgs, f"{m.rail_rows} length · {m.lanes} columns · {m.crossings} crossings"
                   f" · {_release_hlen(lay)} horizontal · {seconds:.2f}s")


def moved(nodes, edges, option) -> L.Layout:
    """The solved layout with some columns set by hand, the rest kept."""
    graph = build(nodes, edges, "light")._graph()
    base = L.layout(*graph, blocks=DR.step_blocks(*graph))
    col = dict(base.col) | option.nodes
    return L.Layout(order=base.order, col=col, edges=base.edges, back=base.back,
                    width=max(col.values()) + 1)


def pinned_cell(nodes, edges, option) -> tuple[str, str, str]:
    lay = moved(nodes, edges, option)
    DR.layout = lambda *a, **k: lay
    try:
        svgs = _svgs(nodes, edges)
    finally:
        DR.layout = _LAYOUT
    return (*svgs, _metric(L.measure(lay), 0.0))


def questions() -> str:
    rows = []
    for name, prompt, options in QUESTIONS:
        cells = []
        for o in options:
            _, nodes, edges = CASES[o.case]
            light, dark, metric = pinned_cell(nodes, edges, o)
            cells.append(
                f'<td><p class="opt"><b>{o.label}</b> {o.caption}</p>'
                f'<div class="svg-light">{light}</div>'
                f'<div class="svg-dark">{dark}</div>'
                f'<p class="m">{metric}</p></td>'
            )
        rows.append(
            f'<tr><th scope="row"><span class="c">{name}</span>'
            f'<span class="q">{prompt}</span></th>{"".join(cells)}</tr>'
        )
    if not rows:
        return ""
    return QUESTIONS_SECTION.replace("{{ROWS}}", "\n".join(rows))


QUESTIONS_SECTION = """  <h2>Questions</h2>
  <p class="lede">Comment on the drawing you pick. Every option in a row is the
    same graph; all but A have a few columns moved by hand.</p>
  <div class="scroll">
    <table class="qs">
      <tbody>
{{ROWS}}
      </tbody>
    </table>
  </div>
"""


def page() -> str:
    head = "".join(
        f'<th><span class="v">{name}</span><span class="d">{note}</span></th>'
        for name, note in ENGINES
    )
    rows = []
    for case, (note, nodes, edges) in CASES.items():
        cells = []
        for light, dark, metric in (release_cell(nodes, edges), cell(nodes, edges)):
            cells.append(
                f'<td><div class="svg-light">{light}</div>'
                f'<div class="svg-dark">{dark}</div>'
                f'<p class="m">{metric}</p></td>'
            )
        rows.append(
            f'<tr><th scope="row"><span class="c">{case}</span>'
            f'<span class="d">{note}</span></th>{"".join(cells)}</tr>'
        )
    return (TEMPLATE.replace("{{HEAD}}", head).replace("{{ROWS}}", "\n".join(rows))
            .replace("{{QUESTIONS}}", questions()))


TEMPLATE = """<title>DAG Lane Strategies</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500&display=swap">
<style>
  :root {
    --ground:#F7F7F8; --surface:#FFF; --ink:#1B1E24; --muted:#6B7484;
    --faint:#9AA2AF; --rule:#E3E3E3; --rule-2:#EFEFF1; --accent:#636EFA;
  }
  @media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
    --ground:#15181D; --surface:#1B1E24; --ink:#DFE3EA; --muted:#8D97A8;
    --faint:#6B7484; --rule:#2F343D; --rule-2:#262B33; --accent:#8B93FB;
  } }
  :root[data-theme="dark"] {
    --ground:#15181D; --surface:#1B1E24; --ink:#DFE3EA; --muted:#8D97A8;
    --faint:#6B7484; --rule:#2F343D; --rule-2:#262B33; --accent:#8B93FB;
  }
  body {
    background:var(--ground); color:var(--ink); margin:0;
    font-family:"IBM Plex Sans",system-ui,sans-serif; font-size:14px;
  }
  .wrap { padding-inline:16px; padding-block:28px 64px; }
  .top { display:flex; align-items:baseline; gap:14px; flex-wrap:wrap; margin-bottom:18px; }
  h1 { font-size:19px; font-weight:500; margin:0; letter-spacing:-0.01em; }
  .lede { color:var(--muted); font-size:13px; margin:0; }
  .switch { display:flex; border:1px solid var(--rule); border-radius:7px;
    overflow:hidden; background:var(--surface); margin-left:auto; }
  .switch button { font:inherit; font-size:12px; color:var(--muted); background:none;
    border:0; border-right:1px solid var(--rule); padding:5px 12px; cursor:pointer; }
  .switch button:last-child { border-right:0; }
  .switch button[aria-pressed="true"] { background:var(--rule-2); color:var(--ink); }
  .scroll { overflow-x:auto; }
  table { border-collapse:collapse; }
  th, td { text-align:left; vertical-align:top; padding:14px 16px;
    border-bottom:1px solid var(--rule-2); border-right:1px solid var(--rule-2); }
  thead th { position:sticky; top:0; background:var(--ground);
    border-bottom:1px solid var(--rule); z-index:1; }
  th[scope="row"] { min-width:13rem; max-width:15rem; }
  .v, .c { display:block; font-family:"IBM Plex Mono",monospace; font-size:13px;
    font-weight:500; color:var(--ink); }
  .d { display:block; font-size:11.5px; color:var(--faint); margin-top:3px; line-height:1.4; }
  td { background:var(--surface); }
  .m { font-family:"IBM Plex Mono",monospace; font-variant-numeric:tabular-nums;
    font-size:11.5px; color:var(--faint); margin:10px 0 0; }
  svg { display:block; }
  h2 { font-size:16px; font-weight:500; margin:40px 0 6px; }
  h2 + .lede { margin-bottom:14px; max-width:62ch; }
  .q { display:block; font-size:12.5px; color:var(--muted); margin-top:6px;
    line-height:1.45; max-width:15rem; }
  .opt { margin:0 0 10px; font-size:12.5px; color:var(--muted); max-width:14rem; }
  .opt b { font-family:"IBM Plex Mono",monospace; color:var(--accent); margin-right:4px; }
  .svg-dark { display:none; }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) .svg-light { display:none; }
    :root:not([data-theme="light"]) .svg-dark { display:block; }
  }
  :root[data-theme="dark"] .svg-light { display:none; }
  :root[data-theme="dark"] .svg-dark { display:block; }
</style>
<div class="wrap">
  <div class="top">
    <h1>DAG Lane Strategies</h1>
    <p class="lede">Same graph through the release engine and the working tree.
      Both are drawn at one row pitch, so a case is one height in both columns.
      Length is total edge length in rows. Horizontal is every edge's
      sideways travel, summed, in columns. A crossing is a run a bar passes,
      a parent that continues below the bar included. The solved column says
      whether its row order and its columns are each proven best.</p>
    <div class="switch" role="group" aria-label="theme">
      <button type="button" data-theme="auto" aria-pressed="true">auto</button>
      <button type="button" data-theme="light" aria-pressed="false">light</button>
      <button type="button" data-theme="dark" aria-pressed="false">dark</button>
    </div>
  </div>
  <div class="scroll">
    <table>
      <thead><tr><th scope="col">case</th>{{HEAD}}</tr></thead>
      <tbody>
{{ROWS}}
      </tbody>
    </table>
  </div>
{{QUESTIONS}}
</div>
<script>
  function setTheme(c) {
    if (c === "auto") document.documentElement.removeAttribute("data-theme");
    else document.documentElement.setAttribute("data-theme", c);
    for (const b of document.querySelectorAll(".switch button"))
      b.setAttribute("aria-pressed", String(b.dataset.theme === c));
    try { localStorage.setItem("devpanel-theme", c); } catch (e) {}
  }
  for (const b of document.querySelectorAll(".switch button"))
    b.addEventListener("click", () => setTheme(b.dataset.theme));
  let s = "auto";
  try { s = localStorage.getItem("devpanel-theme") || "auto"; } catch (e) {}
  setTheme(s);
</script>
"""


if __name__ == "__main__":
    out = HERE / "dev.html"
    with one_row_pitch():
        html = page()
    out.write_text(html, encoding="utf-8")
    print(f"{out}  ({out.stat().st_size // 1024} KiB)"
          f"  {len(CASES)} cases x {len(ENGINES)} engines")
