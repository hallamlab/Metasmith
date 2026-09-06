#!/usr/bin/env python3
"""Assemble the eight rung DAGs into one scrolling gallery page.

Reads what `mkladder.py` wrote -- `ladder-index.json` and the sixteen
`rung-*-{light,dark}.svg` plates -- and inlines them. Run mkladder.py first;
this script only lays out what is already on disk.

    python research/viromics/reports/mkladderpage.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "ladder.html"

XML_DECL = re.compile(r"^<\?xml[^>]*\?>\s*", re.S)


def plate(key: str, theme: str) -> str:
    svg = (HERE / f"rung-{key}-{theme}.svg").read_text()
    svg = XML_DECL.sub("", svg).strip()
    return svg.replace("<svg ", f'<svg class="dag dag-{theme}" ', 1)


HEAD = """<title>Eight Solves</title>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?\
family=Archivo:wdth,wght@62..125,400..800&family=JetBrains+Mono:wght@400;500;700&display=swap">
<style>
:root{
  --paper:#E6EAEC; --plate:#F8FAFA; --band:#DDE3E6;
  --ink:#0E141A; --ink-2:#54666F; --ink-3:#8798A1;
  --rule:#C6D0D4; --rule-soft:#D6DEE1;
  --accent:#1F4E79; --accent-soft:#CBD9E5;
}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
  --paper:#0D1216; --plate:#12171C; --band:#141B21;
  --ink:#DFE7EA; --ink-2:#8FA2AB; --ink-3:#677A84;
  --rule:#232F36; --rule-soft:#1A242A;
  --accent:#7FB3DC; --accent-soft:#17303F;
}}
:root[data-theme="dark"]{
  --paper:#0D1216; --plate:#12171C; --band:#141B21;
  --ink:#DFE7EA; --ink-2:#8FA2AB; --ink-3:#677A84;
  --rule:#232F36; --rule-soft:#1A242A;
  --accent:#7FB3DC; --accent-soft:#17303F;
}

*{box-sizing:border-box}
body{
  margin:0; background:var(--paper); color:var(--ink);
  font-family:"Archivo","Helvetica Neue",Arial,sans-serif;
  font-variation-settings:"wdth" 100;
  -webkit-font-smoothing:antialiased;
}
.mono{font-family:"JetBrains Mono",ui-monospace,"SF Mono",Menlo,monospace}
:focus-visible{outline:2px solid var(--accent); outline-offset:3px}

.sheet{max-width:1000px; margin:0 auto; padding:0 24px 120px}

/* ---------------------------------------------------------------- masthead */
header.top{padding:60px 0 26px}
.slug{font-family:"JetBrains Mono",monospace; font-size:11px; letter-spacing:.18em;
  text-transform:uppercase; color:var(--accent); margin:0 0 20px}
h1{margin:0; font-size:clamp(40px,7vw,86px); line-height:.95; font-weight:800;
  font-variation-settings:"wdth" 118,"wght" 800; letter-spacing:-.02em;
  text-wrap:balance}
.sub{margin:16px 0 0; font-family:"JetBrains Mono",monospace; font-size:13px;
  line-height:1.7; color:var(--ink-2); max-width:58ch}
.sub b{color:var(--ink); font-weight:500}

/* ---------------------------------------------------------------- index */
.index{margin-top:44px; border-top:2px solid var(--ink)}
.irow{display:grid; grid-template-columns:34px minmax(0,1fr) minmax(90px,26vw) 62px;
  gap:16px; align-items:center; padding:11px 0; border-bottom:1px solid var(--rule-soft);
  transition:background .12s ease;
  color:inherit; text-decoration:none}
.irow:hover{background:var(--band)}
.irow .o{font-family:"JetBrains Mono",monospace; font-size:11.5px; color:var(--ink-2);
  font-variant-numeric:tabular-nums}
.irow .t{font-size:15px; font-weight:600; font-variation-settings:"wdth" 108,"wght" 600;
  letter-spacing:-.005em; overflow:hidden; text-overflow:ellipsis; white-space:nowrap}
.irow .b{height:9px; background:var(--rule-soft); border-radius:0 4px 4px 0;
  position:relative; overflow:hidden}
.irow .b i{display:block; height:100%; background:var(--accent); border-radius:0 4px 4px 0}
.irow .n{font-family:"JetBrains Mono",monospace; font-size:13px; font-weight:500;
  text-align:right; font-variant-numeric:tabular-nums}
.ihead{display:grid; grid-template-columns:34px minmax(0,1fr) minmax(90px,26vw) 62px;
  gap:16px; padding:9px 0; font-family:"JetBrains Mono",monospace; font-size:10px;
  letter-spacing:.14em; text-transform:uppercase; color:var(--ink-3);
  border-bottom:1px solid var(--rule)}
.ihead .r{text-align:right}
@media (max-width:620px){.ihead{grid-template-columns:30px 1fr 54px} .ihead .h3{display:none}}
@media (max-width:620px){
  .irow{grid-template-columns:30px 1fr 54px}
  .irow .b{display:none}
}

/* ---------------------------------------------------------------- rungs */
.rung{scroll-margin-top:0}
.label{position:sticky; top:0; z-index:5; background:var(--paper);
  border-bottom:1px solid var(--ink); padding:14px 0 11px;
  display:grid; grid-template-columns:auto minmax(0,1fr) auto auto; gap:18px;
  align-items:baseline}
@media (max-width:620px){.label{grid-template-columns:auto minmax(0,1fr) auto}
  .label .copy{grid-column:2/4; justify-self:start; margin-top:8px}}
.label .o{font-family:"JetBrains Mono",monospace; font-size:12px; font-weight:700;
  color:var(--accent); letter-spacing:.1em; font-variant-numeric:tabular-nums}
.label h2{margin:0; font-size:clamp(19px,2.6vw,27px); font-weight:700;
  font-variation-settings:"wdth" 112,"wght" 700; letter-spacing:-.012em;
  line-height:1.1; text-wrap:balance}
.label .cap{display:block; font-family:"JetBrains Mono",monospace; font-size:11.5px;
  font-weight:400; color:var(--ink-2); letter-spacing:0; margin-top:5px;
  font-variation-settings:normal}
.label .ct{font-family:"JetBrains Mono",monospace; font-size:11.5px; color:var(--ink-2);
  white-space:nowrap; font-variant-numeric:tabular-nums; text-align:right}
.label .ct b{display:block; font-family:"Archivo",sans-serif; font-size:30px;
  font-weight:700; font-variation-settings:"wdth" 120,"wght" 700; color:var(--ink);
  line-height:1; letter-spacing:-.02em}

.label .copy{font-family:"JetBrains Mono",monospace; font-size:11px; letter-spacing:.06em;
  color:var(--ink-2); background:transparent; border:1px solid var(--rule);
  border-radius:2px; padding:6px 11px; cursor:pointer; white-space:nowrap;
  transition:border-color .12s ease, color .12s ease}
.label .copy:hover{border-color:var(--accent); color:var(--accent)}
.label .copy[data-state="done"]{border-color:var(--accent); color:var(--accent)}
.label .copy[data-state="fail"]{border-color:var(--ink-3); color:var(--ink-3)}

.plate{background:var(--plate); border:1px solid var(--rule); border-top:0;
  overflow-x:auto; padding:30px 20px 36px}
.plate .dag{display:block; margin:0 auto; width:auto; max-width:none; height:auto}
.plate .dag-dark{display:none}
@media (prefers-color-scheme:dark){
  :root:not([data-theme="light"]) .plate .dag-light{display:none}
  :root:not([data-theme="light"]) .plate .dag-dark{display:block}
}
:root[data-theme="dark"] .plate .dag-light{display:none}
:root[data-theme="dark"] .plate .dag-dark{display:block}

.key{display:flex; flex-wrap:wrap; gap:10px 24px; padding:22px 0 0;
  font-family:"JetBrains Mono",monospace; font-size:11.5px; color:var(--ink-2)}
.key span{display:flex; align-items:center; gap:8px}
.key svg{display:block; overflow:visible}
.key .mk{fill:var(--plate); stroke:var(--ink); stroke-width:1.5}
.key .mk.solid{fill:var(--ink)}

footer{margin-top:56px; padding-top:20px; border-top:1px solid var(--rule);
  font-family:"JetBrains Mono",monospace; font-size:11.5px; color:var(--ink-3);
  display:flex; flex-wrap:wrap; gap:6px 28px}

@media (prefers-reduced-motion:reduce){*{animation:none!important; transition:none!important}}
</style>
"""


SCRIPT = """
<script>
// The viewer sandbox makes a download inert, so the copy is the way a plate
// leaves the page. Serialise whichever theme's SVG is actually on screen.
document.querySelectorAll('.rung').forEach(function (section) {
  var btn = section.querySelector('.copy');
  if (!btn) return;
  btn.addEventListener('click', function () {
    var svg = Array.prototype.find.call(
      section.querySelectorAll('.plate svg'),
      function (el) { return el.getClientRects().length > 0; }
    );
    if (!svg) return;
    var markup = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\\n'
               + new XMLSerializer().serializeToString(svg);
    var settle = function (label, state) {
      btn.textContent = label;
      btn.setAttribute('data-state', state);
      setTimeout(function () {
        btn.textContent = 'copy svg';
        btn.removeAttribute('data-state');
      }, 1600);
    };
    try {
      navigator.clipboard.writeText(markup).then(
        function () { settle('copied', 'done'); },
        function () { settle('copy blocked', 'fail'); }
      );
    } catch (e) {
      settle('copy blocked', 'fail');
    }
  });
});
</script>
"""

def build() -> str:
    rungs = json.loads((HERE / "ladder-index.json").read_text())
    top = max(r["steps"] for r in rungs)

    idx = []
    for r in rungs:
        idx.append(
            f'<a class="irow" href="#r{r["n"]}">'
            f'<span class="o">{r["n"]:02d}</span>'
            f'<span class="t">{r["title"]}</span>'
            f'<span class="b"><i style="width:{100 * r["steps"] / top:.1f}%"></i></span>'
            f'<span class="n">{r["steps"]}</span></a>'
        )

    body = []
    for r in rungs:
        body.append(
            f'<section class="rung" id="r{r["n"]}">'
            f'<div class="label"><span class="o">{r["n"]:02d}</span>'
            f'<h2>{r["title"]}<span class="cap">{r["caption"]}</span></h2>'
            f'<span class="ct"><b>{r["steps"]}</b>'
            f'{"step" if r["steps"] == 1 else "steps"} &middot; '
            f'{r["targets"]} target{"" if r["targets"] == 1 else "s"}</span>'
            f'<button class="copy" type="button">copy svg</button></div>'
            f'<div class="plate">{plate(r["key"], "light")}{plate(r["key"], "dark")}</div>'
            f'</section>'
        )

    key = (
        '<div class="key">'
        '<span><svg width="15" height="13" viewBox="0 0 15 13">'
        '<path class="mk" d="M 1.4,2 L 13.6,2 L 7.5,11.6 z"/></svg> a transform &mdash; one step</span>'
        '<span><svg width="15" height="13" viewBox="0 0 15 13">'
        '<circle class="mk" cx="7.5" cy="6.5" r="5"/></svg> data it produces</span>'
        '<span><svg width="15" height="13" viewBox="0 0 15 13">'
        '<circle class="mk solid" cx="7.5" cy="6.5" r="5"/></svg> a named target</span>'
        '<span>rows are dependency depth &middot; numbers are execution order</span>'
        '</div>'
    )

    return (
        HEAD
        + '<div class="sheet">'
        + '<header class="top"><p class="slug">metasmith &middot; viromics &middot; '
          'one plan at a time</p><h1>Eight<br>Solves</h1>'
          '<p class="sub">Eight separate plans, each solved on its own. Every rung '
          'changes <b>the inputs and the targets</b> &mdash; never the steps. '
          'What appears in each picture, the planner chose.</p>'
        + '<div class="index"><div class="ihead"><span></span><span>plan</span>'
          '<span class="h3"></span><span class="r">steps</span></div>'
          + "".join(idx) + '</div>'
        + key
        + '</header>'
        + "".join(body)
        + SCRIPT
        + '<footer><span>research/viromics/reports/mkladder.py</span>'
          '<span>solver: rust, seed 42</span>'
          '<span>rung 8 is the shipped template</span></footer>'
        + '</div>'
    )


def main() -> int:
    OUT.write_text(build())
    print(f"{OUT}  {OUT.stat().st_size / 1024:.0f} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
