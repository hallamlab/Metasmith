"""Build engcyc_pipeline.html from page.src.html, ../pipeline.yml and the DAGs the driver renders.

Run the driver with `solve --dag` first: it writes dags/<shape>_{steps,full}.svg and dags/<shape>.json.
"""
import html
import json
import re
from datetime import date
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
DATA = yaml.safe_load((HERE.parent / "pipeline.yml").read_text())
COLUMNS = DATA["columns"]
e = html.escape


def svg(name):
    text = (HERE / "dags" / f"{name}.svg").read_text()
    return text[text.index("<svg"):]


def chip(u):
    return f'<a class="chip chip-open" href="#{u.lower()}">{u}</a>'


def cell(text):
    text = str(text)
    if text in ("=", "= ?"):
        return f'<td class="same">{e(text)}</td>'
    if text == "-":
        return '<td class="none">&ndash;</td>'
    return f"<td>{e(text)}</td>"


def tables():
    out = []
    for group in DATA["groups"]:
        note = f'<span class="group-note">{e(group["note"])}</span>' if group.get("note") else ""
        gopen = f' {chip(group["open"])}' if group.get("open") else ""
        head = "".join(f'<th scope="col">{e(v)}</th>' for v in COLUMNS.values())
        rows = []
        for r in group["rows"]:
            marks = ('<span class="chip chip-diff">diff</span>' if r.get("diff") else "") + (
                chip(r["open"]) if r.get("open") else "")
            cls = ' class="diff"' if r.get("diff") else ""
            cells = "".join(cell(r[k]) for k in COLUMNS)
            rows.append(f'<tr{cls}><th scope="row"><span class="stage">{e(r["stage"])}</span>{marks}</th>{cells}</tr>')
        out.append(f'''<section class="tgroup">
  <h3>{e(group["name"])}{gopen} {note}</h3>
  <div class="scroll"><table>
    <thead><tr><th scope="col">Stage</th>{head}</tr></thead>
    <tbody>{"".join(rows)}</tbody>
  </table></div>
</section>''')
    return "\n".join(out)


def shapes():
    tabs, steps, full = [], [], []
    for i, (key, s) in enumerate(DATA["shapes"].items()):
        stats = json.loads((HERE / "dags" / f"{key}.json").read_text())
        selected = "true" if i == 0 else "false"
        hidden = "" if i == 0 else " hidden"
        tabs.append(f'<button type="button" role="tab" id="tab-{key}" aria-selected="{selected}" '
                    f'data-shape="{key}">{e(s["title"])}</button>')
        meta = (f'<dl class="shape-meta">'
                f'<div><dt>Front end</dt><dd>{e(s["front"])}</dd></div>'
                f'<div><dt>Datasets</dt><dd>{e(s["datasets"])}</dd></div>'
                f'<div><dt>Plan</dt><dd class="num">{stats["steps"]} steps, {stats["tools"]} without reference '
                f'fetches, {stats["targets"]} targets, none dropped</dd></div></dl>')
        steps.append(f'<div class="shape" data-shape="{key}"{hidden}>{meta}'
                     f'<div class="paper" tabindex="0" aria-label="{e(s["title"])} tools">{svg(key + "_steps")}</div></div>')
        full.append(f'<div class="shape" data-shape="{key}"{hidden}>'
                    f'<div class="paper paper-tall" tabindex="0" aria-label="{e(s["title"])} every step">'
                    f'{svg(key + "_full")}</div></div>')
    return "".join(tabs), "".join(steps), "".join(full)


def open_items():
    return "\n".join(f'<li id="{k.lower()}"><span class="u">{k}</span><p>{e(v)}</p></li>'
                     for k, v in DATA["open"].items())


def findings():
    return "\n".join(f'<li><h3>{e(f["title"])}</h3><p>{e(f["body"])}</p></li>' for f in DATA["findings"])


def sources():
    return "\n".join(f'<div><dt>{e(COLUMNS[k])}</dt><dd>{e(v)}</dd></div>' for k, v in DATA["sources"].items())


tabs, steps, full = shapes()
fills = {"TABS": tabs, "STEPS": steps, "FULL": full, "OPEN": open_items(), "FINDINGS": findings(),
         "TABLES": tables(), "SOURCES": sources(), "DATE": date.today().isoformat()}
src = (HERE / "page.src.html").read_text()
src = re.sub(r"\{\{(\w+)\}\}", lambda m: fills[m.group(1)], src)
assert "{{" not in src, "unfilled placeholder"
out = HERE / "engcyc_pipeline.html"
out.write_text(src)
print(out)
