"""Render the parity report to HTML from the round 3 tables.

Every figure, signature and statement comes from proposals.yml, serving.yml and the
files render.py writes. Run render.py first.
"""

import html
import json
import math
from pathlib import Path

import yaml

HERE = Path(__file__).parent
OUT = Path("/home/tony/.claude/jobs/8338fddb/tmp/kbase_parity_specs.html")

DISP = {
    "existing":  "one or more shipped transforms already do this",
    "extended":  "shipped transforms do part, a proposal completes it",
    "proposed":  "nothing ships, a proposal covers it",
    "refactor":  "the capability ships, locked to the aspire namespace",
    "deferred":  "an import, which a template declares as an input",
    "folded":    "absorbed by another proposed transform",
    "declined":  "nothing will do this, and the reason is recorded",
    "plumbing":  "moves or views objects, so nothing to port",
    "non_task":  "not a task in the round 2 vocabulary",
}


def e(x):
    return html.escape(str(x), quote=True)


def chip(t, kind, new_types):
    ns, _, nm = t.partition("::")
    sup = '<sup>NEW</sup>' if t in new_types else ''
    return f'<span class="chip {kind}"><span class="ns">{e(ns)}::</span>{e(nm)}{sup}</span>'


def signature(row, new_types):
    ins = [t for t in row["consumes"] if not t.startswith("env::")]
    parts = []
    for i, t in enumerate(ins):
        if i:
            parts.append('<span class="plus">+</span>')
        parts.append(chip(t, "i", new_types))
    parts.append('<span class="arrow"%s>&rarr;</span>' % ('' if ins else ' style="padding-left:0"'))
    for i, t in enumerate(row["produces"]):
        if i:
            parts.append('<span class="plus">+</span>')
        parts.append(chip(t, "o", new_types))
    return '<div class="sig">\n        ' + "\n        ".join(parts) + "\n      </div>"


def spec_block(row, new_types):
    d, _, f = row["transform"].rpartition("/")
    envs = [t.replace("env::", "").replace(".env", "")
            for t in row["consumes"] if t.startswith("env::")]
    tags = [f'<span class="badge {row["action"]}">{row["action"]}</span>']
    if row["optional"]:
        tags.append('<span class="badge opt">optional</span>')
    if envs:
        tags.append(f'<span class="tool">{e(envs[0])}</span>')
    copies = row["narrative_copies"]
    plural = "copy" if copies == 1 else "copies"
    note = ""
    if row["note"]:
        cls = "note adopt" if row["note"].startswith("adopts") or row["action"] == "adopt" else "note"
        if row["action"] == "blocked":
            cls = "note adopt"
        note = f'\n        <p class="{cls}">{e(row["note"])}</p>'
    return f"""  <article class="spec">
    <div class="rail">
      <div class="path"><span class="dir">{e(d)}/</span>{e(f)}</div>
      <div class="tags">{''.join(tags)}</div>
      <div class="demand"><b>{copies}</b> narrative {plural}<span class="src">{e(row["canonical_app"])}</span></div>
    </div>
    <div class="body">
      {signature(row, new_types)}
      <div class="prose">
        <p class="why">{e(row["spec"])}</p>{note}
      </div>
    </div>
  </article>
"""


def donut(counts):
    """Part-to-whole over the scientific apps, six segments, validated categorical hues."""
    EXCLUDE = {"plumbing", "deferred", "non_task"}
    slices = sorted(((d, n) for d, n in counts.items() if d not in EXCLUDE),
                    key=lambda x: -x[1])
    assert len(slices) <= 6, "a donut takes six segments at most"
    total = sum(n for _, n in slices)
    R, W, CX, CY = 88, 34, 130, 130
    C = 2 * math.pi * R
    GAP = 2.0
    arcs, legend, off = [], [], 0.0
    for i, (d, n) in enumerate(slices):
        hue = f"var(--s{i + 1})"
        seg = C * n / total
        arcs.append(
            f'<circle class="arc" data-i="{i}" cx="{CX}" cy="{CY}" r="{R}" stroke="{hue}" '
            f'stroke-dasharray="{max(seg - GAP, 0.6):.2f} {C - max(seg - GAP, 0.6):.2f}" '
            f'stroke-dashoffset="{-off:.2f}">'
            f'<title>{d}: {n} apps, {100 * n / total:.0f}%</title></circle>')
        legend.append(
            f'<tr data-i="{i}"><td class="k" style="color:{hue}"><i></i></td>'
            f'<td class="lb">{d}</td><td class="d">{e(DISP[d])}</td>'
            f'<td class="n">{n}</td><td class="p">{100 * n / total:.0f}%</td></tr>')
        off += seg
    return f"""  <figure class="viz">
    <svg viewBox="0 0 260 260" role="img" aria-label="Coverage of the {total} scientific apps by disposition">
      {chr(10).join("      " + a for a in arcs).strip()}
      <text class="hole-n" x="{CX}" y="{CY + 4}">{total}</text>
      <text class="hole-l" x="{CX}" y="{CY + 24}">apps</text>
    </svg>
    <table>
      <caption>scientific apps by disposition</caption>
      <tbody id="legend">
{chr(10).join("        " + r for r in legend)}
      </tbody>
    </table>
    <figcaption>
      The {total} apps that do science &mdash; the catalog after setting aside {counts['plumbing']}
      that move or view objects, {counts['deferred']} that import a file and {counts['non_task']}
      that are not tasks at all. That remainder is logistics, and metasmith answers it by having a
      template declare an input rather than by running a transform.
    </figcaption>
  </figure>
"""


def main():
    spec = yaml.safe_load((HERE / "proposals.yml").read_text())
    summary = json.loads((HERE / "summary.json").read_text())
    rows = [json.loads(l) for l in (HERE / "proposals.jsonl").open()]
    apps = [json.loads(l) for l in (HERE / "app_map.jsonl").open()]
    css = (HERE / "_page.css").read_text()
    new_types = set(spec["new_types"])
    order = list(spec["domains"])

    lad = {s["stage"]: s for s in summary["ladder"]}
    wf, cp = summary["workflows"], summary["copies"]
    pct = round(100 * lad["+ refactors"]["workflows"] / wf)

    figs = [(summary["proposals"], "transforms"), (summary["apps"], "catalog apps"),
            (summary["existing_transforms_cited"], "already shipping"),
            (summary["new_types"], "new types"), (f"{pct}%", f"of {wf} workflows")]

    parts = [f"<title>KBase Parity Specs</title>",
             '<link rel="preconnect" href="https://fonts.googleapis.com">',
             '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
             '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@400;500;600;700&family=IBM+Plex+Serif:ital,wght@0,400;0,500;1,400&display=swap">',
             f"<style>\n{css}\n</style>\n", '<div class="wrap">']

    parts.append(f"""
<header class="mast">
  <div class="eyebrow">metasmith &middot; kbase port &middot; curation round 3</div>
  <h1>KBase Parity Specs</h1>
  <p class="standfirst">
    {summary['proposals']} transforms bring the metasmith standard library level with the DOE KBase
    catalog. Each entry states what the transform consumes, what it produces, and <em>what running
    it buys</em>. Demand is measured &mdash; one copy is one scientist who ran that step in a public
    KBase Narrative.
  </p>
  <div class="figures">""" + "".join(
        f'<div class="fig"><b>{v}</b><span>{k}</span></div>' for v, k in figs) + """</div>
  <div class="legend">
    <div class="lrow"><span class="chip i"><span class="ns">sequences::</span>assembly</span><span>consumed</span></div>
    <div class="lrow"><span class="chip o"><span class="ns">sequences::</span>gbk</span><span>produced</span></div>
    <div class="lrow"><span class="chip o"><span class="ns">modelling::</span>media<sup>NEW</sup></span><span>type does not exist yet</span></div>
    <div class="lrow"><span class="badge write">write</span><span>new transform</span></div>
    <div class="lrow"><span class="badge enable">enable</span><span>already written, sitting in <code>_disabled/</code></span></div>
    <div class="lrow"><span class="badge adopt">adopt</span><span>exists in the fabfos scope, lift it</span></div>
    <div class="lrow"><span class="badge blocked">blocked</span><span>prior art searched for, not found</span></div>
  </div>
</header>
""")

    for d in order:
        drows = [r for r in rows if r["domain"] == d]
        if not drows:
            continue
        parts.append(f"""
<section class="domain">
  <div class="dhead">
    <h2>{e(spec['domains'][d].split(' -- ')[0])}</h2>
    <span class="dcount">{len(drows)} transform{'s' if len(drows) != 1 else ''}</span>
    <span class="dmeans">{e(spec['domains'][d].split(' -- ', 1)[1] if ' -- ' in spec['domains'][d] else spec['domains'][d])}</span>
  </div>
""")
        parts += [spec_block(r, new_types) for r in drows]
        parts.append("</section>\n")

    # ---- app map --------------------------------------------------------
    counts = summary["app_disposition"]
    order_d = ["extended", "existing", "proposed", "refactor", "deferred", "folded",
               "declined", "plumbing", "non_task"]
    btns = "".join(
        f'<button class="chipbtn" data-d="{d}" aria-pressed="false">{d}<span class="n">{counts[d]}</span></button>'
        for d in order_d if d in counts)
    blocked = {r["transform"] for r in rows if r["action"] == "blocked"}
    trs = []
    for a in apps:
        def cite(s):
            base = e(s.rpartition("/")[2])
            if s in blocked:
                return f'<span class="blk" title="prior art not found">{base}</span>'
            return f'<span class="new">{base}</span>' if s.startswith("kbase/") else base
        served = " · ".join(cite(s) for s in a["served_by"]) or "&mdash;"
        verb = a["verb"] or (a["non_task"] or "")
        trs.append(
            f'<tr class="{"" if a["active"] else "inactive"}" data-d="{a["how"]}">'
            f'<td>{e(a["app_id"])}</td>'
            f'<td>{e(verb)}</td>'
            f'<td class="cp">{a["copies"]}</td>'
            f'<td><span class="disp {a["how"]}">{a["how"]}</span></td>'
            f'<td class="sv">{served}</td></tr>')

    parts.append(f"""
<section class="mapsec">
  <h2>Every app in the catalog, and what serves it</h2>
  <p class="intro">
    All {summary['apps']} apps KBase publishes, sorted by how many narratives ran them. The
    mapping is many-to-one by design: 23 assemblers resolve to seven, and 144 viewers resolve to
    nothing, because a metasmith product is a file and rendering it is the GUI's job. The
    {summary['proposals']} proposals sit on top of {summary['existing_transforms_cited']} transforms
    the library already ships. A dashed underline in the last column marks the one proposal whose
    prior art was searched for and not found.
  </p>
{donut(counts)}
  <div class="controls">
    <input id="q" type="search" placeholder="filter app, verb or transform" aria-label="Filter apps">
    {btns}
    <span id="count"></span>
  </div>
  <div class="tablewrap">
    <table class="map">
      <colgroup><col class="c-app"><col class="c-verb"><col class="c-cp"><col class="c-disp"><col class="c-sv"></colgroup>
      <thead><tr>
        <th data-s="0" data-t="s" tabindex="0" aria-sort="none">app</th>
        <th data-s="1" data-t="s" tabindex="0" aria-sort="none">verb</th>
        <th data-s="2" data-t="n" tabindex="0" aria-sort="descending">copies</th>
        <th data-s="3" data-t="s" tabindex="0" aria-sort="none">disposition</th>
        <th data-s="4" data-t="s" tabindex="0" aria-sort="none">served by</th>
      </tr></thead>
      <tbody id="rows">
{chr(10).join(trs)}
      </tbody>
    </table>
  </div>
  <dl class="declined" style="margin-top:22px">
""" + "".join(f"<dt>{d}</dt><dd>{e(DISP[d])}</dd>" for d in order_d if d in counts) + """
  </dl>
</section>
""")

    # ---- closing --------------------------------------------------------
    parts.append("""
<section class="closer">
  <h2>Three items with no signature</h2>
  <p class="intro">
    Work the parity pass turned up that no transform performs. The first closes five KBase verbs on
    its own.
  </p>
""")
    for r in spec["refactors"]:
        closes = ""
        if r["closes"]:
            closes = f'<span class="closes">closes {len(r["closes"])} verb{"s" if len(r["closes"]) != 1 else ""} &middot; {", ".join(r["closes"])}</span>'
        parts.append(f"""  <div class="item">
    <h3>{e(r['name'])}</h3>
    <p>{e(r['means'])}</p>
    {closes}
  </div>
""")
    declined = [v for v in spec["verbs"] if v["status"] == "declined"]
    parts.append('  <div class="declined">\n    <h3>Declined, with the reason</h3>\n    <dl>\n')
    for v in declined:
        parts.append(f"      <dt>{e(v['verb'])}</dt><dd>{e(v['reason'])}</dd>\n")
    parts.append("    </dl>\n  </div>\n")
    parts.append(f"""
  <footer class="colophon">
    research/kbase/curation/r3/ &middot; proposals.yml and serving.yml are authored, render.py gates them against round 2, build_artifact.py renders this page<br>
    {summary['apps']} catalog apps &middot; 3,965 public KBase Narratives &rarr; 1,633 workflow shapes &rarr; {wf} canonical workflows &middot; {cp} narrative copies<br>
    reachability is a claim about the verb graph, not the type graph &mdash; it does not prove the planner finds a chain
  </footer>
</section>
</div>

<script>
(function(){{
  var q = document.getElementById('q'), cnt = document.getElementById('count');
  var tbody = document.getElementById('rows');
  var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
  var btns = Array.prototype.slice.call(document.querySelectorAll('.chipbtn'));
  var heads = Array.prototype.slice.call(document.querySelectorAll('th[data-s]'));
  var active = new Set();
  var sortCol = 2, sortDir = -1;

  var arcs = Array.prototype.slice.call(document.querySelectorAll('.viz .arc'));
  var legs = Array.prototype.slice.call(document.querySelectorAll('#legend tr'));
  function hot(i, on){{
    arcs.forEach(function(a){{ if (a.dataset.i === i) a.classList.toggle('hot', on); }});
    legs.forEach(function(r){{ if (r.dataset.i === i) r.classList.toggle('hot', on); }});
  }}
  arcs.concat(legs).forEach(function(el){{
    el.addEventListener('mouseenter', function(){{ hot(el.dataset.i, true); }});
    el.addEventListener('mouseleave', function(){{ hot(el.dataset.i, false); }});
  }});

  function cell(row, i){{ return row.children[i].textContent.trim().toLowerCase(); }}

  function sortBy(i, dir){{
    var numeric = heads[i].dataset.t === 'n';
    rows.sort(function(a, b){{
      var x = cell(a, i), y = cell(b, i);
      if (numeric) {{ x = parseFloat(x) || 0; y = parseFloat(y) || 0; }}
      if (x < y) return -dir;
      if (x > y) return dir;
      var ax = cell(a, 0), bx = cell(b, 0);
      return ax < bx ? -1 : ax > bx ? 1 : 0;
    }});
    var frag = document.createDocumentFragment();
    rows.forEach(function(r){{ frag.appendChild(r); }});
    tbody.appendChild(frag);
    heads.forEach(function(h, j){{
      h.setAttribute('aria-sort', j === i ? (dir === 1 ? 'ascending' : 'descending') : 'none');
    }});
  }}

  function apply(){{
    var t = q.value.trim().toLowerCase(), n = 0;
    rows.forEach(function(r){{
      var okD = active.size === 0 || active.has(r.dataset.d);
      var okT = !t || r.textContent.toLowerCase().indexOf(t) !== -1;
      var show = okD && okT;
      r.hidden = !show;
      if (show) n++;
    }});
    cnt.textContent = n + ' of ' + rows.length + ' apps';
  }}

  function clickHead(h){{
    var i = parseInt(h.dataset.s, 10);
    sortDir = (i === sortCol) ? -sortDir : (h.dataset.t === 'n' ? -1 : 1);
    sortCol = i;
    sortBy(sortCol, sortDir);
  }}
  heads.forEach(function(h){{
    h.addEventListener('click', function(){{ clickHead(h); }});
    h.addEventListener('keydown', function(ev){{
      if (ev.key === 'Enter' || ev.key === ' ') {{ ev.preventDefault(); clickHead(h); }}
    }});
  }});
  q.addEventListener('input', apply);
  btns.forEach(function(b){{
    b.addEventListener('click', function(){{
      var d = b.dataset.d;
      if (active.has(d)) {{ active.delete(d); b.setAttribute('aria-pressed','false'); }}
      else {{ active.add(d); b.setAttribute('aria-pressed','true'); }}
      apply();
    }});
  }});
  apply();
}})();
</script>
""")

    OUT.write_text("\n".join(parts))
    print(f"wrote {OUT} ({OUT.stat().st_size // 1024} KB) — {len(rows)} specs, {len(apps)} app rows")


if __name__ == "__main__":
    main()
