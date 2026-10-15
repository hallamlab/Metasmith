#!/usr/bin/env python3
"""Render the inventory page from data/mm_compare/inventory/*.tsv.

    inventory_page.py <repo root> <out.html>

Counts stay in each method's own units. The prose sections live here, beside the numbers they describe.
"""
import html
import sys
from pathlib import Path

import pandas as pd

METHODS = {"pgdb": "Pathway Tools", "carveme": "CarveMe", "gpr": "fabfos 4-lane"}
LANES = ("clean", "kofam", "pbert", "uniref50")


def fmt(v, digits=0):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return '<span class="na">—</span>'
    if isinstance(v, str):
        return html.escape(v)
    return f"{v:,.{digits}f}"


def table(head, rows, cls=""):
    th = "".join(f"<th>{h}</th>" for h in head)
    tr = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="scroll"><table class="{cls}"><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table></div>'


def tag(method):
    return f'<span class="tag {method}">{METHODS[method]}</span>'


def iqr(s, digits=0):
    s = s.dropna()
    if s.empty:
        return fmt(None)
    q1, q2, q3 = s.quantile([0.25, 0.5, 0.75])
    return f'{fmt(q2, digits)} <span class="iqr">{fmt(q1, digits)}–{fmt(q3, digits)}</span>'


def ecoli_section(cm, gp):
    cm = cm[cm.dataset == "ecoli"].set_index("subject")
    gp = gp[gp.dataset == "ecoli"].set_index("subject")
    order = ["k12", "w3110", "bw25113", "dh1", "dh10b", "epi300"]
    rows = []
    for s in order:
        c, g = cm.loc[s], gp.loc[s]
        rows.append([f"<b>{s}</b>", fmt(int(g.orfs)),
                     fmt(int(c.reactions)), fmt(int(c.reactions_with_gene_rule)), fmt(int(c.genes)),
                     fmt(int(c.metabolites)), fmt(c.memote_total, 3),
                     fmt(int(g.orfs_with_call)), fmt(int(g.calls)), fmt(int(g.mnxr)),
                     *(fmt(int(g[f"{l}_mnxr"])) for l in LANES),
                     fmt(None)])
    head = ["strain", "proteins",
            "reactions", "with gene rule", "genes", "metabolites", "MEMOTE",
            "ORFs with a call", "calls", "distinct MNXR", *(f"{l} MNXR" for l in LANES),
            "PGDB"]
    groups = ('<tr class="group"><th></th><th></th><th colspan="5" class="carveme">CarveMe</th>'
              '<th colspan="7" class="gpr">fabfos 4-lane</th><th class="pgdb">Pathway Tools</th></tr>')
    t = table(head, rows, "num")
    return t.replace("<thead>", f"<thead>{groups}", 1)


def cami_summary(cm, pg, gp):
    c = cm[cm.dataset == "cami_hmp"]
    gen, com = pg[pg.level == "genome"], pg[pg.level == "community"]
    rows = [
        [tag("pgdb"), "gold-standard genome", fmt(len(gen)), "reactions", iqr(gen.reactions)],
        ["", "", "", "reactions with a gene", iqr(gen.reactions_with_gene)],
        ["", "", "", "genes with a reaction", iqr(gen.genes_with_reaction)],
        ["", "", "", "pathways inferred", iqr(gen.pathways)],
        ["", "", "", "pathways in _pwy.tsv", iqr(gen.pwy_tsv_rows)],
        ["", "", "", "mean pathway score", iqr(gen.pwy_score_mean, 3)],
        [tag("pgdb"), "community (one per sample)", fmt(len(com)), "reactions with a gene", iqr(com.reactions_with_gene)],
        ["", "", "", "pathways inferred", iqr(com.pathways)],
        [tag("carveme"), "E5 MAG", fmt(len(c)), "reactions", iqr(c.reactions)],
        ["", "", "", "reactions with a gene rule", iqr(c.reactions_with_gene_rule)],
        ["", "", "", "genes", iqr(c.genes)],
        ["", "", "", "metabolites", iqr(c.metabolites)],
        ["", "", "", "MEMOTE total", iqr(c.memote_total, 3)],
    ]
    g = gp[gp.dataset == "cami_hmp"] if gp is not None else None
    if g is not None and len(g):
        rows += [
            [tag("gpr"), "E5 MAG", fmt(len(g)), "ORFs with a call", iqr(g.orfs_with_call)],
            ["", "", "", "calls", iqr(g.calls)],
            ["", "", "", "distinct MNXR", iqr(g.mnxr)],
            *(["", "", "", f"{l} distinct MNXR", iqr(g[f"{l}_mnxr"])] for l in LANES),
        ]
    else:
        rows.append([tag("gpr"), "E5 MAG", fmt(None), '<span class="pending">running on fir</span>', fmt(None)])
    return table(["method", "subject", "subjects", "count", "median <span class=iqr>IQR</span>"], rows, "summary")


def cami_samples(sm, cm, pg, gp):
    c = cm[cm.dataset == "cami_hmp"].groupby("sample")
    gen = pg[pg.level == "genome"].groupby("sample")
    com = pg[pg.level == "community"].set_index("sample")
    g = gp[gp.dataset == "cami_hmp"].groupby("sample") if gp is not None and (gp.dataset == "cami_hmp").any() else None
    rows = []
    for _, s in sm.sort_values(["body_site", "ryan_sample"]).iterrows():
        r, e = s.ryan_sample, s.e5_sample
        gg = g.get_group(e) if g is not None and e in g.groups else None
        rows.append([f"<b>{r}</b>", s.body_site,
                     fmt(len(gen.get_group(r))), fmt(gen.get_group(r).reactions_with_gene.median()),
                     fmt(int(com.loc[r].reactions_with_gene)), fmt(int(com.loc[r].pathways)),
                     fmt(len(c.get_group(e))), fmt(c.get_group(e).reactions.median()),
                     fmt(c.get_group(e).memote_total.median(), 3),
                     fmt(None if gg is None else gg.mnxr.median())])
    head = ["sample", "body site", "genomes", "median rxns with gene", "community rxns with gene",
            "community pathways", "MAGs", "median reactions", "median MEMOTE", "median distinct MNXR"]
    groups = ('<tr class="group"><th></th><th></th><th colspan="4" class="pgdb">Pathway Tools</th>'
              '<th colspan="3" class="carveme">CarveMe</th><th class="gpr">4-lane</th></tr>')
    return table(head, rows, "num").replace("<thead>", f"<thead>{groups}", 1)


FINDINGS = """
<li><b>The methods describe different genomes on CAMI.</b> Ryan's PGDBs cover the 5,392 gold-standard
CAMI_OTU genomes plus one community PGDB per sample. CarveMe and the 4-lane cover E5's 757 DAS Tool MAGs,
assembled from reads. A per-subject pairing across these two sets needs a MAG-to-genome assignment, which
neither output carries.</li>
<li><b>MEMOTE's total jumps on a small change.</b> DH10B scores 0.847 and the other five strains score
0.680–0.681, with about 40 fewer reactions in DH10B. The whole gap is the consistency section (0.873
against 0.448). The 757 CAMI MAGs split the same way: 431 score 0.66–0.71 and 326 score 0.82–0.88, with
none between. Consistency alone separates them (0.45–0.53 against 0.89–0.97). The four annotation sections
score alike in both groups. Read MEMOTE's total as two states plus noise, not as a scale.</li>
<li><b>A PGDB's reaction list includes reactions with no gene.</b> In one genome, reactions.dat lists 2,320
reactions and 1,443 have an enzyme linked to a gene. Count reactions with a gene when the question is
what the genome encodes.</li>
<li><b>Ryan's <code>_pwy.tsv</code> is a filtered list.</b> PathoLogic's evidence list names more pathways
than the table (358 against 328 in the same genome).</li>
<li><b>The 4-lane emits a scored candidate set, not a model.</b> Each E. coli strain gets about 31,000
calls to about 11,200 distinct MNXR from about 2,800 ORFs. CLEAN alone proposes about 9,700 MNXR.
CarveMe's models hold about 2,480 reactions. Any count comparison with the 4-lane needs a score cut,
and choosing it is part of the test.</li>
<li><b>Some identifiers overlap without translation.</b> CarveMe's SBML annotations carry MetaNetX reaction
ids (about 2,000 of 2,480 in E. coli), MetaCyc reaction ids (about 960) and EC numbers (about 950). The
4-lane speaks MNXR natively and the PGDBs speak MetaCyc and EC. CarveMe's MNXR come from the MetaNetX
release BiGG annotated against, which is older than the 4-lane's.</li>
<li><b>E. coli gene ids line up between CarveMe and the 4-lane.</b> Both read the same RefSeq proteome, so
a gene-level comparison needs no mapping there. MetaPathways calls its own ORFs from the genome, so a
PGDB's gene ids will differ.</li>
"""

STYLE = """
/* Layout: one reading column, wide tables scroll inside their own frame. */
:root {
  --bg: #f6f7f5; --panel: #ffffff; --fg: #1d2421; --muted: #5d6762; --rule: #d9ddd9;
  --accent: #2f6b5a; --pgdb: #9a6a12; --carveme: #2d5f8f; --gpr: #7b3f74;
  --pgdb-bg: #f6ecd9; --carveme-bg: #e1eaf3; --gpr-bg: #f0e3ee; --pending: #a14d1f;
  --display: "Literata", Georgia, serif; --body: "Public Sans", system-ui, sans-serif;
  --mono: "JetBrains Mono", ui-monospace, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #141917; --panel: #1b2220; --fg: #e3e8e5; --muted: #9aa5a0; --rule: #2e3834;
  --accent: #7cc3ac; --pgdb: #e2b15c; --carveme: #8bb8e3; --gpr: #d397cb;
  --pgdb-bg: #352a17; --carveme-bg: #1c2a38; --gpr-bg: #33212f; --pending: #e7926a; color-scheme: dark } }
:root[data-theme="dark"] {
  --bg: #141917; --panel: #1b2220; --fg: #e3e8e5; --muted: #9aa5a0; --rule: #2e3834;
  --accent: #7cc3ac; --pgdb: #e2b15c; --carveme: #8bb8e3; --gpr: #d397cb;
  --pgdb-bg: #352a17; --carveme-bg: #1c2a38; --gpr-bg: #33212f; --pending: #e7926a; color-scheme: dark }
body { background: var(--bg); color: var(--fg); font: 15px/1.55 var(--body); }
.wrap { max-width: 1080px; margin: 0 auto; padding-block: 28px 64px;
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px));
  display: grid; gap: 36px; }
header { display: grid; gap: 10px; }
.top { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; }
.eyebrow { font: 500 12px/1 var(--mono); letter-spacing: .06em; color: var(--muted); text-transform: uppercase; }
h1 { font: 600 clamp(28px, 4vw, 38px)/1.15 var(--display); margin: 0; text-wrap: balance; }
h2 { font: 600 22px/1.25 var(--display); margin: 0; text-wrap: balance; }
p, li { max-width: 68ch; }
p { margin: 0; }
section { display: grid; gap: 14px; min-width: 0; }
.lede { color: var(--muted); font-size: 16px; }
button.theme { font: 500 12px var(--mono); color: var(--fg); background: var(--panel); border: 1px solid var(--rule);
  border-radius: 999px; padding: 6px 12px; cursor: pointer; }
button.theme:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.tag { justify-self: start; display: inline-block; font: 600 11px/1 var(--mono); letter-spacing: .03em; padding: 4px 7px; border-radius: 4px; white-space: nowrap; }
.tag.pgdb { color: var(--pgdb); background: var(--pgdb-bg); }
.tag.carveme { color: var(--carveme); background: var(--carveme-bg); }
.tag.gpr { color: var(--gpr); background: var(--gpr-bg); }
.methods { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; }
.method { background: var(--panel); border: 1px solid var(--rule); border-radius: 8px; padding: 16px; display: grid; gap: 8px; align-content: start; min-width: 0; }
.method dl { margin: 0; display: grid; grid-template-columns: max-content 1fr; gap: 4px 12px; font-size: 13.5px; }
.method dt { color: var(--muted); }
.method dd { margin: 0; min-width: 0; }
.scroll { overflow-x: auto; border: 1px solid var(--rule); border-radius: 8px; background: var(--panel); }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { padding: 6px 10px; text-align: left; border-bottom: 1px solid var(--rule); white-space: nowrap; }
thead th { font: 600 11.5px/1.3 var(--body); color: var(--muted); background: var(--panel); vertical-align: bottom; white-space: normal; min-width: 64px; }
tr.group th { font: 600 11px/1 var(--mono); text-transform: uppercase; letter-spacing: .05em; border-bottom: 2px solid currentColor; }
th.pgdb { color: var(--pgdb); } th.carveme { color: var(--carveme); } th.gpr { color: var(--gpr); }
table.num td:not(:first-child):not(:nth-child(2)), table.summary td:last-child { font-family: var(--mono); font-variant-numeric: tabular-nums; text-align: right; }
table.summary td:nth-child(3) { font-family: var(--mono); text-align: right; }
tbody tr:last-child td { border-bottom: 0; }
.iqr { color: var(--muted); font-size: 11.5px; }
.na { color: var(--muted); }
.pending { color: var(--pending); font-weight: 600; }
code { font: 12.5px var(--mono); }
ol.findings { margin: 0; padding-left: 22px; display: grid; gap: 10px; }
details summary { cursor: pointer; font-weight: 600; color: var(--accent); }
details[open] summary { margin-bottom: 10px; }
.note { font-size: 13px; color: var(--muted); }
ul.paths { margin: 0; padding-left: 20px; display: grid; gap: 4px; font-size: 13.5px; }
"""

SCRIPT = """
(function () {
  var root = document.documentElement, b = document.getElementById('theme');
  var modes = ['system', 'light', 'dark'];
  function apply(m) { if (m === 'system') root.removeAttribute('data-theme'); else root.setAttribute('data-theme', m); b.textContent = 'Theme: ' + m; }
  var m = 'system';
  try { m = localStorage.getItem('theme') || 'system'; } catch (e) {}
  apply(m);
  b.addEventListener('click', function () {
    m = modes[(modes.indexOf(m) + 1) % 3]; apply(m);
    try { localStorage.setItem('theme', m); } catch (e) {}
  });
})();
"""


def main(repo: Path, out: Path):
    inv = repo / "data" / "mm_compare" / "inventory"
    sm = pd.read_csv(repo / "data" / "mm_compare" / "sample_map.tsv", sep="\t")
    cm = pd.read_csv(inv / "carveme_models.tsv", sep="\t")
    pg = pd.read_csv(inv / "pgdb_models.tsv", sep="\t")
    gp = pd.read_csv(inv / "gpr_models.tsv", sep="\t")
    cami_gpr = (gp.dataset == "cami_hmp").any()
    page = f"""<title>Metabolic Reconstruction Inventory</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Literata:opsz,wght@7..72,600&family=Public+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap">
<style>{STYLE}</style>
<div class="wrap">
<header>
  <div class="top"><span class="eyebrow">metasmith · bench-E4 · mm_compare</span><button class="theme" id="theme" type="button">Theme: system</button></div>
  <h1>Metabolic Reconstruction Inventory</h1>
  <p class="lede">Three ways of turning a genome into gene-to-reaction knowledge, each applied as it stands to the
  49 CAMI II toy human microbiome samples and to six E. coli strains. Every count is in the method's own units.
  Nothing here is translated between methods, and nothing here is the comparison test.</p>
</header>

<section>
  <h2>The three methods</h2>
  <div class="methods">
    <div class="method">{tag("pgdb")}
      <dl><dt>Output</dt><dd>A Pathway Tools PGDB per genome and per community, from Ryan's MetaPathways run</dd>
      <dt>Speaks</dt><dd>MetaCyc reaction and pathway frames, EC numbers</dd>
      <dt>Counted</dt><dd>reactions, reactions with a gene, genes with a reaction, pathways</dd>
      <dt>Own score</dt><dd>PathoLogic pathway score (<code>PWY_SCORE</code>)</dd></dl></div>
    <div class="method">{tag("carveme")}
      <dl><dt>Output</dt><dd>A gap-filled SBML model per genome (CarveMe 1.6.6, E5's settings)</dd>
      <dt>Speaks</dt><dd>BiGG ids, annotated with MNXR, MetaCyc and EC</dd>
      <dt>Counted</dt><dd>reactions, reactions with a gene rule, genes, metabolites</dd>
      <dt>Own score</dt><dd>MEMOTE total</dd></dl></div>
    <div class="method">{tag("gpr")}
      <dl><dt>Output</dt><dd>A table of ORF-to-reaction calls from four annotation lanes (CLEAN, KOfam, ProteinBERT, UniRef50)</dd>
      <dt>Speaks</dt><dd>MetaNetX reactions (MNXR)</dd>
      <dt>Counted</dt><dd>ORFs with a call, calls, distinct MNXR, per lane</dd>
      <dt>Own score</dt><dd>a raw score per call, in each lane's own kind</dd></dl></div>
  </div>
</section>

<section>
  <h2>E. coli strains</h2>
  <p>The small-perturbation arm: six lab strains of one species, each from its RefSeq proteome. CarveMe and the
  4-lane read the same proteins, so their gene ids match.</p>
  {ecoli_section(cm, gp)}
  <p class="note">The E. coli PGDBs are not built yet. Choosing their pipeline is open.</p>
</section>

<section>
  <h2>CAMI II samples</h2>
  <p>49 samples in two studies: airways, skin and urogenital (29) and gastrointestinal and oral (20). The PGDBs
  describe the gold-standard genomes. CarveMe and the 4-lane describe the MAGs E5 binned from reads.</p>
  {cami_summary(cm, pg, gp)}
  <details><summary>Per sample</summary>{cami_samples(sm, cm, pg, gp)}</details>
  {"" if cami_gpr else '<p class="note">The 4-lane run on the CAMI MAGs is in progress on fir. Its counts land here when it finishes.</p>'}
</section>

<section>
  <h2>What the test session should know</h2>
  <ol class="findings">{FINDINGS}</ol>
</section>

<section>
  <h2>Where the data lives</h2>
  <ul class="paths">
    <li><code>data/mm_compare/manifest.tsv</code>: one row per output, with path and provenance</li>
    <li><code>data/mm_compare/inventory/</code>: the per-subject counts on this page</li>
    <li><code>research/mm_compare/</code>: the scripts that fetched, ran and counted</li>
  </ul>
  <p class="note">Branch <code>feat/engine/bench-E4</code> of metasmith. The data is pinned by <code>data/mm_compare.dvc</code>.</p>
</section>
</div>
<script>{SCRIPT}</script>
"""
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(page)


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
