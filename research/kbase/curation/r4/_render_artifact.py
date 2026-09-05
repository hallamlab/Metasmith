#!/usr/bin/env python3
"""Render the round-4 showcase from the probe's JSON records."""
import html, json, os
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
REC = [json.loads(l) for l in
       (REPO / "research/metasmith_libraries/kbase_parity.jsonl").read_text().splitlines() if l]

R4_KBASE = lambda p: p.startswith("kbase/")
R4_ASPIRE = {"diversity_analysis", "umap_clustering", "spieceasi", "graph_network",
             "measurement_association", "paired_group_contrast", "indicspecies",
             "network_modules", "asv_mag_link_absent"}
EXTENDED = {"functionalAnnotation/bakta_noncoding.py"}

def kind(path):
    name = path.split("/")[-1].replace(".py", "")
    if R4_KBASE(path): return "new"
    if path.startswith("aspire/") and name in R4_ASPIRE: return "lift"
    if path in EXTENDED: return "ext"
    return "old"

def short(path):
    return path.split("/")[-1].replace(".py", "")

def group(path):
    return path.split("/")[0]

e = html.escape

# ---------------------------------------------------------------- analyses copy
STORY = {
 "a1_model_from_isolate_reads": dict(
   n="1", tag="metabolic modelling",
   verdict="23 steps, one plan, reads to flux."),
 "a2_model_from_bigg": dict(
   n="2", tag="the control",
   verdict="Two steps. The shortest useful proof on this page."),
 "a3_finish_long_read_isolate": dict(
   n="3", tag="isolate finishing",
   verdict="7 steps &mdash; and the first analysis to fail, for a reason worth reading."),
 "a4_mags_from_metagenome": dict(
   n="4", tag="metagenome binning",
   verdict="13 steps, five of them the same filter. Valid, not minimal."),
 "a5_community_structure": dict(
   n="5", tag="community ecology",
   verdict="7 steps from shotgun reads, with no ASPIRE pipeline anywhere in it."),
 "a5b_cooccurrence_network": dict(
   n="5b", tag="community ecology",
   verdict="8 steps &mdash; after five more rows came off the gate."),
 "a6_expression_response": dict(
   n="6", tag="transcriptomics",
   verdict="8 steps. Bacterial RNA-seq, end to end."),
 "a6b_functional_enrichment": dict(
   n="6b", tag="transcriptomics",
   verdict="5 steps, split off from 6 on purpose."),
 "a7_compare_strains": dict(
   n="7", tag="comparative genomics",
   verdict="10 steps, two independent readouts on one strain set."),
 "a8_gene_presence": dict(
   n="8", tag="comparative genomics",
   verdict="3 steps. The whole analysis is two new transforms and an ORF caller."),
 "a9_strain_variants": dict(
   n="9", tag="variation",
   verdict="7 steps, and the alignment it calls on is the one the library already made."),
}

def chain(picked):
    out = []
    for i, p in enumerate(picked):
        k = kind(p)
        if i: out.append('<span class="sep" aria-hidden="true">&rsaquo;</span>')
        out.append(
            f'<span class="step {k}"><span class="grp">{e(group(p))}/</span>{e(short(p))}</span>')
    return "".join(out)

cards = []
for r in REC:
    s = STORY[r["id"]]
    cards.append(f'''
<article class="an" id="{e(r['id'])}">
  <div class="rail">
    <div class="anno"><span class="num">{s['n']}</span><span class="tag">{s['tag']}</span></div>
    <h3>{e(r['question'])}</h3>
    <dl class="io">
      <dt>You supply</dt><dd>{e(r['supplies'])}</dd>
      <dt>You get</dt><dd>{e(r['products'])}</dd>
    </dl>
    <div class="meta">
      <span class="ok">solved</span>
      <span class="n">{r['steps']} steps</span>
      <span class="t">{r['seconds']:.2f}s</span>
    </div>
  </div>
  <div class="body">
    <div class="chain">{chain(r['picked'])}</div>
    <p class="verdict">{s['verdict']}</p>
    <p class="libs">planned over {e(", ".join(r['libraries']))}
      &middot; targets {e(", ".join(t.split("::")[-1] for t in r['targets']))}</p>
  </div>
</article>''')

CARDS = "\n".join(cards)

# ---------------------------------------------------------------- the page
PAGE = f'''<title>Nine Analyses, Planned</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@400;500;600;700&family=IBM+Plex+Serif:ital,wght@0,400;0,500;1,400&display=swap">
<style>
:root{{
  --ground:#F6F7F4; --surface:#FFFFFF; --sunk:#EEF1EC;
  --ink:#141A18; --ink-2:#3A433F; --muted:#68726D;
  --rule:#D8DED7; --rule-soft:#E6EAE4;
  --new:#7A3E6B; --new-bg:#F4EAF1; --new-rule:#D9BCD1;
  --lift:#1F6E8C; --lift-bg:#E6F1F5; --lift-rule:#AFCEDB;
  --ext:#8C5A16; --ext-bg:#F6EEE0;
  --ok:#0E6B54; --ok-bg:#E6F2ED;
  --flag:#9A4527; --flag-bg:#F7EBE5;
  --shadow:0 1px 0 rgba(20,26,24,.04);
}}
@media (prefers-color-scheme: dark){{
  :root:not([data-theme="light"]){{
    --ground:#0E1211; --surface:#161B19; --sunk:#111615;
    --ink:#E9EEEA; --ink-2:#C2CBC6; --muted:#8B958F;
    --rule:#28302D; --rule-soft:#1F2624;
    --new:#C58CB4; --new-bg:#241A21; --new-rule:#4A3243;
    --lift:#7FBBD4; --lift-bg:#132126; --lift-rule:#294653;
    --ext:#DDA95C; --ext-bg:#2A2013;
    --ok:#5CC5A2; --ok-bg:#0F2620;
    --flag:#E29070; --flag-bg:#2A1913;
    --shadow:none;
  }}
}}
:root[data-theme="dark"]{{
  --ground:#0E1211; --surface:#161B19; --sunk:#111615;
  --ink:#E9EEEA; --ink-2:#C2CBC6; --muted:#8B958F;
  --rule:#28302D; --rule-soft:#1F2624;
  --new:#C58CB4; --new-bg:#241A21; --new-rule:#4A3243;
  --lift:#7FBBD4; --lift-bg:#132126; --lift-rule:#294653;
  --ext:#DDA95C; --ext-bg:#2A2013;
  --ok:#5CC5A2; --ok-bg:#0F2620;
  --flag:#E29070; --flag-bg:#2A1913;
  --shadow:none;
}}
*{{box-sizing:border-box}}
body{{
  background:var(--ground); color:var(--ink);
  font-family:"IBM Plex Sans Condensed","Helvetica Neue",Arial,sans-serif;
  font-size:16px; line-height:1.5; -webkit-font-smoothing:antialiased;
}}
.wrap{{max-width:1060px; margin:0 auto; padding:0 28px 96px}}

header.mast{{padding:56px 0 0}}
.eyebrow{{
  font-family:"IBM Plex Mono",ui-monospace,monospace; font-size:11.5px; font-weight:500;
  letter-spacing:.14em; text-transform:uppercase; color:var(--muted);
}}
h1{{
  font-weight:700; font-size:clamp(2.6rem,6.4vw,4.1rem); line-height:1.02;
  letter-spacing:-.015em; margin:14px 0 0; text-wrap:balance;
}}
.standfirst{{
  font-family:"IBM Plex Serif",Georgia,serif; font-size:1.075rem; line-height:1.62;
  color:var(--ink-2); max-width:64ch; margin:20px 0 0;
}}
.standfirst em{{color:var(--ink); font-style:italic}}
.figures{{
  display:flex; flex-wrap:wrap; margin:34px 0 0;
  border-top:1px solid var(--rule); border-bottom:1px solid var(--rule);
}}
.fig{{flex:1 1 140px; padding:14px 20px 15px; border-left:1px solid var(--rule-soft)}}
.fig:first-child{{border-left:0; padding-left:0}}
.fig b{{
  display:block; font-family:"IBM Plex Mono",monospace; font-weight:600;
  font-size:1.55rem; letter-spacing:-.02em; font-variant-numeric:tabular-nums;
}}
.fig span{{
  display:block; font-size:12px; letter-spacing:.06em; text-transform:uppercase;
  color:var(--muted); margin-top:3px;
}}
.legend{{
  margin:24px 0 0; padding:16px 20px; background:var(--sunk);
  border:1px solid var(--rule-soft);
  display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:12px 28px;
}}
.legend .lrow{{display:flex; flex-wrap:wrap; align-items:center; gap:6px 9px;
  font-size:13.5px; color:var(--muted)}}
.legend .step{{white-space:normal}}

h2.sec{{
  font-size:1.5rem; font-weight:700; letter-spacing:-.01em; margin:0 0 6px;
}}
section.block{{margin:60px 0 0; padding-top:32px; border-top:2px solid var(--ink)}}
section.block > p.intro{{
  font-family:"IBM Plex Serif",Georgia,serif; color:var(--ink-2);
  max-width:66ch; margin:0 0 26px; font-size:1rem; line-height:1.62;
}}

/* --- analysis cards --- */
article.an{{
  display:grid; grid-template-columns:19rem 1fr; gap:0 34px;
  padding:26px 0 28px; border-bottom:1px solid var(--rule-soft);
}}
article.an:last-child{{border-bottom:0}}
.anno{{display:flex; align-items:baseline; gap:10px}}
.num{{
  font-family:"IBM Plex Mono",monospace; font-size:12px; font-weight:600;
  color:var(--ink); border:1px solid var(--rule); padding:1px 6px;
}}
.tag{{
  font-size:10.5px; font-weight:700; letter-spacing:.13em; text-transform:uppercase;
  color:var(--muted);
}}
.rail h3{{
  font-family:"IBM Plex Serif",Georgia,serif; font-weight:400; font-style:italic;
  font-size:1.12rem; line-height:1.42; margin:10px 0 0; color:var(--ink);
  text-wrap:balance;
}}
dl.io{{margin:14px 0 0; font-size:13px}}
dl.io dt{{
  font-size:10px; font-weight:700; letter-spacing:.13em; text-transform:uppercase;
  color:var(--muted); margin-top:9px;
}}
dl.io dd{{margin:2px 0 0; color:var(--ink-2); line-height:1.45}}
.meta{{display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-top:14px}}
.meta .ok{{
  font-family:"IBM Plex Mono",monospace; font-size:10.5px; font-weight:600;
  letter-spacing:.1em; text-transform:uppercase; color:var(--ok);
  background:var(--ok-bg); border:1px solid var(--ok); padding:2px 7px;
}}
.meta .n,.meta .t{{
  font-family:"IBM Plex Mono",monospace; font-size:12px; color:var(--muted);
  font-variant-numeric:tabular-nums;
}}
.chain{{
  display:flex; flex-wrap:wrap; align-items:center; gap:5px 3px;
  padding:13px 14px; background:var(--surface); border:1px solid var(--rule);
  box-shadow:var(--shadow);
}}
.step{{
  font-family:"IBM Plex Mono",monospace; font-size:12.5px; line-height:1.35;
  padding:4px 8px; white-space:nowrap; color:var(--ink-2); background:var(--sunk);
  border:1px solid transparent;
}}
.step .grp{{opacity:.5}}
.step.new{{color:var(--new); background:var(--new-bg); border-color:var(--new-rule); font-weight:500}}
.step.lift{{color:var(--lift); background:var(--lift-bg); border-color:var(--lift-rule); font-weight:500}}
.step.ext{{color:var(--ext); background:var(--ext-bg); border-color:currentColor}}
.sep{{font-family:"IBM Plex Mono",monospace; color:var(--muted); opacity:.55; padding:0 1px}}
p.verdict{{
  font-family:"IBM Plex Serif",Georgia,serif; font-size:15.5px; line-height:1.62;
  color:var(--ink); margin:14px 0 0; max-width:66ch;
}}
p.libs{{
  font-family:"IBM Plex Mono",monospace; font-size:11.5px; color:var(--muted);
  line-height:1.55; margin:9px 0 0;
}}

/* --- findings --- */
.finding{{padding:22px 0 4px; border-top:1px solid var(--rule-soft)}}
.finding:first-of-type{{border-top:0; padding-top:0}}
.finding h3{{font-size:1.06rem; font-weight:600; margin:0 0 4px}}
.finding .where{{
  font-family:"IBM Plex Mono",monospace; font-size:11.5px; color:var(--flag);
  letter-spacing:.04em; text-transform:uppercase; font-weight:600; margin:0 0 9px;
}}
.finding p{{
  font-family:"IBM Plex Serif",Georgia,serif; font-size:15.5px; line-height:1.66;
  color:var(--ink-2); margin:0 0 11px; max-width:68ch;
}}
.finding p:last-child{{margin-bottom:0}}
.finding code, code.t{{
  font-family:"IBM Plex Mono",monospace; font-size:.88em; color:var(--ink);
  background:var(--sunk); padding:1px 4px;
}}
pre.code{{
  font-family:"IBM Plex Mono",monospace; font-size:12.5px; line-height:1.6;
  background:var(--surface); border:1px solid var(--rule); padding:12px 14px;
  overflow-x:auto; margin:0 0 12px; color:var(--ink-2);
}}
pre.code b{{color:var(--flag); font-weight:600}}

figure{{margin:0 0 26px}}
figure svg{{
  display:block; width:100%; height:auto; background:var(--surface);
  border:1px solid var(--rule);
}}
figcaption{{
  font-family:"IBM Plex Serif",Georgia,serif; font-size:13.5px; color:var(--muted);
  line-height:1.55; margin-top:11px; max-width:70ch;
}}
.svg-b{{fill:var(--surface); stroke:var(--rule)}}
.svg-t{{font-family:"IBM Plex Mono",monospace; font-size:12px; fill:var(--ink-2)}}
.svg-t.lbl{{font-size:10.5px; fill:var(--muted)}}
.svg-t.hd{{font-family:"IBM Plex Sans Condensed",sans-serif; font-size:11px;
  font-weight:700; letter-spacing:.12em; fill:var(--muted)}}
.svg-gate{{fill:var(--flag-bg); stroke:var(--flag)}}
.svg-gate-t{{font-family:"IBM Plex Mono",monospace; font-size:12px; fill:var(--flag)}}
.svg-new{{fill:var(--new-bg); stroke:var(--new-rule)}}
.svg-new-t{{font-family:"IBM Plex Mono",monospace; font-size:12px; fill:var(--new)}}
.svg-line{{stroke:var(--muted); fill:none}}
.svg-cut{{stroke:var(--flag); fill:none; stroke-dasharray:4 3}}

table.fold{{
  border-collapse:collapse; width:100%; font-size:13px; margin:0 0 6px;
  background:var(--surface); border:1px solid var(--rule);
}}
table.fold th{{
  font-size:10.5px; font-weight:700; letter-spacing:.11em; text-transform:uppercase;
  color:var(--muted); text-align:left; padding:9px 12px;
  border-bottom:1px solid var(--rule);
}}
table.fold td{{
  padding:8px 12px; border-bottom:1px solid var(--rule-soft); vertical-align:top;
  font-family:"IBM Plex Mono",monospace; font-size:12.5px; line-height:1.5;
}}
table.fold tr:last-child td{{border-bottom:0}}
table.fold td.why{{
  font-family:"IBM Plex Serif",Georgia,serif; font-size:13.5px; color:var(--muted);
}}
table.fold td.gone{{color:var(--flag)}}

footer.colophon{{
  margin-top:56px; padding-top:18px; border-top:1px solid var(--rule);
  font-family:"IBM Plex Mono",monospace; font-size:11.5px; color:var(--muted);
  line-height:1.75;
}}
@media (max-width:820px){{
  .wrap{{padding:0 18px 64px}}
  article.an{{grid-template-columns:1fr; gap:16px}}
}}
@media (prefers-reduced-motion:reduce){{*{{animation:none!important; transition:none!important}}}}
a{{color:inherit}}
a:focus-visible,[tabindex]:focus-visible{{outline:2px solid var(--ok); outline-offset:2px}}
</style>

<div class="wrap">

<header class="mast">
  <div class="eyebrow">metasmith &middot; kbase port &middot; curation round 4</div>
  <h1>Nine Analyses, Planned</h1>
  <p class="standfirst">
    Round 3 proposed 22 transforms and measured parity against a table of KBase task verbs.
    A verb table is not a plan. This round wrote the transforms, tightened the ASPIRE lane
    they sit beside, and then asked the planner the only question that settles it:
    <em>given the inputs a lab actually has, does the chain connect?</em>
    Eleven analyses, each named for the question it answers. Every chain below is the plan
    the solver produced, not one drawn by hand.
  </p>
  <div class="figures">
    <div class="fig"><b>21</b><span>new transforms</span></div>
    <div class="fig"><b>28</b><span>new types</span></div>
    <div class="fig"><b>11</b><span>ASPIRE rows lifted</span></div>
    <div class="fig"><b>11/11</b><span>analyses solve</span></div>
    <div class="fig"><b>0</b><span>shipped templates moved</span></div>
  </div>
  <div class="legend">
    <div class="lrow"><span class="step new"><span class="grp">kbase/</span>cobra_fba</span><span>new in round 4</span></div>
    <div class="lrow"><span class="step lift"><span class="grp">aspire/</span>spieceasi</span><span>lifted out of the ASPIRE gate</span></div>
    <div class="lrow"><span class="step ext"><span class="grp">functionalAnnotation/</span>bakta_noncoding</span><span>extended</span></div>
    <div class="lrow"><span class="step"><span class="grp">assembly/</span>megahit</span><span>already shipping</span></div>
  </div>
</header>

<section class="block">
  <h2 class="sec">The eleven chains</h2>
  <p class="intro">
    Each is one <code class="t">Spec.Solve()</code> over only the transform groups that analysis
    needs &mdash; a solve over the whole library is not a harder test, it is a meaningless one,
    because a library with pure sources answers every target with &ldquo;import it from
    staging.&rdquo; Steps are shown in the order the plan runs them.
  </p>
{CARDS}
</section>

<section class="block">
  <h2 class="sec">What the planner refused, and why</h2>
  <p class="intro">
    Two analyses did not solve on the first pass. Both failures were real, both were fixed,
    and they are the most useful thing this round produced &mdash; a verb table could not have
    surfaced either one.
  </p>

  <div class="finding">
    <div class="where">a3 &middot; 0 steps &middot; no_plan_at_all</div>
    <h3>A polisher that could never run</h3>
    <pre class="code">reads = model.AddRequirement(lib.GetType("sequences::clean_short_reads"),
                             <b>parents={{asm}}</b>)   # &larr; backwards</pre>
    <p>
      <code>polypolish</code> declared its short reads as a <em>child</em> of the assembly.
      No read set in this library descends from an assembly &mdash; the arrow runs the other
      way &mdash; so the requirement had no candidates, and the target went with it. So did
      every other target in the analysis: one unreachable target drops the whole plan, and the
      error names four dropped types, none of which is the actual problem.
    </p>
    <p>
      Fixed to the idiom the library already uses for &ldquo;these came from the same
      sample&rdquo;: a shared <code>sequences::read_metadata</code> ancestor, exactly as
      <code>assembly_stats</code> and <code>megahit</code> do it. The reads and the assembly
      are siblings under the metadata node, not parent and child.
    </p>
    <p>
      The second half has no fix yet. A hybrid assembly joins a long-read set to a short-read
      library, and <em>nothing in this library sits above <code>read_metadata</code> to say
      they are the same isolate.</em> a3 supplies one metadata node covering both, with
      <code>length_class: hybrid</code> &mdash; honest as a value, but the node is doing a job
      it was not named for. A sample-level grouping type is the real answer and a library-wide
      decision.
    </p>
  </div>

  <div class="finding">
    <div class="where">a5b &middot; 0 steps &middot; no_plan_at_all</div>
    <h3>Lifting a transform does not lift its inputs</h3>
    <p>
      The first refactor took the six community-ecology transforms off
      <code>aspire::run</code> and hung them on a generic <code>amplicon::survey</code> node.
      Four then solved from a bare count table. <code>spieceasi</code> and
      <code>graph_network</code> did not, because their own inputs were still produced only
      inside the pipeline: the gate had moved, not gone.
    </p>
    <p>
      Five more rows came off the same way &mdash; <code>indicspecies</code> and its off-arm,
      <code>network_modules</code> and its off-arm, and <code>asv_mag_link_absent</code>.
      <code>asv_mag_link</code> itself stays gated on purpose: it reads
      <code>aspire::asv_filtered_seqs</code>, which nothing outside ASPIRE produces. Linking
      ASVs to MAGs remains a pipeline capability; declining to link them does not.
    </p>
  </div>

  <div class="finding">
    <div class="where">a4 &middot; solved &middot; 13 steps, 5 of them identical</div>
    <h3>Valid plans that are longer than the work</h3>
    <p>
      a4 runs <code>seqkit_filter_contigs</code> five times. Asking for the assembly and the
      filtered assembly alone gives exactly one filter step; adding CheckM over the bins is
      what multiplies it. The type hierarchy is not the cause &mdash;
      <code>filtered_assembly IsA assembly</code> holds and <code>IsA putative_genome</code>
      does not, so no filter output can masquerade as a bin. Every instance produces the same
      file from the same assembly.
    </p>
    <p>
      a1 grew a polish step nobody asked for, for the mirror-image reason: fixing
      <code>polypolish</code> made it reachable, and reachable means it is a candidate producer
      of <code>sequences::assembly</code> everywhere an assembly is wanted. Both plans are
      correct and neither is minimal. There is no way in this language to say
      <em>not that one</em>.
    </p>
  </div>
</section>

<section class="block">
  <h2 class="sec">The ecology lift</h2>
  <p class="intro">
    Five KBase task verbs &mdash; transform a matrix, ordinate, correlate, test, build a
    network &mdash; were already performed by transforms in this library, and none of them
    could be reached. Every one was gated on <code class="t">aspire::run</code>, the node that
    says &ldquo;this came from an ASPIRE pipeline.&rdquo; The fix is not to delete the gate:
    a fan-in genuinely needs a grouping parent its inputs descend from. The fix is to
    generalise it.
  </p>
  <figure>
    <svg viewBox="0 0 980 330" role="img" aria-label="Before, six ecology transforms require aspire::run, which only the ASPIRE pipeline produces. After, they require amplicon::survey, which an ASPIRE run satisfies and which a kraken-based survey also provides.">
      <defs>
        <marker id="ar" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
          <polygon points="0,1 8,4 0,7" fill="currentColor"/>
        </marker>
      </defs>
      <g class="svg-line" color="var(--muted)">
      <text x="24" y="30" class="svg-t hd">BEFORE</text>
      <rect x="24" y="46" width="196" height="40" rx="2" class="svg-b" stroke-width="1"/>
      <text x="122" y="71" class="svg-t" text-anchor="middle">ASPIRE pipeline</text>
      <line x1="220" y1="66" x2="286" y2="66" stroke-width="1.2" marker-end="url(#ar)"/>
      <rect x="292" y="46" width="150" height="40" rx="2" class="svg-gate" stroke-width="1.4"/>
      <text x="367" y="71" class="svg-gate-t" text-anchor="middle">aspire::run</text>
      <line x1="442" y1="66" x2="508" y2="66" stroke-width="1.2" marker-end="url(#ar)"/>
      <rect x="514" y="40" width="234" height="52" rx="2" class="svg-b" stroke-width="1"/>
      <text x="631" y="61" class="svg-t" text-anchor="middle">six ecology transforms</text>
      <text x="631" y="79" class="svg-t lbl" text-anchor="middle">diversity, ordination, network, tests</text>
      <rect x="24" y="112" width="196" height="36" rx="2" class="svg-b" stroke-width="1"/>
      <text x="122" y="135" class="svg-t" text-anchor="middle">any other count table</text>
      <path d="M220 130 L 286 130" class="svg-cut" stroke-width="1.4"/>
      <line x1="264" y1="118" x2="284" y2="142" stroke="var(--flag)" stroke-width="1.6"/>
      <line x1="284" y1="118" x2="264" y2="142" stroke="var(--flag)" stroke-width="1.6"/>
      <text x="300" y="135" class="svg-t lbl" fill="var(--flag)">no route in</text>

      <line x1="24" y1="182" x2="956" y2="182" stroke="var(--rule)" stroke-width="1"/>

      <text x="24" y="216" class="svg-t hd">AFTER</text>
      <rect x="24" y="232" width="196" height="36" rx="2" class="svg-b" stroke-width="1"/>
      <text x="122" y="255" class="svg-t" text-anchor="middle">ASPIRE pipeline</text>
      <rect x="24" y="278" width="196" height="36" rx="2" class="svg-new" stroke-width="1"/>
      <text x="122" y="301" class="svg-new-t" text-anchor="middle">kraken_abundance</text>
      <path d="M220 250 L 268 250 L 284 262" stroke-width="1.2" marker-end="url(#ar)"/>
      <path d="M220 296 L 268 296 L 284 284" stroke-width="1.2" marker-end="url(#ar)"/>
      <rect x="292" y="253" width="150" height="40" rx="2" class="svg-new" stroke-width="1.4"/>
      <text x="367" y="278" class="svg-new-t" text-anchor="middle">amplicon::survey</text>
      <line x1="442" y1="273" x2="508" y2="273" stroke-width="1.2" marker-end="url(#ar)"/>
      <rect x="514" y="247" width="234" height="52" rx="2" class="svg-b" stroke-width="1"/>
      <text x="631" y="268" class="svg-t" text-anchor="middle">the same six transforms</text>
      <text x="631" y="286" class="svg-t lbl" text-anchor="middle">unchanged bodies, unchanged edges</text>
      <text x="770" y="264" class="svg-t lbl">a run IS a survey,</text>
      <text x="770" y="280" class="svg-t lbl">so nothing inside</text>
      <text x="770" y="296" class="svg-t lbl">ASPIRE changed</text>
      </g>
    </svg>
    <figcaption>
      <code class="t">aspire::run</code> now carries <code class="t">amplicon::survey</code>'s
      property, so an ASPIRE run still satisfies every requirement it did before &mdash; the
      shipped amplicon template's plan is unchanged &mdash; while a kraken-based survey of
      shotgun reads reaches the same six transforms for the first time. That is analysis 5.
    </figcaption>
  </figure>
</section>

<section class="block">
  <h2 class="sec">The ASPIRE fold</h2>
  <p class="intro">
    The ASPIRE lane is a one-for-one port of a Nextflow pipeline, so it still described how
    that pipeline staged its files rather than what the analysis does. A curation pass folded
    the logistics into the transforms that consume them. The measurement is the shipped
    amplicon template, which runs this lane: <strong>18 steps to 15</strong>, with every
    remaining step identical.
  </p>
  <table class="fold">
    <thead><tr><th>Row removed</th><th>Folded into</th><th>Why it was not a result</th></tr></thead>
    <tbody>
      <tr><td class="gone">dereplicate</td><td>denoise</td>
          <td class="why">Dereplication is how DADA2 is fed, not a thing anyone asks for.</td></tr>
      <tr><td class="gone">merge_reads</td><td>merge_and_filter_reads</td>
          <td class="why">Merging and filtering were one decision split across two processes.</td></tr>
      <tr><td class="gone">filter_reads</td><td>merge_and_filter_reads</td>
          <td class="why">&mdash;</td></tr>
      <tr><td class="gone">prepare_blast_databases</td><td>mitomaster</td>
          <td class="why">Indexing a reference FASTA is staging; the decontamination is the result.</td></tr>
      <tr><td class="gone">master_summary_optional_slot</td><td>&mdash; deleted</td>
          <td class="why">A slot that was always empty. It never appeared in a plan.</td></tr>
    </tbody>
  </table>
  <figcaption>
    50 rows to 46, 138 types to 132. Everything a researcher would name as a target was kept,
    including the eight policy off-arms &mdash; deleting one of those removes the ability to
    turn a stage off, which is a capability rather than clutter.
  </figcaption>
</section>

<section class="block">
  <h2 class="sec">What did not move</h2>
  <p class="intro">
    Adding 21 producers to a library changes what every existing target could be answered by.
    Four requirements became ambiguous for the first time &mdash;
    <code class="t">read_qc_stats</code> went from one producer to three,
    <code class="t">gbk</code> and <code class="t">stringtie_quant_gtf</code> from one to two,
    and <code class="t">metabolic_model</code> from none to three &mdash; and two more got
    wider. None of it changed a plan.
  </p>
  <p class="intro">
    All eleven shipped templates were re-solved and compared step for step against
    <code class="t">HEAD</code>, not merely by step count: ten are byte-identical, and the
    eleventh is the amplicon lane, three steps shorter, for the three folds named above.
    Total solve time went 26 seconds to 27. The static gates &mdash; 40 compatibility checks
    and 27 type-hierarchy tests &mdash; pass.
  </p>
  <p class="intro">
    That is a weaker guarantee than it looks. The templates pin their targets by lineage;
    an unpinned target downstream of any of those four is now free to be answered from either
    producer, and the symptom is a split plan rather than an error. It is exactly what a1's
    unasked-for polish step is.
  </p>
</section>

<footer class="colophon">
  metasmith &middot; kbase/dev &middot; curation round 4<br>
  chains rendered from research/metasmith_libraries/kbase_parity.jsonl &mdash; one record per solve<br>
  probe: research/metasmith_libraries/probe_kbase_parity.py &middot;
  findings: research/kbase/curation/r4/{{analyses,template_gate,aspire_topology}}.md<br>
  every transform body in transforms/kbase/ is a stub that touches its outputs; the signatures are real, the protocols are next
</footer>

</div>
'''

out = Path(os.environ.get("MSM_ARTIFACT_OUT", Path(__file__).resolve().parent / "r4_showcase.html"))
out.write_text(PAGE)
print("wrote", out, len(PAGE), "bytes")
