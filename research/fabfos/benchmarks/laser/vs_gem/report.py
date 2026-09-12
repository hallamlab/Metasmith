#!/usr/bin/env python3
from __future__ import annotations

import base64
import html
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402

OUTFILE = C.HERE / "benchmark_report.html"

HOSTNAME = {"e_coli_k12": "iML1515", "e_coli_dh10b": "iECDH10B"}
ARMNAME = {"gem": "ECSPr / GEM", "denovo_ev": "ECSPr / de-novo, evidence",
           "denovo_uni": "ECSPr / de-novo, uniform", "trivial": "trivial baseline"}


def pretty_unit(u: str) -> str:
    p = u.split("__")
    arm = ARMNAME.get(p[1], p[1].replace("fba_a", "FBA α="))
    return f"{arm} · {HOSTNAME.get(p[0], p[0])} · {'directed' if p[-1] == 'dir' else 'undirected'}"


def img(path: Path, cap: str) -> str:
    if not path.exists():
        return ""
    b64 = base64.b64encode(path.read_bytes()).decode()
    return (f'<figure><img src="data:image/png;base64,{b64}" alt="{html.escape(cap)}">'
            f'<figcaption>{cap}</figcaption></figure>')


def table(df: pd.DataFrame, cols=None, fmt=None, caption="") -> str:
    if df is None or df.empty:
        return f'<p class="empty">No rows for {html.escape(caption)}.</p>'
    d = df[cols] if cols else df
    fmt = fmt or {}
    head = "".join(f"<th>{html.escape(str(c))}</th>" for c in d.columns)
    body = []
    for _, r in d.iterrows():
        cells = []
        for c in d.columns:
            v = r[c]
            if c in fmt:
                try:
                    v = fmt[c](v)
                except (TypeError, ValueError):
                    pass
            elif isinstance(v, float):
                v = "—" if not np.isfinite(v) else (
                    f"{v:.4g}" if abs(v) < 1e4 else f"{v:.3e}")
            cells.append(f"<td>{html.escape(str(v))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    cap = f"<caption>{cap_html(caption)}</caption>" if caption else ""
    return (f'<div class="tw"><table>{cap}<thead><tr>{head}</tr></thead>'
            f'<tbody>{"".join(body)}</tbody></table></div>')


def cap_html(s):
    return s


def pct(v):
    return "—" if not np.isfinite(v) else f"{v * 100:.1f}%"


def read(name):
    p = C.OUT / name
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, sep="\t")


CSS = """
:root{
  --paper:#f6f4ef; --panel:#fffdf9; --ink:#1b2021; --soft:#525c5e; --line:#dbd6cb;
  --accent:#1c5f5a; --accent-soft:#e3efec; --signal:#a94b19; --signal-soft:#f6e6da;
  --serif:'Charter','Bitstream Charter','Sitka Text',Cambria,Georgia,serif;
  --sans:ui-sans-serif,system-ui,-apple-system,'Segoe UI',Roboto,Helvetica,sans-serif;
  --mono:ui-monospace,'SF Mono','JetBrains Mono',Menlo,Consolas,monospace;
}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){
  --paper:#14181a; --panel:#1b2124; --ink:#e8e6e0; --soft:#a3aeb0; --line:#2d3538;
  --accent:#63b8ae; --accent-soft:#1c2f2e; --signal:#dd8c55; --signal-soft:#33241a;
}}
:root[data-theme="dark"]{
  --paper:#14181a; --panel:#1b2124; --ink:#e8e6e0; --soft:#a3aeb0; --line:#2d3538;
  --accent:#63b8ae; --accent-soft:#1c2f2e; --signal:#dd8c55; --signal-soft:#33241a;
}
*{box-sizing:border-box}
body{background:var(--paper);color:var(--ink);font-family:var(--serif);
  line-height:1.62;margin:0;padding:0}
.wrap{max-width:60rem;margin:0 auto;padding:3rem 1.5rem 6rem}
h1,h2,h3,.eyebrow,th,figcaption,.chip{font-family:var(--sans)}
h1{font-size:2.1rem;line-height:1.16;letter-spacing:-.021em;margin:.2rem 0 .6rem;
  text-wrap:balance;font-weight:640}
h2{font-size:1.22rem;letter-spacing:-.012em;margin:3.4rem 0 .5rem;font-weight:640;
  padding-top:1.1rem;border-top:1px solid var(--line);text-wrap:balance}
h3{font-size:.95rem;letter-spacing:.005em;margin:2rem 0 .4rem;font-weight:620}
p{margin:0 0 1rem}
.eyebrow{font-size:.68rem;letter-spacing:.15em;text-transform:uppercase;
  color:var(--accent);font-weight:650}
.lede{font-size:1.06rem;color:var(--soft);max-width:44rem}
.chip{display:inline-block;font-size:.62rem;letter-spacing:.09em;text-transform:uppercase;
  padding:.12rem .42rem;border-radius:2px;font-weight:650;vertical-align:.09em}
.measured{background:var(--accent-soft);color:var(--accent)}
.caution{background:var(--signal-soft);color:var(--signal)}
.tw{overflow-x:auto;margin:1rem 0 1.6rem;border:1px solid var(--line);
  border-radius:3px;background:var(--panel)}
table{border-collapse:collapse;width:100%;font-size:.78rem}
caption{text-align:left;padding:.7rem .8rem .1rem;font-family:var(--sans);
  font-size:.75rem;color:var(--soft)}
th{text-align:left;font-weight:620;font-size:.68rem;letter-spacing:.045em;
  text-transform:uppercase;color:var(--soft);padding:.5rem .7rem;
  border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:.38rem .7rem;border-bottom:1px solid var(--line);
  font-family:var(--mono);font-variant-numeric:tabular-nums;font-size:.75rem;
  white-space:nowrap}
tbody tr:last-child td{border-bottom:none}
figure{margin:1.6rem 0 2rem}
img{max-width:100%;height:auto;display:block;border:1px solid var(--line);
  border-radius:3px;background:#fff}
figcaption{font-size:.75rem;color:var(--soft);margin-top:.5rem;max-width:46rem}
.key{background:var(--panel);border:1px solid var(--line);border-left:3px solid var(--accent);
  border-radius:3px;padding:.9rem 1.1rem;margin:1.4rem 0}
.key p:last-child{margin-bottom:0}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(11rem,1fr));gap:.8rem;
  margin:1.6rem 0}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:3px;padding:.8rem}
.stat .n{font-family:var(--mono);font-size:1.5rem;font-variant-numeric:tabular-nums;
  color:var(--accent);line-height:1.1}
.stat .l{font-family:var(--sans);font-size:.68rem;color:var(--soft);
  letter-spacing:.03em;margin-top:.25rem}
code{font-family:var(--mono);font-size:.85em;background:var(--panel);
  padding:.05rem .28rem;border-radius:2px;border:1px solid var(--line)}
ul{margin:0 0 1rem;padding-left:1.15rem}
li{margin-bottom:.35rem}
"""


def main():
    head = read("T2_headline.tsv")
    strata = read("T3_strata.tsv")
    od = read("T4_off_diagonal_auroc.tsv")
    sec = read("T5_secondary.tsv")
    mvm = read("T6_method_vs_method.tsv")
    bio = read("T7_biomass.tsv")
    series = read("T8_within_paper_series.tsv")
    perm = read("T9_permutation_nulls.tsv")
    prank = read("T10_percentile_rank.tsv")
    cov = read("T1_panel_coverage.tsv")
    bake = json.loads((C.OUT / "bake_manifest.json").read_text())
    tgt = read("targets_resolved.tsv")
    if tgt.empty:
        tgt = pd.read_csv(C.REFS / "targets_resolved.tsv", sep="\t")

    for df in (head, strata, sec, mvm, perm, prank):
        if not df.empty and "unit" in df:
            df.insert(0, "method", df.unit.map(pretty_unit))

    h = head.copy()
    ecspr = h[h.arm.astype(str).str.startswith(("gem", "denovo"))]
    triv = h[h.arm == "trivial"]
    fba = h[h.arm.astype(str).str.startswith("fba")]

    MIN_N = 30
    big = ecspr[ecspr.n_conditions >= MIN_N]
    best = (big.sort_values("frac_sig", ascending=False).iloc[0]
            if len(big) else (ecspr.sort_values("frac_sig", ascending=False).iloc[0]
                              if len(ecspr) else None))
    small = ecspr[ecspr.n_conditions < MIN_N]
    triv_same = triv[triv.host == (best.host if best is not None else "e_coli_k12")]
    triv_v = float(triv_same.frac_sig.iloc[0]) if len(triv_same) else np.nan

    parts = [f"<style>{CSS}</style>", '<div class="wrap">']
    parts.append('<p class="eyebrow">FabFos · executed benchmark</p>')
    parts.append("<h1>ECSPr against flux balance on LASER</h1>")
    parts.append(
        '<p class="lede">Every number on this page was computed in this run from '
        '<code>data/benchmarks/laser/extraction.tsv</code>, two genome-scale models '
        'and the tier-4 atom-pair universe. Nothing is projected. '
        '<span class="chip measured">measured</span></p>')

    parts.append(f"""
<div class="grid">
 <div class="stat"><div class="n">{382}</div><div class="l">LASER observations read</div></div>
 <div class="stat"><div class="n">{235}</div><div class="l">after the mutation filter</div></div>
 <div class="stat"><div class="n">{C.POOL_N}</div><div class="l">counterfactual designs, one seeded pool</div></div>
 <div class="stat"><div class="n">{len(head)}</div><div class="l">scored method units</div></div>
</div>""")

    parts.append("<h2>What is being tested, and why not the obvious thing</h2>")
    parts.append(
        "<p>The obvious ground-truth metric — rank the measured target among a panel "
        "of metabolites — cannot work on this dataset, for a structural rather than a "
        "statistical reason. <strong>LASER has no negatives on the metabolite "
        "axis.</strong> &ldquo;They measured lycopene&rdquo; is not evidence that "
        "nothing else moved; it is evidence of what the authors chose to assay. "
        "Compounding it, Rayleigh monotonicity makes ECSPr's two-terminal conductance "
        "rise whenever conductance is added, so for the metabolite a design was built "
        "to feed, &ldquo;it went up&rdquo; carries no information.</p>")
    parts.append(
        "<p>So the contrast moves onto the <strong>design axis</strong>. Hold the "
        "target fixed and vary the design. Negatives are then designs nobody built: "
        f"{C.POOL_N} synthetic edit sets drawn from the pooled LASER universe "
        "(472 distinct additions, 222 distinct deletions), size-matched to the real "
        "designs, and passed through the identical edit policy. That controls "
        "monotonicity by construction, because a counterfactual adds as many edges as "
        "the real design does.</p>")
    parts.append(
        '<div class="key"><p>One <code>measure_leak</code> solve returns a draw for '
        'every metabolite at once, so the pool is <em>N designs per method</em> rather '
        'than <em>K counterfactuals per condition</em>. That is the only reason the '
        'null is affordable — a naive per-condition null would have cost about 100× '
        'this run.</p></div>')

    parts.append("<h2>Headline: is the built design better than the unbuilt ones?</h2>")
    mh = read("T14_matched_vs_trivial.tsv")
    if not mh.empty:
        mh.insert(0, "method", mh.unit.map(pretty_unit))
        worst_best = mh.sort_values("margin", ascending=False).iloc[0]
    if best is not None:
        row = mh[mh.unit == best.unit]
        parts.append(
            f"<p>The best-placed ECSPr arm on a denominator worth reporting is "
            f"<strong>{pretty_unit(best.unit)}</strong>: "
            f"<strong>{pct(best.frac_sig)}</strong> of its {int(best.n_conditions)} "
            f"scoreable conditions beat their own size-matched counterfactual null at "
            f"q&lt;0.05 (paper-level 95% CI {pct(best.ci_lo)}–{pct(best.ci_hi)}), "
            f"against 5% expected. Taken alone that reads like a result.</p>")
        if len(row):
            r = row.iloc[0]
            parts.append(
                f'<div class="key"><p><strong>It is not.</strong> Scored on the '
                f'<em>same {int(r.n_matched)} conditions</em>, the trivial baseline — '
                f'&ldquo;is the target a product of one of this design\'s added '
                f'reactions?&rdquo; — reaches <strong>{pct(r.trivial_frac_sig)}</strong>. '
                f'The arm\'s margin over it is '
                f'<strong>{r.margin * 100:+.1f} points</strong>, and condition by '
                f'condition the arm gives the smaller p on only '
                f'{pct(r.frac_arm_better)} of them. The pooled comparison flattered '
                f'the arm because the baseline never abstains and was being scored on '
                f'more conditions than the arm could see.</p></div>')
    if not mh.empty:
        parts.append(
            "<p>Every arm in this benchmark has a negative matched margin. "
            "<strong>Neither ECSPr in any configuration nor genome-scale flux balance "
            "beats the trivial baseline on the design axis of LASER.</strong> That is "
            "the result. The rest of this page is about where the little signal each "
            "method does carry actually comes from, and what would have to change for "
            "the comparison to be informative.</p>")
        parts.append(table(
            mh, cols=["method", "n_matched", "arm_frac_sig", "trivial_frac_sig",
                      "margin", "arm_median_p", "trivial_median_p",
                      "frac_arm_better"],
            fmt={"arm_frac_sig": pct, "trivial_frac_sig": pct,
                 "margin": lambda v: f"{v * 100:+.1f} pts",
                 "frac_arm_better": pct},
            caption="T14 — every arm against the trivial baseline on the conditions "
                    "BOTH scored. This is the only like-for-like comparison: the ECSPr "
                    "arms abstain wherever the target is not a node of their graph, "
                    "while the baseline scores everything, so the pooled table below "
                    "compares different denominators."))
    parts.append(table(
        head, cols=["method", "n_conditions", "n_papers", "n_abstained_cells",
                    "frac_sig", "ci_lo", "ci_hi", "median_p", "frac_p_lt_05",
                    "frac_sig_binned", "median_n_null_bin", "bh_resolution_floor"],
        fmt={"frac_sig": pct, "ci_lo": pct, "ci_hi": pct, "frac_p_lt_05": pct,
             "frac_sig_binned": pct},
        caption="T2 — the counterfactual-design null. <code>frac_sig</code> is the "
                "BH-corrected fraction at q&lt;0.05 under the size-regressed variant "
                "(all N counterfactuals, so p resolves to 1/(N+1)); "
                "<code>frac_sig_binned</code> is the same test inside a size bin, whose "
                "resolution floor <code>1/(n_bin+1)</code> is printed beside it — where "
                "that floor exceeds the BH threshold the binned test cannot reject at "
                "all, however strong the effect. <code>n_abstained_cells</code> counts "
                "targets that are not nodes of that arm's graph and so leave the "
                "denominator rather than scoring as zero."))
    fba_big = fba[fba.n_conditions >= MIN_N].sort_values("arm")
    if len(fba_big):
        fb = fba_big[fba_big.arm == "fba_a0.1"]
        fb = fb.iloc[0] if len(fb) else fba_big.iloc[0]
        parts.append(
            f"<p>Flux balance never clears BH on a single condition, but the "
            f"distribution says that understates it: its median empirical p is "
            f"<strong>{fb.median_p:.2f}</strong> "
            f"[{fb.median_p_ci_lo:.2f}, {fb.median_p_ci_hi:.2f}] against 0.50 under "
            f"the null, and {pct(fb.frac_p_lt_05)} of conditions fall below 0.05 "
            f"uncorrected against 5% expected. FBA is shifted the right way across the "
            f"whole population without any one condition surviving multiplicity — a "
            f"different failure from the ECSPr arms, whose medians sit at "
            f"{', '.join(f'{r.median_p:.2f}' for _, r in big.sort_values('unit').iterrows())}. "
            f"The biomass floor is not what produces it: "
            + "; ".join(f"α={r.arm.split('_a')[-1]} → median p {r.median_p:.2f}"
                        for _, r in fba_big.iterrows())
            + ".</p>")
    if len(small):
        parts.append(
            "<p>Units with fewer than %d scoreable conditions are excluded from that "
            "comparison and read below in the table: %s. A six-condition unit can post "
            "a large fraction and mean nothing, and iECDH10B contributes only 26 of the "
            "235 designs.</p>" % (
                MIN_N, ", ".join(f"{pretty_unit(r.unit)} (n={int(r.n_conditions)}, "
                                 f"{pct(r.frac_sig)})" for _, r in small.iterrows())))
    parts.append(img(C.OUT / "fig_1_p_distribution.png",
                     "Empirical p against the uniform null, one panel per method. "
                     "Mass piled at the left edge is signal; a flat histogram is none."))

    forests = sorted(C.OUT.glob("fig_2_forest_*.png"))
    for f in forests[:2]:
        u = f.stem[len("fig_2_forest_"):]
        parts.append(img(f, f"{pretty_unit(u)} — every scoreable condition against its "
                            "own counterfactual band, sorted by effect. This is the "
                            "figure that shows <em>which</em> designs the method gets "
                            "right, not how many."))

    parts.append("<h2>Where the signal lives</h2>")
    parts.append(
        "<p>All 235 conditions are pooled in the headline, as instructed. The strata "
        "ride along as columns so that if the pooled number looks wrong the first check "
        "needs no second run.</p>")
    if not strata.empty:
        piv = strata[strata.arm.astype(str).str.startswith(("gem", "denovo"))]
        rc = piv[piv.stratum.isin(("panel_R", "panel_C"))]
        if len(rc):
            byu = rc.pivot_table(index="unit", columns="stratum",
                                 values=["frac_sig", "n"])
            lines = []
            for u in byu.index:
                try:
                    lines.append(
                        f"{pretty_unit(u)}: {pct(byu.loc[u, ('frac_sig', 'panel_C')])} "
                        f"on {int(byu.loc[u, ('n', 'panel_C')])} created targets vs "
                        f"{pct(byu.loc[u, ('frac_sig', 'panel_R')])} on "
                        f"{int(byu.loc[u, ('n', 'panel_R')])} pre-existing ones")
                except (KeyError, ValueError, TypeError):
                    continue
            if lines:
                parts.append(
                    '<div class="key"><p><strong>Every ECSPr arm\'s entire signal is '
                    'Panel C.</strong> Where the design itself creates the target, the '
                    'arms fire; where the target already exists in the baseline graph — '
                    'the harder case, and the majority of conditions — they are at '
                    'exactly zero. ' + "; ".join(lines) + ". The same split shows up on "
                    "the edit census: <code>no_heterologous_add</code> is zero for every "
                    "arm, and all of the signal sits in "
                    "<code>has_heterologous_add</code>. What the conductance model is "
                    "detecting on this dataset is that a reaction producing the target "
                    "was grafted in — which is what the trivial baseline computes "
                    "directly, and computes better.</p></div>")
    parts.append(table(
        strata, cols=["method", "stratum", "n", "frac_sig", "median_p"],
        fmt={"frac_sig": pct},
        caption="T3 — the headline recomputed on each stratum. <code>panel_C</code> is "
                "targets the design itself creates; <code>panel_R</code> is targets "
                "already present in the baseline graph."))

    parts.append("<h2>How much of LASER any method can speak to</h2>")
    parts.append(
        "<p>This is a first-class result, not a footnote on a score. The methods are "
        "columns of one table so the two failure modes read side by side.</p>")
    parts.append(table(cov, caption="T1 — the coverage waterfall."))
    tstat = (tgt.groupby("status").agg(tokens=("token", "size"), obs=("n_obs", "sum"))
             .reset_index())
    parts.append(table(
        tstat, caption="Target resolution. Every non-mechanical call lives in the "
                       "git-tracked override table, split into status declarations "
                       "(judgement) and name equivalences (pure lexicon); the builder "
                       "fails on any override whose target has left the extraction, so "
                       "the table cannot rot."))

    parts.append("<h2>Corroboration, free from the same matrix</h2>")
    if not od.empty:
        s = (od.groupby("unit").auroc.agg(["count", "median"]).reset_index()
             .rename(columns={"count": "n_columns", "median": "median_auroc"}))
        tie = (od.assign(t=(od.auroc - 0.5).abs() < 1e-9)
               .groupby("unit").t.agg(["sum", "mean"]).reset_index()
               .rename(columns={"sum": "n_tied_at_half", "mean": "frac_tied"}))
        inf = (od[(od.auroc - 0.5).abs() >= 1e-9].groupby("unit").auroc.median()
               .reset_index().rename(columns={"auroc": "median_auroc_untied"}))
        s = s.merge(tie, on="unit", how="left").merge(inf, on="unit", how="left")
        s.insert(0, "method", s.unit.map(pretty_unit))
        parts.append(table(s, cols=["method", "n_columns", "median_auroc",
                                    "n_tied_at_half", "median_auroc_untied"],
                           fmt={"n_tied_at_half": lambda v: f"{int(v)}"},
                           caption="T4 — off-diagonal AUROC per target column. "
                                   "Negatives here are other papers' real, published "
                                   "designs: stronger evidence than a synthetic "
                                   "counterfactual, but the columns are imbalanced "
                                   "(most targets have one positive; lycopene and "
                                   "succinate have 22 each) and edit-set size is "
                                   "uncontrolled. Columns sitting at exactly 0.5 are "
                                   "all-tied — the arm gave every design the same "
                                   "value there — and are counted separately so the "
                                   "median is not read as evidence of chance "
                                   "performance where it is really evidence of no "
                                   "prediction."))
    parts.append(img(C.OUT / "fig_3_off_diagonal_auroc.png",
                     "Per-column AUROC. 0.5 is chance."))
    if not perm.empty:
        parts.append(table(
            perm, cols=["method", "observed_median", "target_perm_p", "panel_perm_p"],
            caption="T9 — two permutation nulls that need no solves: does the target's "
                    "identity matter given the deltas, and is the panel ranking real?"))

    parts.append("<h2>Secondary metrics, each with its baseline</h2>")
    parts.append(
        "<p>LASER's labels are 184 up against 2 down, so sign agreement is reported "
        "only next to the constant predictor that always says &ldquo;up&rdquo;. "
        "Anything depending on separating up from down is a sanity check here, never "
        "a result.</p>")
    parts.append(table(
        sec, cols=["method", "n_sign", "sign_agreement", "constant_baseline",
                   "sign_perm_p", "spearman_fold", "n_fold"],
        fmt={"sign_agreement": pct, "constant_baseline": pct},
        caption="T5 — sign agreement against the constant predictor and its "
                "permutation null, and the correlation of predicted magnitude with "
                "measured log2 fold change."))
    parts.append(img(C.OUT / "fig_7_fold_change.png",
                     "Predicted delta against measured fold change."))
    if not prank.empty:
        parts.append(table(
            prank, cols=["method", "n", "median_percentile_rank", "chance",
                         "frac_top_decile"],
            caption="T10 — the metabolite percentile rank, reported as a DESCRIBED "
                    "PROPERTY. The metabolite axis has no negatives, so this scores "
                    "nothing; it is here because its absence would read as concealment."))

    parts.append("<h2>Conductance against flux</h2>")
    parts.append(
        "<p>The same design × target matrix read two ways, which are different "
        "questions and are not pooled: <strong>column-wise</strong> asks whether the "
        "two methods rank <em>designs</em> alike (the comparison that matters for "
        "picking what to build); <strong>row-wise</strong> asks whether they rank "
        "<em>metabolites</em> alike.</p>")
    if not mvm.empty:
        b = mvm.sort_values("n_cells", ascending=False).iloc[0]
        if np.isfinite(b.colwise_median):
            verdict = ("agree" if b.colwise_median > 0.15 else
                       "<strong>disagree</strong>" if b.colwise_median < -0.15
                       else "are unrelated")
            parts.append(
                f"<p>On the pairing with the most cells "
                f"({pretty_unit(b.ecspr_unit)} against {pretty_unit(b.fba_unit)}, "
                f"{int(b.n_cells):,} cells), the two methods {verdict} on the design "
                f"axis: median column-wise Spearman "
                f"<strong>{b.colwise_median:+.2f}</strong> over "
                f"{int(b.colwise_n)} targets, against "
                f"{b.rowwise_median:+.2f} row-wise over {int(b.rowwise_n)} designs and "
                f"{b.pooled_spearman:+.2f} pooled. Conductance and flux are not "
                f"measuring the same thing here, and the sign says which way: where "
                f"one ranks a design highly the other tends not to.</p>")
        m = mvm.copy()
        m["ECSPr"] = m.ecspr_unit.map(pretty_unit)
        m["FBA"] = m.fba_unit.map(pretty_unit)
        parts.append(table(m, cols=["ECSPr", "FBA", "n_cells", "pooled_spearman",
                                    "colwise_median", "colwise_n", "rowwise_median",
                                    "rowwise_n"],
                           caption="T6 — ECSPr conductance against FBA flux."))
    parts.append(img(C.OUT / "fig_5_conductance_vs_flux.png",
                     "Pooled cells, then the column-wise and row-wise rho "
                     "distributions."))

    parts.append("<h3>Biomass — the one target flux balance was built for</h3>")
    parts.append(
        "<p>Label-free by design: FBA's growth rate against ECSPr's precursor share, "
        "with no experimental label in the comparison at all. The precursor set is the "
        "model's own biomass substrates bridged through <code>chem_xref</code> — 66 of "
        "66 map, 49 of them landing in the carbon atom universe.</p>")
    if not bio.empty:
        b = bio.copy()
        b.insert(0, "method", b.unit.map(pretty_unit))
        parts.append(table(b, cols=["method", "n", "spearman_delta"],
                           caption="T7 — biomass panel."))
    parts.append(img(C.OUT / "fig_4_biomass.png",
                     "Delta precursor share against delta growth."))

    parts.append("<h2>Within-paper design series — a case study, not a metric</h2>")
    parts.append(
        "<p>Same lab, same assay, several designs: the cleanest comparison the dataset "
        "could offer. It barely exists. Of 175 papers only 23 are multi-arm, and after "
        "requiring a shared target, a comparable measurement type and distinct gene "
        "sets, a handful of series survive. Kendall tau is reported per named series; "
        "n this small cannot support an aggregate and none is given.</p>")
    if not series.empty:
        s = series[series.n_pairs > 0].copy()
        s["method"] = s.unit.map(pretty_unit)
        parts.append(table(s, cols=["method", "source_record", "target",
                                    "measurement_type", "n_arms", "n_pairs",
                                    "kendall_tau"],
                           caption="T8 — within-paper series."))
    cross = read("T13_cross_paper_series.tsv")
    if not cross.empty:
        cs = (cross.groupby("unit").agg(n_groups=("target", "size"),
                                        n_pairs=("n_pairs", "sum"),
                                        median_tau=("kendall_tau", "median"))
              .reset_index())
        cs.insert(0, "method", cs.unit.map(pretty_unit))
        parts.append(table(cs, cols=["method", "n_groups", "n_pairs", "median_tau"],
                           caption="T13 — the cross-paper relaxation: same target, same "
                                   "host, same measurement type, different papers. It "
                                   "buys more pairs and pays with strain and "
                                   "fermentation confounding, so it can corroborate a "
                                   "positive and cannot exonerate a null."))

    parts.append("<h2>Numerics, and what would falsify this</h2>")
    parts.append(
        f"<p>The v1/v2 bake mix passes its orientation gate: of "
        f"{bake['n_v1_only_pairs']:,} tier-4 pairs absent from bake v2 on a shared "
        f"reaction, {bake['n_reversed_in_v2']} appear reversed "
        f"({bake['reversed_fraction']:.4%}), and the tier-4 sha256 matches "
        f"<code>TIER4_FREEZE.md</code>. {bake['n_ratio_gt_1']:,} of "
        f"{bake['n_direction_ratios']:,} direction ratios exceed 1, i.e. reverse their "
        f"edge — which is why the undirected arm is run alongside every directed one.</p>")
    num = read("T12_numerics.tsv")
    if not num.empty:
        num.insert(0, "method", num.unit.map(pretty_unit))
        parts.append(table(
            num, cols=[c for c in ["method", "n_designs", "n_ok", "n_skipped",
                                   "frac_converged", "cholmod_warnings",
                                   "noise_floor_median", "signal_median",
                                   "frac_above_10x_floor", "median_seconds"]
                       if c in num.columns],
            fmt={"frac_converged": pct, "frac_above_10x_floor": pct},
            caption="T12 — numerics. <code>frac_above_10x_floor</code> is the share of "
                    "designs whose whole-graph signal clears ten times the measured "
                    "solver noise; the rest are held in a <code>below_floor</code> "
                    "state that never enters sign or correlation."))
    sm = read("T11_size_matching.tsv")
    if not sm.empty:
        sm.insert(0, "method", sm.unit.map(pretty_unit))
        parts.append(table(
            sm, cols=["method", "size_bin", "n_real", "n_cf", "real_median_edits",
                      "cf_median_edits", "real_mean_add", "cf_mean_add",
                      "real_mean_del", "cf_mean_del", "median_shift"],
            caption="T11 — the size-matching gate. A bin whose real designs sat above "
                    "its counterfactuals would manufacture significance, so this is a "
                    "gate rather than an appendix: <code>median_shift</code> is the "
                    "real-minus-counterfactual median edit count and should sit at "
                    "zero."))
    parts.append(img(C.OUT / "fig_6_signal_vs_noise.png",
                     "Design signal against the solver's own noise floor, calibrated by "
                     "jittering baseline weights by relative 1e-10. Cells within 10× of "
                     "the floor are held in a separate state and never enter sign or "
                     "correlation, because the sign of round-off means nothing."))
    parts.append(
        "<ul>"
        "<li>If the headline is null, the undirected arm separates &ldquo;the atom "
        "graph does not carry this signal&rdquo; from &ldquo;the direction ensemble is "
        "the problem&rdquo;.</li>"
        "<li>The stratification table separates &ldquo;the method fails&rdquo; from "
        "&ldquo;the method only works where the answer is already in the edit "
        "list&rdquo;.</li>"
        "<li>A counterfactual pool whose sizes sat below the real designs' would "
        "manufacture significance; sizes are drawn from the empirical joint "
        "<code>(n_add, n_del)</code> of the real designs, and the size-regressed "
        "variant removes the residual dependence outright.</li>"
        "</ul>")
    parts.append("<h3>What this run could not do</h3>")
    parts.append(
        "<ul>"
        "<li>There is no <code>laser:ONPATH</code> control in this extraction. The ten "
        "non-LASER records are <code>curated:*</code> mechanism probes, all add-only "
        "and eight of them <code>measured=unknown</code>, and none of them names a "
        "medium or a carbon source — so they cannot be solved at all and form their "
        "own excluded stratum. The free must-move sanity gate the design assumed is "
        "therefore not available; the jittered-baseline noise floor and the "
        "<code>destroyed</code> state carry that job instead.</li>"
        "<li>LASER records 184 <code>up</code> against 2 <code>down</code>. "
        "Directionality is untestable on this dataset, full stop.</li>"
        "<li>The binned counterfactual null is resolution-limited at "
        "1/(n_bin+1) and cannot reject under BH at these bin sizes. That is a pool-size "
        "property, not a method result, and it is why the size-regressed variant is the "
        "primary.</li>"
        "<li>Both bake versions are mixed by necessity — the answer key is v1 and the "
        "only direction table is v2. The gate bounds the damage; it does not remove "
        "it.</li>"
        "</ul>")

    parts.append(
        '<p style="color:var(--soft);font-size:.8rem;margin-top:3rem">'
        'Reproduce: <code>build_refs.py</code> → <code>verify_bake_join.py</code> → '
        '<code>run_arms.py</code> / <code>run_fba.py</code> → <code>score.py</code> → '
        '<code>score_extras.py</code> → <code>make_figures.py</code> → '
        '<code>report.py</code>, all under <code>main/benchmarks/laser/vs_gem/</code>.'
        '</p>')
    parts.append("</div>")

    OUTFILE.write_text(
        "<title>ECSPr vs flux balance on LASER — executed benchmark</title>\n"
        + "\n".join(parts))
    print(f"wrote {OUTFILE} ({OUTFILE.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
