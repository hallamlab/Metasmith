"""The crossed table: every corpus x lane cell E5 filled, beside the E1-E4 and published values for the cells they own.

    crossed_table.py > crossed_table.tsv

One row per (corpus, study, lane, source, metric). E5's rows come from mags_closed.tsv, votu_quality.tsv.gz and
pratama_score.json. E1 and E2 come from their CheckM2 and AMBER tables in results/e1 and results/e2. E3 comes from
findings/E5_RUN.md's Pratama table, which copies feat/engine/bench-E3's E3_ABLATION.md. E4 and metaGEM's bin
counts come from results/e4 at E4_COMMIT, read through git.
"""
import io
import json
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
RES = HERE.parent
E4_COMMIT = "2b6bf0e7"

CORPUS = {
    "toy_mousegut": "cami", "toy_hmp_airskinurogenital": "cami", "toy_hmp_gastrooral": "cami",
    "toy_humangut": "cami", "plant_associated": "cami", "marine": "cami", "strain": "cami",
    "pratama_short": "pratama", "pratama_hybrid": "pratama", "pratama": "pratama",
    "korem2015": "metagem", "li2019": "metagem", "bissett_base": "metagem", "karlsson2013": "metagem",
    "sunagawa2015": "metagem",
}
E5_READS = {"toy_humangut": "hybrid", "plant_associated": "hybrid", "marine": "pacbio", "strain": "pacbio",
            "pratama_hybrid": "hybrid", "pratama": "short+hybrid"}
PRIOR_READS = {"toy_humangut": "long", "plant_associated": "long"}
CATALOGUE_RUN = {"12QF5z5K": "toy_humangut", "dgWZ0owd": "plant_associated", "2ZnIRSUv": "pratama"}
PRIOR = {"mag": {"cami"}, "viral": {"pratama"}, "gem": {"metagem"}}
CHECKV = {"Complete": "complete", "High-quality": "high", "Medium-quality": "medium", "Low-quality": "low",
          "Not-determined": "not_determined"}

rows = []


def emit(study, lane, source, metrics, reads=""):
    for metric, value in metrics.items():
        text = f"{value:.6g}" if isinstance(value, float) else str(value)
        rows.append((CORPUS[study], study, lane, source, reads, metric, text))


def quartiles(name, s):
    s = s.dropna()
    return {f"{name}_q1": s.quantile(0.25), f"{name}_median": s.median(), f"{name}_q3": s.quantile(0.75)}


def tiers(df):
    hq = (df.Completeness > 90) & (df.Contamination < 5)
    mq = (df.Completeness >= 50) & (df.Contamination < 10) & ~hq
    return {"hq": int(hq.sum()), "mq": int(mq.sum()), "lq": int((~hq & ~mq).sum())}


def cami_study(name):
    return re.sub(r"(_long_nano|_long)?_sample_\d+$", "", name).removesuffix("_long_nano").removesuffix("_long")


def e4_table(name):
    path = f"research/metasmith_benchmark/results/e4/{name}"
    text = subprocess.run(["git", "show", f"{E4_COMMIT}:{path}"], capture_output=True, text=True, check=True,
                          cwd=HERE).stdout
    return pd.read_csv(io.StringIO(text), sep="\t")


asm = pd.read_csv(HERE / "assemblies.tsv", sep="\t")
runs = asm.groupby("study")["sample"].nunique().to_dict() | {"toy_humangut": 20, "plant_associated": 21}
runs["pratama"] = runs["pratama_short"] + runs["pratama_hybrid"]

mags = pd.read_csv(HERE / "mags_closed.tsv", sep="\t")
for study, df in mags.groupby("study"):
    emit(study, "mag", "E5", {
        "runs": runs[study], "runs_with_mags": df["sample"].nunique(), "mags": len(df), **tiers(df),
        **quartiles("completeness", df.Completeness), **quartiles("contamination", df.Contamination),
        "genome_size_mb_median": df.Genome_Size.median() / 1e6, "contig_n50_median": df.Contig_N50.median(),
    }, E5_READS.get(study, "short"))
    emit(study, "gem", "E5", {
        "models": int(df.reactions.notna().sum()), **quartiles("reactions", df.reactions),
        **quartiles("metabolites", df.metabolites), **quartiles("genes", df.genes),
        **quartiles("memote_total", df.memote_total), **quartiles("memote_consistency", df.memote_consistency),
        "memote_annotation_rxn_median": df.memote_annotation_rxn.median(),
        "memote_annotation_gene_median": df.memote_annotation_gene.median(),
    }, E5_READS.get(study, "short"))

study_of = dict(zip(asm["sample"], asm.study))
votu = pd.read_csv(HERE / "votu_quality.tsv.gz", sep="\t")
votu["study"] = votu["sample"].map(study_of).fillna(votu.run.map(CATALOGUE_RUN))
votu.loc[votu.study.str.startswith("pratama_"), "study"] = "pratama"
assert votu.study.notna().all()
for study, df in votu.groupby("study"):
    counts = df.checkv_quality.value_counts()
    emit(study, "viral", "E5", {
        "runs": runs[study], "votus": len(df), "votus_per_run": len(df) / runs[study],
        **{f"checkv_{name}": int(counts.get(q, 0)) for q, name in CHECKV.items()},
        "proviruses": int(((df.provirus == "Yes") | (df.trimmed == 1)).sum()),
        "singletons": int((df.cluster_size == 1).sum()),
        **quartiles("length_kb", df.length / 1e3), **quartiles("completeness", df.completeness),
    }, E5_READS.get(study, "short"))

score = json.loads((HERE / "pratama_score.json").read_text())
emit("pratama", "viral", "E5", {
    "matched": score["matched"], "recovered": score["recovered"], "recovery_pct": 100 * score["recovery"],
    "paired_median_identity": score["paired_median_identity"],
    **{f"recovery_{k}_pct": 100 * v for k, v in score["recovery_by_published_lane"].items()},
}, "short+hybrid")
for source, m in {
    "E3_R3": (172295, 123599, 168336, 65.4, 98.88, 60.7, 78.3, 36.0),
    "E3_R0": (243186, 196547, 202053, 78.5, 99.05, 72.2, 81.8, 89.8),
}.items():
    emit("pratama", "viral", source, dict(zip(
        ["votus", "matched", "recovered", "recovery_pct", "paired_median_identity",
         "recovery_spades_pct", "recovery_megahit_pct", "recovery_hybrid_pct"], m)), "short+hybrid")
emit("pratama", "viral", "Pratama2026", {"votus": 257252}, "short+hybrid")

for exp in ("e1", "e2"):
    cm = pd.read_csv(RES / exp / f"{exp}_checkm2.tsv.gz", sep="\t")
    cm = cm[cm.binner == "DASTool"]
    amber = pd.read_csv(RES / exp / f"{exp}_amber_summary.tsv", sep="\t")
    amber = amber[amber.Tool == "DASTool"]
    for dataset, df in cm.groupby("dataset"):
        study = cami_study(dataset)
        f1 = amber[amber["sample"].map(cami_study) == study].f1_score_bp
        emit(study, "mag", exp.upper(), {
            "runs_with_mags": df["sample"].nunique(), "mags": len(df), **tiers(df),
            **quartiles("completeness", df.Completeness), **quartiles("contamination", df.Contamination),
            "genome_size_mb_median": df.Genome_Size.median() / 1e6, "contig_n50_median": df.Contig_N50.median(),
            "amber_f1_bp_median": f1.median(), "amber_samples": len(f1),
        }, PRIOR_READS.get(study, "short"))

amber5 = pd.concat([pd.read_csv(p, sep="\t") for p in sorted(HERE.glob("amber_*_summary.tsv"))])
amber5 = amber5[amber5.Tool == "DASTool"]
for study, df in amber5.groupby("study"):
    emit(study, "mag", "E5", {"amber_f1_bp_median": df.f1_score_bp.median(), "amber_samples": len(df)},
         E5_READS.get(study, "short"))

tally = e4_table("tally.tsv")
for study, df in tally.groupby("study"):
    emit(study, "mag", "metaGEM", {"mags": len(df)})
parity = e4_table("parity_modern.tsv")
parity = parity[parity.reactions_ours != "no published GEM"]
parity = parity.assign(**{c: pd.to_numeric(parity[c]) for c in parity.columns if c.endswith(("_ours", "_theirs"))})
memote = e4_table("quality_modern.tsv")
for study, df in parity.groupby("study"):
    mq = memote[memote.study == study]
    emit(study, "gem", "E4", {
        "models": int(tally[tally.study == study].modern_model.sum()),
        **quartiles("reactions", df.reactions_ours), **quartiles("metabolites", df.metabolites_ours),
        **quartiles("genes", df.genes_ours), **quartiles("memote_total", mq.total_score),
        **quartiles("memote_consistency", mq.consistency),
        "memote_annotation_rxn_median": mq.annotation_rxn.median(),
        "memote_annotation_gene_median": mq.annotation_gene.median(),
    })
    emit(study, "gem", "metaGEM", {
        "models": int(tally[tally.study == study].metagem_gem.sum()), **quartiles("reactions", df.reactions_theirs),
        **quartiles("metabolites", df.metabolites_theirs), **quartiles("genes", df.genes_theirs),
    })

out = pd.DataFrame(rows, columns=["corpus", "study", "lane", "source", "reads", "metric", "value"])
out["cell"] = ["e1_e4" if corpus in PRIOR[lane] else "new" for corpus, lane in zip(out.corpus, out.lane)]
out[["corpus", "study", "lane", "cell", "source", "reads", "metric", "value"]].to_csv(sys.stdout, sep="\t", index=False)
