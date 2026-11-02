#!/usr/bin/env python3
"""Compare one of R4's own reconstructed models against metaGEM's published
model for the SAME bin, on the four axes the brief asks for: reaction count,
metabolite count, MEMOTE total score, and whether either model grows.

    mamba run -n build-refs-cobra python research/metagem/metagem_compare.py \\
        --study li2019 --bin SRR7664615_bin.3.s \\
        --our-model /path/to/carveme_model_cplex.xml \\
        [--our-memote-score /path/to/memote_score.json] \\
        [--published-root /scratch/phyberos/metagem/published] \\
        [--out-json comparison.json]

Every comparison states which published file it read (record id + relpath from
`cluster/published_manifest.tsv`, and the exact archive member extracted) --
never just "metaGEM's published model", because both the GEM archive and the
MEMOTE export are named differently per study (`GEMs.tar.gz` vs
`tara_gems.tar.gz`, `memote_korem.csv` vs `metagem_tara.csv.gz`) and a
generic glob would silently read nothing on the studies that don't match.

WHAT "GROWS" MEANS: `model.slim_optimize()` under the model's own bounds and
objective (no medium is re-applied here -- CarveMe writes the medium/biomass
objective it carved the draft under directly into the SBML, and forcing a
DIFFERENT medium onto metaGEM's published model, which we did not carve,
would not be a fair comparison). A model with no objective reaction at all
(observed on some early-lane drafts, see modelling.yml's `memote_report`
comment) slim-optimizes to 0.0, which this script correctly calls "does not
grow" rather than raising.

WHAT THE MEMOTE COMPARISON IS AND IS NOT: metaGEM's published per-study CSV
(`memote_<study>.csv`) is MEMOTE's row-level test export, one row per test per
model -- there is no top-level `total_score` column in it, because that
number only exists inside MEMOTE's own `report snapshot` HTML/JSON, which
metaGEM did not publish. `total_score` is computed as `sum(numeric*weight)`
across every row for that model that carries a real `score`+`weight` pair
(most "informational" tests carry neither and are correctly excluded) divided
by `sum(weight)`. This is a flat weighted mean over test rows, not MEMOTE's
own two-level (test -> section -> total) aggregation, so treat it as an
approximation of the number MEMOTE's CLI would report, not an exact
reproduction -- labelled `published_memote_score_approx` in the output for
exactly that reason. Our own score, by contrast, comes straight from
`memote_score.py`'s `total_score` (Report.compute_score()'s real number,
see that transform's docstring for why one MEMOTE run yields it directly).
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import shutil
import sys
import tarfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_MANIFEST = HERE / "cluster" / "published_manifest.tsv"
DEFAULT_PUBLISHED_ROOT = Path("/scratch/phyberos/metagem/published")

# Per-study GEM archive and MEMOTE export filenames, straight out of
# cluster/published_manifest.tsv's `relpath` column. Not a regex/glob because
# naming is NOT uniform across studies (metaGEM_paper's five Zenodo records
# were built independently) -- see this module's docstring.
STUDY_FILES = {
    "li2019":        {"gems": "GEMs.tar.gz",        "memote": "memote_china_soil.csv"},
    "korem2015":     {"gems": "GEMs.tar.gz",        "memote": "memote_korem.csv"},
    "karlsson2013":  {"gems": "GEMs.tar.gz",        "memote": "memote_gut_GEMs.csv.gz"},
    "bissett_base":  {"gems": "GEMs.tar.gz",        "memote": "memote_straya.csv"},
    "sunagawa2015":  {"gems": "tara_gems.tar.gz",   "memote": "metagem_tara.csv.gz"},
}

GROWTH_EPSILON = 1e-6


def load_manifest(manifest_path: Path) -> list[dict]:
    with manifest_path.open() as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def find_manifest_row(rows: list[dict], study: str, relpath: str) -> dict:
    for r in rows:
        if r["study"] == study and r["relpath"] == relpath:
            return r
    raise SystemExit(
        f"[{relpath}] not found for study [{study}] in the manifest -- "
        f"STUDY_FILES names a file cluster/published_manifest.tsv does not carry"
    )


def extract_published_gem(published_root: Path, study: str, bin_name: str,
                           gems_relpath: str, tmp_dir: Path) -> Path:
    archive = published_root / study / gems_relpath
    if not archive.exists():
        raise SystemExit(
            f"[{archive}] does not exist -- has this study's GEM archive been "
            f"fetched yet? see cluster/fetch_zenodo_published.sh"
        )
    with tarfile.open(archive, "r:gz") as tf:
        candidates = [m for m in tf.getnames()
                      if Path(m).name.startswith(bin_name) and
                      (m.endswith(".xml.gz") or m.endswith(".xml"))]
        if not candidates:
            raise SystemExit(
                f"no member starting with [{bin_name}] (.xml/.xml.gz) inside "
                f"[{archive}]. metaGEM may not have published a model for this bin."
            )
        if len(candidates) > 1:
            raise SystemExit(f"[{bin_name}] is ambiguous inside [{archive}]: {candidates}")
        member = candidates[0]
        tf.extract(member, path=tmp_dir, filter="data")
    extracted = tmp_dir / member
    if extracted.suffix == ".gz":
        out = extracted.with_suffix("")
        with gzip.open(extracted, "rb") as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst)
        extracted = out
    return extracted, member


def _open_maybe_gzip_text(path: Path):
    if path.suffix == ".gz":
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")
    return path.open()


def published_memote_score(published_root: Path, study: str, bin_name: str,
                            memote_relpath: str) -> tuple[float | None, int]:
    """Returns (weighted_score_or_None, n_scored_rows). See module docstring
    for exactly what this number is and is not."""
    csv_path = published_root / study / memote_relpath
    if not csv_path.exists():
        return None, 0
    total_wt = 0.0
    total_wtscore = 0.0
    n = 0
    with _open_maybe_gzip_text(csv_path) as fh:
        for row in csv.DictReader(fh):
            if row.get("model") != bin_name:
                continue
            s, w = row.get("score", "").strip(), row.get("weight", "").strip()
            if not s or not w:
                continue
            try:
                s_f, w_f = float(s), float(w)
            except ValueError:
                continue
            total_wt += w_f
            total_wtscore += s_f * w_f
            n += 1
    if total_wt <= 0:
        return None, n
    return total_wtscore / total_wt, n


def model_metrics(model_path: Path) -> dict:
    import cobra
    model = cobra.io.read_sbml_model(str(model_path))
    try:
        obj = model.slim_optimize(error_value=None)
    except Exception as e:  # infeasible / no objective / solver hiccup
        obj = None
        print(f"  note: slim_optimize on [{model_path.name}] raised {type(e).__name__}: {e}",
              file=sys.stderr)
    grows = bool(obj is not None and obj > GROWTH_EPSILON)
    return {
        "n_reactions": len(model.reactions),
        "n_metabolites": len(model.metabolites),
        "objective_value": obj,
        "grows": grows,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--study", required=True, choices=sorted(STUDY_FILES))
    ap.add_argument("--bin", required=True, dest="bin_name",
                     help="bin id exactly as metaGEM's own MAGs/GEMs archives name it, "
                          "e.g. SRR7664615_bin.3.s")
    ap.add_argument("--our-model", required=True, type=Path)
    ap.add_argument("--our-memote-score", type=Path, default=None,
                     help="score.json from this repo's memote_score.py (has total_score)")
    ap.add_argument("--published-root", type=Path, default=DEFAULT_PUBLISHED_ROOT)
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    ap.add_argument("--tmp-dir", type=Path, default=None)
    ap.add_argument("--out-json", type=Path, default=None)
    args = ap.parse_args(argv)

    manifest_rows = load_manifest(args.manifest)
    files = STUDY_FILES[args.study]
    gems_row = find_manifest_row(manifest_rows, args.study, files["gems"])
    memote_row = find_manifest_row(manifest_rows, args.study, files["memote"])

    tmp_dir = args.tmp_dir or Path(f"/tmp/metagem_compare_{args.study}_{args.bin_name}")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    print(f"published GEM archive : {args.study}/{files['gems']} "
          f"(zenodo record {gems_row['record']}, md5 {gems_row['md5']})")
    published_path, member = extract_published_gem(
        args.published_root, args.study, args.bin_name, files["gems"], tmp_dir)
    print(f"  extracted member     : {member}")
    print(f"published MEMOTE export: {args.study}/{files['memote']} "
          f"(zenodo record {memote_row['record']}, md5 {memote_row['md5']})")

    ours = model_metrics(args.our_model)
    published = model_metrics(published_path)

    our_score = None
    if args.our_memote_score is not None:
        our_score = json.loads(args.our_memote_score.read_text())["total_score"]
    pub_score, pub_n_rows = published_memote_score(
        args.published_root, args.study, args.bin_name, files["memote"])

    result = {
        "study": args.study,
        "bin": args.bin_name,
        "our_model_path": str(args.our_model),
        "published_gem_source": {
            "study": args.study, "relpath": files["gems"],
            "record": gems_row["record"], "md5": gems_row["md5"], "member": member,
        },
        "published_memote_source": {
            "study": args.study, "relpath": files["memote"],
            "record": memote_row["record"], "md5": memote_row["md5"],
            "n_scored_rows": pub_n_rows,
        },
        "reaction_count": {"ours": ours["n_reactions"], "published": published["n_reactions"]},
        "metabolite_count": {"ours": ours["n_metabolites"], "published": published["n_metabolites"]},
        "grows": {"ours": ours["grows"], "published": published["grows"]},
        "objective_value": {"ours": ours["objective_value"], "published": published["objective_value"]},
        "memote_total_score": {"ours": our_score, "published_approx": pub_score},
    }

    print(f"\n{'metric':<22} {'ours':>18} {'published':>18}")
    print(f"{'reactions':<22} {ours['n_reactions']:>18} {published['n_reactions']:>18}")
    print(f"{'metabolites':<22} {ours['n_metabolites']:>18} {published['n_metabolites']:>18}")
    print(f"{'grows':<22} {str(ours['grows']):>18} {str(published['grows']):>18}")
    print(f"{'objective value':<22} {str(ours['objective_value']):>18} {str(published['objective_value']):>18}")
    print(f"{'MEMOTE total score':<22} {str(our_score):>18} {str(pub_score):>18} "
          f"(published is a {pub_n_rows}-row weighted approximation, see module docstring)")

    if args.out_json:
        args.out_json.write_text(json.dumps(result, indent=2))
        print(f"\nwrote {args.out_json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
