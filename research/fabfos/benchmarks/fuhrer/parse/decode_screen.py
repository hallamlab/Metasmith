#!/usr/bin/env python3
"""Fuhrer 2017's screen matrices and their four key workbooks, decoded into a gene roster,
an ion key and two z-score matrices.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fuhrer/parse/decode_screen.py
    ... --publish        # writes data/fabfos/runs/fuhrer_clones/parse/screen/

EVERY JOIN IN THIS FILE IS POSITIONAL, AND THAT IS THE WHOLE RISK. `zscore_neg.tsv` and
`zscore_pos.tsv` are headerless numeric grids. Row identity lives in `neg_ionMz.xls` /
`pos_ionMz.xls`, column identity in `sample_id_zscore.xls`, and nothing in any of the five
files names the other. An off-by-one produces a complete, plausible, entirely wrong
benchmark that no downstream guard would catch. So the correspondence is PROVEN here,
four ways, and the script exits rather than writing if any of them fails:

  G1 SHAPE.  3,169 negative ions, 4,365 positive ions, 3,807 mutants -- the paper's own
     counts (Table EV1B legend, and Methods for the 3,807). Read, never tuned.
  G2 ION INDEX -> m/z, EXACTLY.  The KEGG annotation workbooks carry both an `ion` column
     and an `mz` column, and `ionMz.xls[ion - 1] == mz` holds to the BIT for all 13,509
     annotation rows. This nails matrix row <-> ion index with no tolerance at all.
  G3 AN INDEPENDENT ROW ORDER.  Table EV1B, in the article supplement rather than in the
     data deposit, lists every ion's m/z against a 1..N index. Its masses differ from the
     deposit's by up to 16 ppm -- a calibration difference, not an ordering one -- so the
     check is NEAREST-NEIGHBOUR rather than equality: EV1B's i-th mass must be the closest
     EV1B mass to the deposit's i-th mass, and conversely. A transposition anywhere breaks
     it, because neighbouring ions almost always sit far further apart than that offset.
     EXACTLY ONE PAIR IN 7,534 IS THE EXCEPTION, and the guard names it rather than being
     loosened to admit it: negative-mode ions 2418 and 2419 are 15.44 mDa apart while the
     calibration offset at that mass is 9.37 mDa, so each one's nearest EV1B mass is the
     OTHER one's. Where the local gap falls below twice the local offset, nearest-neighbour
     is arithmetically incapable of deciding, and the requirement there drops to `the
     competitor is an immediate neighbour`. Everywhere else it stays exact. The undecidable
     count is printed, so a future re-acquisition that turned one row into fifty would be
     visible rather than absorbed.
  G4 THE PAPER'S OWN ANNOTATION SUMMARY.  Table EV2 states how many ions carry an
     annotation and how many compounds match, per mode. Rebuilding those four numbers from
     the KEGG workbooks reproduces them exactly, which is a check on the annotation join
     that comes from a different supplement than the annotations do.

WHY THE m/z KEYS AND EV1B DISAGREE AT ALL: the deposit's masses run ~14.5 ppm high of
EV1B's in negative mode and ~1.9 ppm high in positive, with a few ppm of scatter. One is
recalibrated and the paper does not say which. It does not matter here -- no mass in this
arm is used for chemistry, only for identity -- but it is why G3 cannot be an equality and
why the ion key carries BOTH columns rather than picking a winner.

THE COLUMN KEY IS 3,807 BARE GENE NAMES with no b-number, and ONE OF THEM IS `wt`. The
Methods say the screen ends with "3,807 mutants", but the deposit's 3,807 columns are
3,806 deletions plus a wild-type control column. Nothing in the deposit flags it and a
cohort built on the column count alone would carry the reference strain as a mutant. It
survives here as a roster row with blank identifiers, because dropping it would break the
positional join to the matrix -- column 3,806 is `wt` and stays column 3,806. Every
consumer must exclude it by name.

Table EV1A supplies JW identifier, Blattner identifier and growth rate for 3,874 named
strains, against the screen's 3,806 deletions -- the paper removes mutants at three
separate stages (21 extremely sick, 16 injection errors, 3 with inconsistent replicates),
so the two counts legitimately differ and the roster reconciles them BY NAME rather than
by count. A screen gene EV1A cannot name would keep its row with blank identifiers; on
this acquisition `wt` is the only one.

float32 IS PROVABLY ROUND-TRIP EXACT FOR THIS DATA and the script asserts it rather than
arguing it. The matrices are text at five decimal places with |z| <= 61.07; float32's
spacing at 64 is 3.8e-6, half of which is below the 5e-6 needed to recover a 1e-5 grid.
Every cell is re-formatted from the stored float and compared to the source token.
"""
from __future__ import annotations

import argparse
import bisect
import sys
from pathlib import Path

import numpy as np
import openpyxl
import pandas as pd

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/keio"))
from baba_roster import first_sheet, ole_streams  # noqa: E402

RAW = REPO / "data/fabfos/originals/benchmarks/fuhrer"
SCREEN = RAW / "screen"
EV1 = RAW / "44320_2017_BFMSB167150_MOESM2_ESM.xlsx"
EV2 = RAW / "44320_2017_BFMSB167150_MOESM3_ESM.xlsx"
# The workbook names its sheets "Table EV5"; the Methods call the same table EV4. Both
# names appear in the paper and they are the same object.
EV4 = RAW / "44320_2017_BFMSB167150_MOESM5_ESM.xlsx"
OUT = REPO / "data/fabfos/runs/fuhrer_clones/parse/screen"

MODES = {
    "neg": {"n_ions": 3169, "mz": "neg_ionMz.xls", "kegg": "neg_kegg_all_3mD.xls",
            "matrix": "zscore_neg.tsv", "ev4_sheet": "negative mode"},
    "pos": {"n_ions": 4365, "mz": "pos_ionMz.xls", "kegg": "pos_kegg_all_3mD.xls",
            "matrix": "zscore_pos.tsv", "ev4_sheet": "positive mode"},
}
N_MUTANTS = 3807

# Table EV2, transcribed from the article supplement: {mode: (annotated_ions, compounds)}.
EV2_ANNOTATION = {"neg": (1484, 2093), "pos": (1646, 2075)}

ROSTER_COLS = ("matrix_col", "gene", "jw_id", "b_number", "keio_status",
               "growth_mean", "growth_sd", "annotation")
ION_COLS = ("mode", "ion_index", "mz_deposit", "mz_ev1b", "n_compound", "kegg_ids",
            "kegg_names", "best_auc", "best_auc_id", "best_auc_name", "z_cutoff")


def read_xls(path: Path) -> list[list]:
    streams = ole_streams(path.read_bytes())
    key = next(k for k in streams if k.lower().lstrip("\x05") in ("workbook", "book"))
    return first_sheet(streams[key])


def read_table(rows: list[list]) -> list[dict]:
    return [dict(zip(rows[0], r)) for r in rows[1:]]


def unquote(v) -> str:
    """Table EV4's text cells are stored with literal single quotes around them."""
    return str(v).strip("'") if v is not None else ""


def read_ev1b() -> dict[str, list[float]]:
    wb = openpyxl.load_workbook(EV1, read_only=True, data_only=True)
    out: dict[str, list[tuple[int, float]]] = {"neg": [], "pos": []}
    for row in wb["Table EV1B"].iter_rows(min_row=3, values_only=True):
        if row[0] in out:
            out[row[0]].append((int(row[1]), float(row[2])))
    wb.close()
    for mode, pairs in out.items():
        if [i for i, _ in pairs] != list(range(1, len(pairs) + 1)):
            raise SystemExit(f"!! G3: EV1B {mode} ion index is not 1..N in file order")
    return {m: [z for _, z in p] for m, p in out.items()}


def read_ev1a() -> dict[str, dict]:
    wb = openpyxl.load_workbook(EV1, read_only=True, data_only=True)
    ws = wb["Table EV1A"]
    out: dict[str, dict] = {}
    for row in ws.iter_rows(min_row=5, values_only=True):
        if not row[3]:
            continue
        out[str(row[3]).strip()] = {
            "jw_id": str(row[1] or "").strip(),
            "b_number": str(row[2] or "").strip(),
            "annotation": str(row[4] or "").strip(),
            "keio_status": str(row[5] or "").strip(),
            "growth_mean": str(row[6] or "").strip(),
            "growth_sd": str(row[7] or "").strip(),
        }
    wb.close()
    return out


def read_ev4() -> dict[tuple[str, int, str], dict]:
    """{(mode, ion_index, kegg_id): {auc, z_cutoff, name}}.

    The AUC is keyed on the ION-ANNOTATION PAIR, not on either alone: the ranker is the
    ion's z-score column and the label set is the compound's adjacent enzymes, so one ion
    with three candidate compounds carries three different AUCs and one compound seen at
    two ions likewise. Collapsing to either key alone silently averages unrelated tests.
    """
    wb = openpyxl.load_workbook(EV4, read_only=True, data_only=True)
    out: dict[tuple[str, int, str], dict] = {}
    for mode, spec in MODES.items():
        it = wb[spec["ev4_sheet"]].iter_rows(values_only=True)
        header = [unquote(h) for h in next(it)]
        for raw in it:
            r = dict(zip(header, [unquote(c) for c in raw]))
            if not r.get("ion"):
                continue
            out[(mode, int(float(r["ion"])), r["id"])] = {
                "auc": float(r["AUC"]), "z_cutoff": float(r["Z-cutoff"]), "name": r["name"],
            }
    wb.close()
    return out


MAX_CALIBRATION_MDA = 20.0     # a sanity bound on the two files' disagreement, not a fit


def check_row_order(mode: str, deposit: list[float], ev1b: list[float]) -> int:
    """G3. Each deposit mass must have the same-index EV1B mass as its nearest, and the
    reverse -- except where the two neighbouring masses are closer together than twice the
    calibration offset, which makes nearest-neighbour undecidable by arithmetic rather
    than by disorder. Returns how many rows landed in that undecidable class.

    Sorted lists, so a binary search plus its two neighbours is the whole search.
    """
    def nearest(sorted_masses: list[float], v: float) -> int:
        i = bisect.bisect_left(sorted_masses, v)
        return min((j for j in (i - 1, i, i + 1) if 0 <= j < len(sorted_masses)),
                   key=lambda j: abs(sorted_masses[j] - v))

    offsets = [abs(d - e) for d, e in zip(deposit, ev1b)]
    if max(offsets) * 1000 > MAX_CALIBRATION_MDA:
        worst = offsets.index(max(offsets))
        raise SystemExit(f"!! G3 {mode}: the two m/z files disagree by "
                         f"{max(offsets) * 1000:.2f} mDa at index {worst + 1} -- past "
                         f"{MAX_CALIBRATION_MDA} mDa this is not a calibration difference")

    undecidable = 0
    for name, a, b in ((f"{mode} deposit->EV1B", deposit, ev1b),
                       (f"{mode} EV1B->deposit", ev1b, deposit)):
        for i in range(len(a)):
            j = nearest(b, a[i])
            if j == i:
                continue
            gap = min(abs(b[k] - b[i]) for k in (i - 1, i + 1) if 0 <= k < len(b))
            if abs(j - i) == 1 and gap < 2 * offsets[i]:
                undecidable += 1
                continue
            raise SystemExit(
                f"!! G3 {name}: index {i + 1} ({a[i]:.6f}) is nearest to index {j + 1} "
                f"({b[j]:.6f}), local gap {gap * 1000:.2f} mDa against an offset of "
                f"{offsets[i] * 1000:.2f} mDa -- the two files are not in the same ion "
                f"order and no positional join is safe")
    return undecidable


def read_matrix(path: Path, n_row: int, n_col: int) -> np.ndarray:
    """float32 with the round-trip asserted against the source tokens, not assumed."""
    tokens: list[list[str]] = []
    with path.open() as fh:
        for line in fh:
            tokens.append(line.rstrip("\n").split("\t"))
    if len(tokens) != n_row or {len(t) for t in tokens} != {n_col}:
        shapes = sorted({len(t) for t in tokens})
        raise SystemExit(f"!! G1 {path.name}: {len(tokens)} rows x {shapes} cols, "
                         f"expected {n_row} x {n_col}")
    m = np.array(tokens, dtype=np.float32)
    lost = 0
    for r, row in enumerate(tokens):
        for c, tok in enumerate(row):
            decimals = len(tok.partition(".")[2])
            if f"{m[r, c]:.{decimals}f}" != tok:
                lost += 1
    if lost:
        raise SystemExit(f"!! {path.name}: {lost} cells do not survive float32 -- "
                         f"store float64 instead of losing the paper's own precision")
    return m


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}")
    a = ap.parse_args()

    genes = [r[0] for r in read_xls(SCREEN / "sample_id_zscore.xls")]
    if len(genes) != N_MUTANTS or len(set(genes)) != N_MUTANTS:
        raise SystemExit(f"!! G1 sample_id_zscore.xls: {len(genes)} names "
                         f"({len(set(genes))} distinct), expected {N_MUTANTS} of each")
    ev1a, ev1b, ev4 = read_ev1a(), read_ev1b(), read_ev4()
    print(f"G1 column key: {len(genes)} distinct mutants  |  "
          f"Table EV1A names {len(ev1a)} strains")

    unnamed = [g for g in genes if g not in ev1a]
    print(f"   {len(genes) - len(unnamed)} screen genes carry an EV1A row"
          + (f", {len(unnamed)} do not: {unnamed[:12]}" if unnamed else "")
          + ("  <- `wt` is the wild-type control column, not a deletion"
             if "wt" in unnamed else ""))

    roster = [{"matrix_col": i, "gene": g, **{k: ev1a.get(g, {}).get(k, "")
                                              for k in ROSTER_COLS[2:]}}
              for i, g in enumerate(genes)]

    ion_rows: list[dict] = []
    matrices: dict[str, np.ndarray] = {}
    for mode, spec in MODES.items():
        deposit = [r[0] for r in read_xls(SCREEN / spec["mz"])]
        if len(deposit) != spec["n_ions"]:
            raise SystemExit(f"!! G1 {spec['mz']}: {len(deposit)} ions, "
                             f"the paper says {spec['n_ions']}")
        undecidable = check_row_order(mode, deposit, ev1b[mode])

        annot = read_table(read_xls(SCREEN / spec["kegg"]))
        off = [r for r in annot if deposit[int(r["ion"]) - 1] != r["mz"]]
        if off:
            raise SystemExit(f"!! G2 {spec['kegg']}: {len(off)} of {len(annot)} rows "
                             f"whose `ion` index does not land on their own `mz`")
        by_ion: dict[int, list[dict]] = {}
        for r in annot:
            by_ion.setdefault(int(r["ion"]), []).append(r)
        n_compound = len({r["id"] for r in annot})
        want = EV2_ANNOTATION[mode]
        if (len(by_ion), n_compound) != want:
            raise SystemExit(f"!! G4 {mode}: {len(by_ion)} annotated ions / {n_compound} "
                             f"compounds, Table EV2 says {want[0]} / {want[1]}")
        print(f"G2/G3/G4 {mode}: {len(deposit)} ions, {len(annot)} annotation rows on "
              f"{len(by_ion)} ions ({n_compound} compounds) -- row order proven "
              f"({undecidable} rows undecidable by mass alone), Table EV2 reproduced")

        matrices[mode] = read_matrix(SCREEN / spec["matrix"], spec["n_ions"], N_MUTANTS)

        for idx in range(1, spec["n_ions"] + 1):
            hits = by_ion.get(idx, [])
            ids = sorted({str(r["id"]) for r in hits})
            scored = [(ev4[(mode, idx, i)]["auc"], i) for i in ids
                      if (mode, idx, i) in ev4]
            best = max(scored) if scored else None
            ion_rows.append({
                "mode": mode, "ion_index": idx,
                "mz_deposit": f"{deposit[idx - 1]:.9f}",
                "mz_ev1b": f"{ev1b[mode][idx - 1]:.9f}",
                "n_compound": len(ids), "kegg_ids": ";".join(ids),
                "kegg_names": ";".join(sorted({str(r["name"]) for r in hits})),
                "best_auc": f"{best[0]:.6f}" if best else "",
                "best_auc_id": best[1] if best else "",
                "best_auc_name": ev4[(mode, idx, best[1])]["name"] if best else "",
                "z_cutoff": f"{ev4[(mode, idx, best[1])]['z_cutoff']:.6f}" if best else "",
            })

    scored = sum(1 for r in ion_rows if r["best_auc"])
    print(f"\n{len(ion_rows)} ions, {sum(1 for r in ion_rows if r['n_compound'])} "
          f"annotated, {scored} carry a published AUC, "
          f"{sum(1 for r in ion_rows if r['n_compound'] == 1)} annotated unambiguously")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(REPO)})")
        return 0

    OUT.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(roster, columns=list(ROSTER_COLS)).to_csv(
        OUT / "gene_roster.tsv", sep="\t", index=False)
    pd.DataFrame(ion_rows, columns=list(ION_COLS)).to_csv(
        OUT / "ion_key.tsv", sep="\t", index=False)
    for mode, m in matrices.items():
        path = OUT / f"zscore_{mode}.parquet"
        path.unlink(missing_ok=True)      # DVC checks out read-only hardlinks
        pd.DataFrame(m, columns=genes).to_parquet(path, index=False, compression="zstd")
        print(f"-> {path.relative_to(REPO)}: {m.shape[0]} ions x {m.shape[1]} mutants")
    print(f"-> {OUT.relative_to(REPO)}/gene_roster.tsv: {len(roster)} rows")
    print(f"-> {OUT.relative_to(REPO)}/ion_key.tsv: {len(ion_rows)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
