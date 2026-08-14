"""How many inserts closed on 2, 1 and 0 vector junctions.

SPEC
  The count the length figure's barcode encodes, as a table. One row per
  junction count, plus a total: how many inserts, what fraction of the set, and
  the median length of that group -- the last because the junction count is a
  statement about closure and the reader's first question is whether the
  unclosed ones are the short ones.

  `inserts.csv`'s `ends` column is taken verbatim, exactly as `insert_lengths.py`
  takes it; closure is not recomputed here (see `pieces.py`, NO BACKBONE BLAST).
  Nothing else is derived, so this table costs one file read and needs neither
  the pieces rebuild nor blastn.

INPUT   data/fabfos/runs/scadc_fosmids/sequences/inserts/insert_metadata/inserts.csv
ENV     mamba run -n figure-net python main/figures/inserts/junction_table.py
OUT     cache/junction_table.{png,svg}
        cache/junction_table.tsv
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt      # noqa: E402
import pandas as pd                  # noqa: E402

from _common import CACHE, FAINT, INK, INSERT_META, REPO, save

INSERT_TABLE = INSERT_META / "inserts.csv"
ENDS = [2, 1, 0]                # both ends closed, one, neither
COLS = ["N junctions", "Inserts", "% of set", "Median (kb)"]
COL_X = [0.02, 0.46, 0.70, 1.00]
COL_HA = ["left", "right", "right", "right"]


def tabulate():
    """-> a frame of one row per junction count, most closed first, plus a total."""
    ins = pd.read_csv(INSERT_TABLE)
    ins["ends"] = ins["ends"].astype(int)
    if not set(ins["ends"]) <= set(ENDS):
        raise SystemExit(f"unexpected junction counts: {sorted(set(ins['ends']))}")

    rows = []
    for e in ENDS + ["all"]:
        grp = ins if e == "all" else ins[ins["ends"] == e]
        rows.append({"junctions": str(e),
                     "inserts": len(grp),
                     "pct": 100.0 * len(grp) / len(ins),
                     "median_kb": grp["length"].median() / 1000.0})
    return pd.DataFrame(rows)


def render(tab):
    n_rows = len(tab)
    fig = plt.figure(figsize=(3.9, 0.28 * (n_rows + 1.5)), dpi=300)
    ax = fig.add_subplot(111)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, n_rows + 0.9)
    ax.axis("off")

    y_head = n_rows + 0.15
    for x, ha, lab in zip(COL_X, COL_HA, COLS):
        ax.text(x, y_head, lab, ha=ha, va="bottom", fontsize=8.5, color=INK)
    ax.plot([0, 1], [y_head - 0.14] * 2, color=INK, lw=0.8)

    for i, r in enumerate(tab.itertuples()):
        total = r.junctions == "all"
        y = n_rows - 0.75 - i
        if total:
            ax.plot([0, 1], [y + 0.62] * 2, color=FAINT, lw=0.6)
        cells = [("all" if total else r.junctions), f"{r.inserts}",
                 f"{r.pct:.0f}", f"{r.median_kb:.1f}"]
        for x, ha, txt in zip(COL_X, COL_HA, cells):
            ax.text(x, y, txt, ha=ha, va="center", fontsize=9, color=INK,
                    fontweight="bold" if total else "normal")

    fig.patch.set_facecolor("white")
    save(fig, "junction_table", dpi=300)
    plt.close(fig)


def generate():
    tab = tabulate()
    render(tab)
    tsv = CACHE / "junction_table.tsv"
    tab.to_csv(tsv, sep="\t", index=False, float_format="%.1f")
    print(f"wrote {tsv.relative_to(REPO)}", flush=True)
    for r in tab.itertuples():
        print(f"  {r.junctions:>3} junctions  {r.inserts:>4} inserts  "
              f"{r.pct:5.1f}%  median {r.median_kb:.1f} kb")


if __name__ == "__main__":
    generate()
