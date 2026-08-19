import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt      # noqa: E402
import pandas as pd                  # noqa: E402

from _common import CACHE, FAINT, INK, INSERT_META, REPO, save

INSERT_TABLE = INSERT_META / "inserts.csv"
ENDS = [2, 1, 0]
COLS = ["N junctions", "Inserts", "% of set", "Median (kb)"]
COL_X = [0.02, 0.46, 0.70, 1.00]
COL_HA = ["left", "right", "right", "right"]


def tabulate():
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
