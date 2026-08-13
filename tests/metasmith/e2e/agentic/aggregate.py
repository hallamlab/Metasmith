"""Aggregate token-benchmark ``results.csv`` rows into a summary + life-cycle plot.

Reads one or more ``results.csv`` files (written by ``run_cell.py``), merges
them into a tidy dataframe, and emits:

  (a) a per-(arm, test) summary CSV — for each of the 4 token counts
      (``tokens_in`` / ``tokens_cached`` / ``tokens_out`` /
      ``tokens_cache_creation``) the median **among successful runs** plus the
      raw success points, and the cell's success rate (successes / executed).
      Token cost is measured only over successes; failures are kept in the
      denominator (success rate) but never in the token medians.

  (b) the CUMULATIVE life-cycle plot — cumulative total tokens across the tests
      (t1 → t7, in order) per arm, one line per arm, rendered to PNG.

    python -m tests.metasmith.e2e.agentic.aggregate \
        --results <data>/token-benchmark/results.csv \
        --out-dir <data>/token-benchmark/aggregate

Matplotlib is optional: if it (or another plotting dep) is unavailable, the
summary CSV is still written and a clear note is printed / recorded — the plot
is simply skipped rather than aborting the whole aggregation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


_TOKEN_COLS = ("tokens_in", "tokens_cached", "tokens_out", "tokens_cache_creation")
_DEFAULT_RESULTS = Path(
    "/home/tony/agentic_workspace/data/metasmith/token-benchmark/results.csv"
)

#: Canonical test ordering for the life-cycle x-axis.
_TEST_ORDER = ("install", "pipeline", "run", "adapt_new_host", "adapt_hpc",
               "adapt_add_tool", "adapt_from_middle")


def _truthy(v) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes", "y")


def load_results(paths: list[Path]) -> pd.DataFrame:
    """Concatenate result CSVs into one tidy dataframe with numeric tokens."""
    frames = []
    for p in paths:
        if p.exists() and p.stat().st_size > 0:
            frames.append(pd.read_csv(p, dtype=str, keep_default_na=False))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    for c in _TOKEN_COLS + ("iterations", "wall_s", "test_id"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["_total_tokens"] = df[list(_TOKEN_COLS)].sum(axis=1, skipna=True)
    # "Executed" = the loop ran (outcome stamped) OR the harness crashed before
    # the loop (status=error, blank outcome). Both are attempts and belong in the
    # DNF denominator.
    _outcome = df["outcome"].astype(str).str.strip() if "outcome" in df else ""
    _status = df["status"].astype(str).str.strip() if "status" in df else ""
    df["_executed"] = (
        (_outcome.ne("") if "outcome" in df else False)
        | (_status.eq("error") if "status" in df else False)
    )
    df["_success"] = (
        df["_executed"]
        & df.get("outcome", "").astype(str).str.strip().eq("done")
        & df.get("artifact_ok", "").map(_truthy)
    )
    # DNF = attempted but did not succeed. `over_budget` is the "quota reached"
    # subset the study surfaces explicitly. Derived here (not a CSV column) so
    # the master experiments.csv schema is untouched.
    df["_dnf"] = df["_executed"] & ~df["_success"]
    df["_over_budget"] = df["_executed"] & (
        _outcome.eq("over_budget") if "outcome" in df else False
    )
    return df


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """Per-(arm, test) summary: token medians over successes + success rate."""
    if df.empty:
        return pd.DataFrame()
    rows = []
    for (arm, test), g in df[df["_executed"]].groupby(["arm", "test"], dropna=False):
        succ = g[g["_success"]]
        rec = {
            "arm": arm,
            "test": test,
            "test_id": int(g["test_id"].dropna().iloc[0]) if g["test_id"].notna().any() else "",
            "n_executed": int(len(g)),
            "n_success": int(len(succ)),
            "success_rate": round(len(succ) / len(g), 4) if len(g) else 0.0,
            "n_dnf": int(g["_dnf"].sum()),
            "dnf_rate": round(int(g["_dnf"].sum()) / len(g), 4) if len(g) else 0.0,
            "n_over_budget": int(g["_over_budget"].sum()),
        }
        for c in _TOKEN_COLS:
            pts = succ[c].dropna().tolist()
            rec[f"median_{c}"] = float(succ[c].median()) if pts else ""
            rec[f"points_{c}"] = json.dumps([int(x) for x in pts])
        tot_pts = succ["_total_tokens"].dropna().tolist()
        rec["median_total_tokens"] = float(succ["_total_tokens"].median()) if tot_pts else ""
        rows.append(rec)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["test_id", "arm"], kind="stable").reset_index(drop=True)
    return out


def _test_sort_key(test: str) -> int:
    try:
        return _TEST_ORDER.index(test)
    except ValueError:
        return len(_TEST_ORDER)


def render_cumulative_plot(summary: pd.DataFrame, out_png: Path) -> tuple[bool, str]:
    """Render cumulative total tokens across tests per arm to ``out_png``.

    Returns ``(ok, note)``. ``ok=False`` with an explanatory note when a plotting
    dep is missing or there is nothing plottable — the caller keeps the summary.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # noqa: BLE001 — optional dep
        return False, (f"plot skipped: matplotlib unavailable ({exc}); "
                       f"summary CSV still written")

    if summary.empty:
        return False, "plot skipped: no rows to plot"

    plot_df = summary.copy()
    # Per-cell representative cost = median total among successes (blank → 0 so
    # the cumulative line stays continuous across tests with no success yet).
    plot_df["_cell_total"] = pd.to_numeric(
        plot_df["median_total_tokens"], errors="coerce"
    ).fillna(0.0)
    plot_df["_order"] = plot_df["test"].map(_test_sort_key)

    fig, ax = plt.subplots(figsize=(9, 5.5))
    plotted = 0
    for arm, g in plot_df.groupby("arm"):
        g = g.sort_values("_order", kind="stable")
        cum = g["_cell_total"].cumsum()
        ax.plot(g["test"], cum, marker="o", label=str(arm))
        plotted += 1
    if plotted == 0:
        plt.close(fig)
        return False, "plot skipped: no arms to plot"

    ax.set_xlabel("benchmark test (life-cycle stage)")
    ax.set_ylabel("cumulative tokens (median total among successes)")
    ax.set_title("Token life-cycle: cumulative cost across tests, per arm")
    ax.legend(title="arm", fontsize="small", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate(rotation=30)
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return True, f"plot written to {out_png}"


def aggregate(
    results_paths: list[Path], out_dir: Path,
    *, summary_path: Path | None = None, plot_path: Path | None = None,
) -> dict:
    """Load → summarize → write CSV → render plot. Returns a small report dict."""
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = summary_path or (out_dir / "summary.csv")
    plot_path = plot_path or (out_dir / "cumulative_lifecycle.png")

    df = load_results(results_paths)
    summary = summarize(df)
    summary.to_csv(summary_path, index=False)

    plot_ok, plot_note = render_cumulative_plot(summary, plot_path)
    if not plot_ok:
        (out_dir / "PLOT_NOTE.txt").write_text(plot_note + "\n")

    return {
        "n_rows": int(len(df)),
        "n_cells": int(len(summary)),
        "summary_path": str(summary_path),
        "plot_ok": plot_ok,
        "plot_note": plot_note,
        "plot_path": str(plot_path) if plot_ok else None,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="aggregate",
        description="Aggregate token-benchmark results into a summary + plot.",
    )
    ap.add_argument("--results", type=Path, nargs="*", default=None,
                    help="one or more results.csv files (default: the sibling "
                         "results.csv of experiments.csv).")
    ap.add_argument("--results-dir", type=Path, default=None,
                    help="directory to glob 'results*.csv' from (merged with "
                         "--results).")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="output dir for summary.csv + the plot PNG (default: "
                         "<data>/token-benchmark/aggregate).")
    ap.add_argument("--summary", type=Path, default=None)
    ap.add_argument("--plot", type=Path, default=None)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    paths: list[Path] = list(args.results or [])
    if args.results_dir:
        paths += sorted(args.results_dir.glob("results*.csv"))
    if not paths:
        paths = [_DEFAULT_RESULTS]
    paths = [p.resolve() for p in paths]

    out_dir = (args.out_dir or _DEFAULT_RESULTS.with_name("aggregate")).resolve()
    report = aggregate(
        paths, out_dir, summary_path=args.summary, plot_path=args.plot,
    )
    print(f"aggregated {report['n_rows']} rows -> {report['n_cells']} cells")
    print(f"  summary -> {report['summary_path']}")
    print(f"  {report['plot_note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
