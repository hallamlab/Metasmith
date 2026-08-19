"""Run the PlanBench Blocksworld instance set through pyperplan and the
metasmith rust engine, writing `results.csv` + `results.md`.

The Python solver path is deliberately not exercised here -- it is a fallback
whose backward distance-walk is budget-bounded on cyclic transform universes,
not the search this benchmark is about.

The instances live in a DVC chunk rather than in git, so `--help` and every
argument check answer before anything imports pyperplan or touches the data.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
REPO_ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))

#: The DVC chunk holding the instances; see MANIFEST.md for what is in it.
DEFAULT_INSTANCE_DIR = REPO_ROOT / "data" / "metasmith" / "solver_benchmarks" / "blocksworld"
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "scratch" / "solver_benchmarking"

#: (subset label, subdirectory, block count) -- both bounded subsets in MANIFEST.md.
SUBSETS = [
    ("blocksworld_3", "generated_basic_3", 3),
    ("blocksworld_4", "generated_basic", 4),
]

FIELDS = [
    "subset", "instance", "n_blocks", "n_states", "n_edges",
    "pyperplan_solved", "pyperplan_plan_len", "pyperplan_states_expanded", "pyperplan_seconds",
    "ms_rust_solved", "ms_rust_checked_ok", "ms_rust_plan_len", "ms_rust_seconds", "ms_rust_timed_out",
    "error",
]


def resolve_instance_dir(path: Path) -> Path:
    """Fail with the command that materialises the chunk, not an empty glob.

    A DVC pin that has not been checked out looks exactly like a correct path
    holding nothing, and the benchmark would otherwise report a clean run over
    zero instances.
    """
    domain = path / "generated_domain.pddl"
    if domain.is_file():
        return path
    raise SystemExit(
        f"no Blocksworld domain at [{domain}].\n"
        f"If this is the pinned chunk, materialise it first:\n"
        f"    dvc checkout {DEFAULT_INSTANCE_DIR.relative_to(REPO_ROOT)}.dvc"
    )


def run_all(instance_dir: Path, *, limit: int | None = None) -> list[dict]:
    from pddl_ground import ground_instance
    from graph_expand import expand_state_graph, build_solver_problem, StateGraphTooLarge
    from run_pyperplan import solve_with_pyperplan
    from run_metasmith import solve_with_metasmith_rust

    domain = instance_dir / "generated_domain.pddl"
    rows = []
    for subset_label, subdir, n_blocks in SUBSETS:
        instance_files = sorted(
            (instance_dir / subdir).glob("instance-*.pddl"),
            key=lambda p: int(p.stem.split("-")[1]),
        )
        if limit is not None:
            instance_files = instance_files[:limit]
        for pf in instance_files:
            row = {"subset": subset_label, "instance": pf.stem, "n_blocks": n_blocks, "error": ""}
            try:
                gi = ground_instance(domain, pf)
                graph = expand_state_graph(gi)
                row["n_states"] = len(graph.states)
                row["n_edges"] = len(graph.edges)

                pp = solve_with_pyperplan(gi)
                row["pyperplan_solved"] = pp.solved
                row["pyperplan_plan_len"] = pp.plan_length
                row["pyperplan_states_expanded"] = pp.states_expanded
                row["pyperplan_seconds"] = pp.wall_seconds

                problem = build_solver_problem(graph)

                mr = solve_with_metasmith_rust(problem)
                row["ms_rust_solved"] = mr.solved
                row["ms_rust_checked_ok"] = mr.checked_ok
                row["ms_rust_plan_len"] = mr.plan_length
                row["ms_rust_seconds"] = mr.wall_seconds
                row["ms_rust_timed_out"] = mr.timed_out
            except StateGraphTooLarge as e:
                row["error"] = str(e)
            except Exception as e:
                row["error"] = f"{type(e).__name__}: {e}"
            rows.append(row)
            print(
                f"{subset_label}/{row['instance']}: "
                f"states={row.get('n_states', '?')} "
                f"pyperplan={'ok' if row.get('pyperplan_solved') else 'FAIL'} "
                f"ms-rust={'ok' if row.get('ms_rust_checked_ok') else 'FAIL'} "
                f"{row['error']}",
                flush=True,
            )
    return rows


def write_csv(rows: list[dict], path: Path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in FIELDS})


def summarize(rows: list[dict], subset_label: str) -> dict:
    subset_rows = [r for r in rows if r["subset"] == subset_label and not r["error"]]
    n = len(subset_rows)
    def rate(key):
        return sum(1 for r in subset_rows if r.get(key)) / n if n else 0.0
    def mean(key):
        vals = [r[key] for r in subset_rows if r.get(key) is not None]
        return sum(vals) / len(vals) if vals else 0.0
    return {
        "n": n,
        "pyperplan_coverage": rate("pyperplan_solved"),
        "ms_rust_coverage": rate("ms_rust_checked_ok"),
        "pyperplan_mean_s": mean("pyperplan_seconds"),
        "ms_rust_mean_s": mean("ms_rust_seconds"),
        "pyperplan_mean_plan_len": mean("pyperplan_plan_len"),
        "ms_rust_mean_plan_len": mean("ms_rust_plan_len"),
        "ms_rust_timeouts": sum(1 for r in subset_rows if r.get("ms_rust_timed_out")),
    }


def write_report(rows: list[dict], path: Path):
    lines = []
    lines.append("# metasmith solver (rust engine) vs. pyperplan on PlanBench Blocksworld\n")
    lines.append(
        f"Measured {datetime.date.today().isoformat()}. The coverage claim is "
        "`check_plan`-validated and holds independently of the machine; the "
        "wall-time columns do not.\n"
    )
    lines.append(
        "Instances: PlanBench's own generated Blocksworld set "
        "(`karthikv792/LLMs-Planning`, `plan-bench/instances/blocksworld/`), "
        "grounded with pyperplan's parser, fully state-graph-expanded, and "
        "packaged as pure-state metasmith `SolverProblem`s (see `MANIFEST.md`, "
        "`graph_expand.py`). Only the Rust solver engine is exercised here -- "
        "the Python fallback is not part of this comparison.\n"
    )
    for subset_label, _dir, n_blocks in SUBSETS:
        s = summarize(rows, subset_label)
        n_errors = sum(1 for r in rows if r["subset"] == subset_label and r["error"])
        lines.append(f"## {subset_label} ({n_blocks} blocks, n={s['n']}, {n_errors} errored)\n")
        lines.append("| solver | coverage | mean plan length | mean wall time (s) |")
        lines.append("|---|---|---|---|")
        lines.append(
            f"| pyperplan | {s['pyperplan_coverage']:.0%} | "
            f"{s['pyperplan_mean_plan_len']:.1f} | {s['pyperplan_mean_s']:.4f} |"
        )
        lines.append(
            f"| metasmith (rust) | {s['ms_rust_coverage']:.0%} | "
            f"{s['ms_rust_mean_plan_len']:.1f} | {s['ms_rust_mean_s']:.4f} |"
        )
        lines.append("")
        if s["ms_rust_timeouts"]:
            lines.append(f"({s['ms_rust_timeouts']} rust timeout(s).)\n")

    lines.append("## Verdict\n")
    s3 = summarize(rows, "blocksworld_3")
    s4 = summarize(rows, "blocksworld_4")
    lines.append(
        f"Published bar (PlanBench, Fast Downward on its full Blocksworld set): "
        f"~100% coverage. This run: pyperplan {s3['pyperplan_coverage']:.0%}/"
        f"{s4['pyperplan_coverage']:.0%} (3-block/4-block), metasmith-rust "
        f"{s3['ms_rust_coverage']:.0%}/{s4['ms_rust_coverage']:.0%} "
        "(all metasmith numbers are `check_plan`-validated, not just "
        "\"the search returned\").\n"
    )
    lines.append(
        "pyperplan runs in-process while the Rust engine round-trips through a "
        "subprocess per solve, so the wall-time columns are not perfectly "
        "apples-to-apples -- read them as orders of magnitude, not to the "
        "millisecond.\n"
    )
    path.write_text("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--instance-dir", type=Path, default=DEFAULT_INSTANCE_DIR,
        help="Blocksworld chunk to read (default: the DVC pin at %(default)s)",
    )
    p.add_argument(
        "--out-dir", type=Path, default=DEFAULT_OUT_DIR,
        help="where results.csv/results.md are written (default: %(default)s)",
    )
    p.add_argument(
        "--limit", type=int, default=None,
        help="run only the first N instances of each subset (smoke runs)",
    )
    args = p.parse_args(argv)

    instance_dir = resolve_instance_dir(args.instance_dir)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    rows = run_all(instance_dir, limit=args.limit)
    write_csv(rows, args.out_dir / "results.csv")
    write_report(rows, args.out_dir / "results.md")
    print(f"done in {time.time()-t0:.1f}s -- wrote results.csv, results.md to {args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
