"""Regenerate the measured half of `COSTS.md` from `scoreboard.tsv`.

The measured rows are generated rather than transcribed, because a hand-maintained cost
table is one a later session updates in the prose and not in the number. Everything
between the two marker comments in `COSTS.md` is owned by this script; everything outside
them is prose and is left alone.

    PYTHONPATH=src mamba run -n ecspr python research/fabfos/llm_curation/costs.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
BEGIN = "<!-- BEGIN generated: costs.py -->"
END = "<!-- END generated -->"

# The residual is what the AAM lane is actually run over; the universe is what the
# direction lane is. Quoting one number for both is the mistake this table avoids.
AAM_TARGET = 12_417
DIRECTION_TARGET = 83_795
AAM_OPINIONS = 3
DIRECTION_ORIENTATIONS = 2


def table(sb: pd.DataFrame) -> str:
    if sb.empty:
        return "_No runs recorded yet._\n"
    rows = ["| model | revision | split | n | tok/reaction | rxn/min | parallel | note |",
            "|---|---|---|---:|---:|---:|---:|---|"]
    for r in sb.itertuples(index=False):
        rows.append(f"| `{r.model}` | `{r.revision}` | {r.split} | {r.n} | "
                    f"{r.tokens_per_rxn} | {r.rxn_per_min} | "
                    f"{getattr(r, 'parallel', '')} | {r.note} |")
    return "\n".join(rows) + "\n"


def projection(sb: pd.DataFrame) -> str:
    if sb.empty:
        return "\n_Projection needs at least one measured run._\n"
    aam = sb[sb.split.astype(str).str.startswith(("dev", "heldout"))]
    if aam.empty:
        aam = sb
    # The most recent run of the best-covered revision is the honest rate: an early
    # revision's cost describes a prompt nobody will run.
    rate = int(aam.tokens_per_rxn.iloc[-1])
    per_min = float(aam.rxn_per_min.iloc[-1] or 0)
    aam_tok = rate * AAM_TARGET * AAM_OPINIONS
    dir_tok = rate * DIRECTION_TARGET * DIRECTION_ORIENTATIONS
    hours = (AAM_TARGET * AAM_OPINIONS + DIRECTION_TARGET * DIRECTION_ORIENTATIONS) \
        / per_min / 60 if per_min else float("nan")
    return (
        f"\nAt the most recent measured rate of **{rate:,} tokens/reaction** and "
        f"**{per_min:,.0f} reactions/min**:\n\n"
        f"| lane | reactions | passes | completions | tokens |\n|---|---:|---:|---:|---:|\n"
        f"| AAM, over the residual | {AAM_TARGET:,} | {AAM_OPINIONS} | "
        f"{AAM_TARGET * AAM_OPINIONS:,} | {aam_tok / 1e6:,.0f} M |\n"
        f"| direction, over the universe | {DIRECTION_TARGET:,} | "
        f"{DIRECTION_ORIENTATIONS} | "
        f"{DIRECTION_TARGET * DIRECTION_ORIENTATIONS:,} | {dir_tok / 1e6:,.0f} M |\n"
        f"| **total** | | | "
        f"{AAM_TARGET * AAM_OPINIONS + DIRECTION_TARGET * DIRECTION_ORIENTATIONS:,} | "
        f"**{(aam_tok + dir_tok) / 1e6:,.0f} M** |\n\n"
        f"That is roughly **{hours:,.1f} GPU-hours** on one card at the measured "
        f"throughput, which is the number a held allocation is actually billed on. The "
        f"direction rate is measured on the AAM prompt here and will be lower in "
        f"practice: a direction call is one word out, an AAM rewrite is a whole "
        f"equation.\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scoreboard", type=Path, default=HERE / "scoreboard.tsv")
    ap.add_argument("--costs", type=Path, default=HERE / "COSTS.md")
    a = ap.parse_args()

    sb = (pd.read_csv(a.scoreboard, sep="\t") if a.scoreboard.exists()
          else pd.DataFrame())
    body = f"{BEGIN}\n\n{table(sb)}{projection(sb)}\n{END}"

    text = a.costs.read_text()
    if BEGIN in text and END in text:
        head, rest = text.split(BEGIN, 1)
        text = head + body + rest.split(END, 1)[1]
    else:
        text = text.rstrip() + "\n\n" + body + "\n"
    a.costs.write_text(text)
    print(f"{len(sb)} scoreboard rows -> {a.costs}")


if __name__ == "__main__":
    main()
