#!/usr/bin/env python3
"""Solve the ported ASPIRE amplicon pipeline and draw the DAG. Nothing else.

No agent, no deployment, no execution -- this runs anywhere, including a laptop
with no cluster access. It exists to answer one question while the port is still
stubs: does the topology in `transforms/aspire/` close, and does the picture look
like the pipeline?

    python research/aspire/aspire_asv_pipeline.py            # the core spine
    python research/aspire/aspire_asv_pipeline.py all --dag  # everything, as an SVG

Paths are irrelevant to a plan, so every input is `DEFERRED` and nothing here is
opened. The library examples under `research/metasmith_libraries/examples/` drive real
clusters and refuse to start until their site config is filled in; this one has
no site config to fill.

## The switches

ASPIRE has ~35 config toggles. The eight that have downstream consumers cannot
be config here, because Metasmith has no way to rebind the channel eleven
consumers read -- so each is a pair of mutually exclusive input tokens, and
which one this driver registers is what selects the arm. `--on`/`--off` move
them; `--switches` lists them. Flipping one changes the rendered DAG
deterministically, which a config flag threaded through a dozen call sites
never quite did.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MLIB = REPO / "src" / "metasmith_libraries"

if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])

from metasmith.python_api import (  # noqa: E402
    DEFERRED, DataInstanceLibrary, Spec, TransformInstanceLibrary,
)

# Solver scratch. `/cache` is already git-ignored at the repo root.
CACHE = REPO / "cache" / "aspire"
TRANSFORMS = [MLIB / "transforms" / n for n in ("aspire", "logistics")]

# The topology table is the one place the port lives; reading the switch list
# and the leaf types out of it rather than restating them here is what keeps
# `all` honest when a row is added or two nodes are collapsed.
_spec = importlib.util.spec_from_file_location(
    "_aspire_topology", MLIB / "transforms" / "aspire" / "_generate.py")
TOPOLOGY = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(TOPOLOGY)

SWITCHES: dict[str, str] = {base: desc for base, desc in TOPOLOGY.POLICIES}

# What the pipeline is normally run with: every stage on except the two that
# rewrite the analysis tables underneath their consumers.
DEFAULT_ON = {
    "augmentation": False,
    "batch_correction": False,
    "indicspecies": True,
    "spieceasi": True,
    "network_modules": True,
    "asv_mag_link": True,
    "graph_network": True,
    "sankey": True,
}


ALL_TOKENS = {f"aspire::{b}_{arm}" for b in SWITCHES for arm in ("on", "off")}

# References the .nf read out of its config -- two of them synchronously, during
# Groovy parsing, before any process ran. Here they are ordinary inputs, which is
# what lets `--download-silva` withhold one and get a download step in the DAG.
REFERENCES = [
    "aspire::sample_metadata",
    "aspire::sina_arb_reference",
    "aspire::silva_ref_taxonomy",
    "aspire::mito_reference_source",
    "aspire::contaminant_reference_source",
    "amplicon::silva_db",
]
# The one off-arm that is not a null producer: with SpiecEasi off the .nf reads
# pre-computed graphs off disk (asv_pipeline.nf:2708-2715).
EXTERNAL_GRAPH = [
    "aspire::external_graph_all",
    "aspire::external_graph_thr",
    "aspire::external_node_features",
]


def given_types(on: dict[str, bool]) -> set[str]:
    """What `build_inputs` registers. `amplicon::silva_db` counts either way --
    withheld, transforms/logistics/downloadSilvaDB produces it."""
    given = {
        "aspire::run", "aspire::sample_id", "sequences::read_pair",
        "sequences::zipped_forward_short_reads", "sequences::zipped_reverse_short_reads",
        *REFERENCES,
    }
    given |= {f"aspire::{b}_{'on' if v else 'off'}" for b, v in on.items()}
    if not on["spieceasi"]:
        given |= set(EXTERNAL_GRAPH)
    return given


def reachable_leaves(on: dict[str, bool]) -> list[str]:
    """One target per terminal transform, dropping the ones this switch
    setting makes unbuildable.

    `all` asks for the end of every chain rather than for every leaf *type*:
    the 45-process port has 59 unconsumed products, and the MCTS planner is not
    the tool for a 59-way ask when fifteen of them pull in all the rest.

    The drop is not cosmetic. `outlier_checker` reads the CLR matrix, which only
    the batch-correction arm produces -- exactly as in the .nf, where turning
    correction off leaves that channel null. Asking for it anyway would make
    `all` fail by default rather than show the pipeline you configured.
    """
    enabled = [r for r in TOPOLOGY.TABLE
               if not any(d in ALL_TOKENS and d not in given_types(on)
                          for _v, d, _p in r.requires)]
    consumed = {d for r in enabled for _v, d, _p in r.requires}

    have = given_types(on)
    while True:
        grew = False
        for row in enabled:
            if all(d in have for _v, d, _p in row.requires):
                for _v, d in row.products:
                    if d not in have:
                        have.add(d)
                        grew = True
        if not grew: break

    leaves = []
    for row in enabled:
        if any(d in consumed for _v, d in row.products): continue
        if not all(d in have for _v, d, _p in row.requires): continue
        leaves.append(row.products[0][1])
    return leaves


# Named asks, smallest first. Each is a slice of the pipeline someone would
# actually want to look at; `all` is the stress case.
#
# Target counts are kept small on purpose. The MCTS planner degrades once a
# solve carries more than a handful of *divergent* chains, and these all share
# one spine, so a case asks for the few leaves that pull the rest in rather
# than naming everything it wants built.
CASES: dict[str, list[str]] = {
    "core": [
        "amplicon::asv_taxonomy",
        "aspire::counts_filtered",
        "aspire::sankey_outputs",
    ],
    "mito": [
        "aspire::mito_summary_tables",
        "aspire::mito_plots",
        "aspire::counts_micro",
    ],
    "metadata": [
        "aspire::analysis_metadata",
        "aspire::analysis_asv_meta",
        "aspire::analysis_counts",
    ],
    "analysis": [
        "aspire::diversity_outputs",
        "aspire::collectors_outputs",
        "aspire::indicspecies_plots",
        "aspire::clustermap_outputs",
        "aspire::power_analysis_outputs",
    ],
    "networks": [
        "aspire::network_outputs",
        "aspire::asv_mag_network_outputs",
        "aspire::sample_module_heatmaps",
    ],
    "summary": [
        "aspire::master_long",
    ],
    # "all" is computed from the switches; see reachable_leaves.
    "all": [],
}


def targets_for(case: str, on: dict[str, bool]) -> list[str]:
    return reachable_leaves(on) if case == "all" else CASES[case]


def build_inputs(location: Path, samples: int, on: dict[str, bool],
                 download_silva: bool) -> DataInstanceLibrary:
    """The study, its samples, its references, and one token per switch."""
    if location.exists(): shutil.rmtree(location)
    inputs = DataInstanceLibrary(location)
    inputs.Purge()
    for ns in ("aspire.yml", "amplicon.yml", "sequences.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / ns)

    # ASPIRE is one study over N samples with a hard fan-in at CONCAT_FASTAS,
    # and everything past it is a singleton. `run` is what makes that
    # expressible: per-sample stages group by `sample_id`, the collector groups
    # by `run`, and the collector recovers each sequence's label from the
    # `sample_id` its fasta descends from.
    run = inputs.AddValue("run.txt", "aspire_study", "aspire::run")
    for i in range(1, samples + 1):
        sid = inputs.AddValue(f"sample_{i}.txt", f"sample_{i}",
                              "aspire::sample_id", parents={run})
        pair = inputs.AddValue(f"read_pair_{i}.txt", f"sample_{i}",
                               "sequences::read_pair", parents={sid})
        inputs.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        inputs.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})

    for dtype in REFERENCES:
        if dtype == "amplicon::silva_db" and download_silva: continue
        inputs.AddItem(DEFERRED, dtype)

    # Exactly one token per switch. The losing arm's transform has zero
    # candidates for its token slot, so the solver never instantiates it --
    # that, and not a search preference, is what makes the choice stick.
    #
    # They hang off `run` so a driver that does split the library by sample
    # cannot mask them out from underneath the stages that need them.
    for base, enabled in on.items():
        arm = "on" if enabled else "off"
        inputs.AddValue(f"policy_{base}.txt", arm, f"aspire::{base}_{arm}", parents={run})
    if not on["spieceasi"]:
        for dtype in EXTERNAL_GRAPH:
            inputs.AddItem(DEFERRED, dtype)

    inputs.Save()
    return inputs


def transform_names() -> dict[str, str]:
    return {
        ti.model.key: (ti.name or str(path))
        for location in TRANSFORMS
        for path, ti in TransformInstanceLibrary.Load(location).IterateTransforms()
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("case", nargs="?", default="core", choices=sorted(CASES),
                    help="which slice of the pipeline to ask for (default: core)")
    ap.add_argument("--samples", type=int, default=3,
                    help="how many samples to fan out over (default: 3)")
    ap.add_argument("--on", action="append", default=[], metavar="SWITCH",
                    help="turn a switch on; repeatable, see --switches")
    ap.add_argument("--off", action="append", default=[], metavar="SWITCH",
                    help="turn a switch off; repeatable, see --switches")
    ap.add_argument("--switches", action="store_true",
                    help="list the switches and their defaults, then exit")
    ap.add_argument("--download-silva", action="store_true",
                    help="withhold the SILVA reference so the planner fetches it")
    ap.add_argument("--dag", action="store_true",
                    help="render the solved DAG to results/aspire_dags/ (git-ignored)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-iter", type=int, default=1024)
    ap.add_argument("--max-refine", type=int, default=256)
    args = ap.parse_args()

    if args.switches:
        print("switch                default  effect when on")
        for base, desc in SWITCHES.items():
            print(f"  {base:<20}{'on' if DEFAULT_ON[base] else 'off':<9}{desc}")
        return 0

    on = dict(DEFAULT_ON)
    for name in args.on + args.off:
        if name not in on:
            print(f"unknown switch [{name}]; choices: {', '.join(on)}")
            return 2
    on.update({n: True for n in args.on})
    on.update({n: False for n in args.off})

    targets = targets_for(args.case, on)
    flags = " ".join(f"{'+' if v else '-'}{k}" for k, v in on.items())
    print(f"[{args.case}] {len(targets)} target(s), {args.samples} sample(s)")
    print(f"  switches: {flags}")

    inputs = build_inputs(CACHE / f"aspire_{args.case}.xgdb", args.samples, on,
                          args.download_silva)
    spec = Spec(
        input_library=inputs,
        target_types=targets,
        transform_libraries=TRANSFORMS,
        # The env declarations. No aspire transform requires one yet -- ASPIRE
        # is 31 conda environments and zero containers, and that is a separate
        # port -- but transforms/logistics does, so without this `--download-silva`
        # dead-ends on `env::python_for_data_science.env`.
        resource_libraries=[MLIB / "resources" / "env"],
        # No sample_type: ASPIRE is one study, and the whole library is the one
        # view the planner needs. Splitting by sample would hand the collecting
        # transform one sample at a time and hide the references from all of
        # them.
        sample_type=None,
    )
    task = spec.Solve(max_iter=args.max_iter, max_refine=args.max_refine, seed=args.seed)

    if not task.ok:
        # Never print `task.plan` -- its repr embeds the whole search tree.
        print(f"  PLAN FAILED steps={len(task.plan.steps)}")
        print(f"  dropped: {sorted(task.plan.dropped_targets)}")
        # A PlanHint's repr carries its whole chain and every near miss; the
        # message and the dead end are the two lines you actually read.
        for hint in getattr(task.plan, "hints", []) or []:
            print(f"  {getattr(hint, 'message', hint)}")
            for link in (getattr(hint, "chain", None) or [])[-2:]:
                print(f"      {link}")
        return 1

    names = transform_names()
    print(f"  OK {len(task.plan.steps)} steps")
    for i, step in enumerate(task.plan.steps, 1):
        key = getattr(step.transform, "model", None)
        key = key.key if key is not None else getattr(step.transform, "_key", "?")
        print(f"    {i:2d}. {names.get(key, f'<unmapped {key}>')}")

    if args.dag:
        out = REPO / "research/aspire/reports/dag" / f"{args.case}"
        out.parent.mkdir(parents=True, exist_ok=True)
        print(f"  dag: {task.plan.RenderDAG(str(out), format='svg')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
