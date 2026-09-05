#!/usr/bin/env python3
"""Build the ladder of growing target sets over one metagenomics sample, and
encode each rung to the solver wire format.

    PYTHONPATH="$PWD/src" mamba run -n msm python \
        research/metasmith/solver_ratchet/build_ladder.py <outdir> --probe --emit

The ladder is the corpus report 04 measured node selection on: one sample --
`metagenomics_from_paired_reads`'s paired short reads -- its four transform
libraries, and a target list that grows. Composing several templates into one
task is unsound: every sample becomes its own timeline and every target is
demanded of every timeline, so a mixed input type fails on reachability while
looking like a search failure. Growing the target set over one sample is the
only axis that grows the search.

Three sources of rungs, in order:

  * prefixes of the template's own 25 targets. A prefix is always well formed
    because a target's `parents` only ever index targets declared earlier;
  * blocks lifted from the assembly-rooted templates (annotation palette,
    annotation trio, viromics survey, CLEAN), re-rooted at the metagenomics
    assembly rather than at a supplied one;
  * types found reachable by `--probe`: each declared type solved alone as a
    two-target case beside the assembly, keeping the ones that come back
    complete.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
MLIB = ROOT / "src" / "metasmith_libraries"
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(MLIB))

from metasmith.agents.spec import Spec, _as_data_lib, _as_transform_lib  # noqa: E402
from metasmith.models.libraries import DataInstanceLibraryView  # noqa: E402

import metagenomics_from_paired_reads as TEMPLATE  # noqa: E402

ASSEMBLY = 0  # index of `sequences::megahit_assembly` in TEMPLATE.TARGETS

# Namespaces that hold inputs rather than products: environments, script
# libraries, reference data and the transform meta-type. Excluding them leaves
# the 192 candidate types the probe walks.
NON_PRODUCT_NAMESPACES = {"env", "lib", "ref", "transforms"}

# Each of these is reachable in its own template and NOT from this one, checked
# alone before being dropped. They are reachability failures, not search
# failures, and leaving one in drops every other target with it.
DROPPED = {
    # wants the palette template's `eggnog_source.marker` shared input
    "annotation::eggnog_results",
    # wants an ancestor the reads root does not supply
    "annotation::crassphage_coverage",
    # declared over the fosmid insert lineage, not over an assembly
    "annotation::gpr_table",
    "ecspr::results",
    # probes complete alone beside the assembly and is dropped the moment the
    # template's own targets share the timeline -- reachable only in isolation,
    # and one unreachable target drops every other target with it
    "sequences::putative_genome",
}

# Targets taken from the assembly-rooted templates, in the order they are
# appended. Each becomes `{type, parents:[0]}`: what the same target means when
# the assembly is being built rather than handed over.
BLOCKS = [
    ("palette", [
        "annotation::kofamscan_results",
        "annotation::kofamscan_descriptions",
        "annotation::diamond_uniref50_results",
        "annotation::diamond_uniref50_descriptions",
        "annotation::eggnog_results",
        "annotation::proteinbert_embeddings",
        "taxonomy::metabuli",
    ]),
    ("trio", [
        "annotation::kofamscan_results",
        "annotation::kofamscan_descriptions",
        "annotation::diamond_uniref50_results",
        "annotation::diamond_uniref50_descriptions",
        "annotation::interproscan_results",
        "annotation::interproscan_descriptions",
    ]),
    ("viromics", [
        "annotation::virsorter2_viral_sequences",
        "taxonomy::genomad_virus_summary",
        "taxonomy::genomad_plasmid_summary",
        "annotation::dramv_distill",
        "annotation::crassphage_coverage",
    ]),
    ("clean", [
        "annotation::kofamscan_results",
        "annotation::clean_predictions",
        "annotation::diamond_uniref50_results",
        "annotation::proteinbert_embeddings",
        "annotation::gpr_table",
    ]),
]

# Prefix lengths of the shipped 25.
PREFIX_RUNGS = [3, 10, 15, 20, 25]


# --------------------------------------------------------------------------
# spec assembly


def _spec():
    return TEMPLATE.build_spec()


def _views(spec):
    data_lib = _as_data_lib(spec.input_library)
    samples = [
        s if isinstance(s, DataInstanceLibraryView) else DataInstanceLibraryView(s)
        for s in data_lib.AsSamples(spec.sample_type)
    ]
    resources = [_as_data_lib(x) for x in spec.resource_libraries]
    transforms = [_as_transform_lib(x) for x in spec.transform_libraries]
    return samples, resources, transforms


def declared_types(transforms) -> list[str]:
    out = set()
    for trlib in transforms:
        for ns, tlib in trlib.types.items():
            if ns in NON_PRODUCT_NAMESPACES:
                continue
            for name in list(tlib):
                out.add(f"{ns}::{name[0] if isinstance(name, tuple) else name}")
    return sorted(out)


def _type_of(target) -> str:
    return target["type"] if isinstance(target, dict) else target


# --------------------------------------------------------------------------
# the ladder


def block_appendix() -> list[tuple[str, str]]:
    """The block-sourced targets, deduplicated against the shipped 25.

    A target is a slot: `TargetBuilder` refuses a second target of one type with
    identical parents, and every re-rooted block target carries `parents:[0]`,
    so a type the template already pins to the assembly is not appendable.
    """
    have = {_type_of(t) for t in TEMPLATE.TARGETS}
    out: list[tuple[str, str]] = []
    for block, types in BLOCKS:
        for t in types:
            if t in have or t in DROPPED:
                continue
            have.add(t)
            out.append((block, t))
    return out


def probe_pool(cache: Path) -> list[str]:
    """Types the probe found reachable, minus the fan-out intermediates.

    `_chunk` types are a transform's internal fan-out unit rather than something
    a user names, so they are excluded even though they solve.
    """
    if not cache.exists():
        return []
    keep = []
    for line in cache.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("complete") and "_chunk" not in row["type"]:
            keep.append(row["type"])
    return sorted(set(keep))


def full_target_list(cache: Path) -> list:
    """The shipped 25, then the blocks, then the probe pool -- all re-rooted."""
    targets = list(TEMPLATE.TARGETS)
    have = {_type_of(t) for t in targets}
    for _block, t in block_appendix():
        targets.append({"type": t, "parents": [ASSEMBLY]})
        have.add(t)
    for t in probe_pool(cache):
        if t in have or t in DROPPED:
            continue
        have.add(t)
        targets.append({"type": t, "parents": [ASSEMBLY]})
    return targets


def rungs(cache: Path, wanted: list[int] | None = None) -> list[tuple[int, list]]:
    full = full_target_list(cache)
    sizes: list[int] = list(PREFIX_RUNGS)
    n = len(TEMPLATE.TARGETS)
    seen_block = None
    for block, _t in block_appendix():
        if seen_block is not None and block != seen_block:
            sizes.append(n)
        seen_block = block
        n += 1
    sizes.append(n)
    tail = len(full)
    for size in (43, 53, 62, tail):
        if size > n and size <= tail:
            sizes.append(size)
    sizes = sorted({s for s in sizes if s <= tail})
    if wanted:
        sizes = sorted({s for s in wanted if s <= tail})
    return [(s, full[:s]) for s in sizes]


# --------------------------------------------------------------------------
# solving and encoding


def solve(targets, *, max_iter: int = 256, max_refine: int = 8, seed: int = 42):
    spec = _spec()
    samples, resources, transforms = _views(spec)
    return Spec.SolveViews(
        samples=samples, resources=resources, transforms=transforms,
        targets=list(targets), max_iter=max_iter, max_refine=max_refine, seed=seed,
    )


def encode(targets, *, max_iter: int = 256, max_refine: int = 8, seed: int = 42) -> dict:
    """The wire payload the engine would be handed, without solving it.

    `solve_by_mcts` is where `max_refine=None` is resolved and the givens are
    canonicalised, so the interception has to sit after both or the payload is
    not the one the engine sees.
    """
    from metasmith.models.solver_wire import encode_problem
    from metasmith.models.solver_engine import SOLVER_WIRE_VERSION
    from metasmith.models.solver import _canonicalise_givens
    import metasmith.models.solver as S
    import metasmith.models.workflow.plan as P

    captured: dict = {}

    class _Done(Exception):
        pass

    def capture(given, transforms, target, seed=42, max_iter=256, max_refine=None):
        from metasmith.models.solver_backend import REFINER_BUDGET
        if max_refine is None:
            max_refine = REFINER_BUDGET
        _canonicalise_givens(given)
        captured["enc"] = encode_problem(
            given, transforms, target,
            seed=seed, max_iter=max_iter, max_refine=max_refine,
            wire_version=SOLVER_WIRE_VERSION,
        )
        raise _Done()

    real_s, real_p = S.solve_by_mcts, P.solve_by_mcts
    S.solve_by_mcts = capture
    P.solve_by_mcts = capture
    try:
        solve(targets, max_iter=max_iter, max_refine=max_refine, seed=seed)
    except _Done:
        pass
    finally:
        S.solve_by_mcts = real_s
        P.solve_by_mcts = real_p
    assert "enc" in captured, "never reached the solver"
    return captured["enc"].payload


# --------------------------------------------------------------------------
# commands


def cmd_probe(args) -> None:
    cache: Path = args.probe_cache
    cache.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if cache.exists():
        for line in cache.read_text().splitlines():
            if line.strip():
                done.add(json.loads(line)["type"])

    spec = _spec()
    _, _, transforms = _views(spec)
    shipped = {_type_of(t) for t in TEMPLATE.TARGETS}
    candidates = [t for t in declared_types(transforms)
                  if t not in shipped and t not in done]
    print(f"probing {len(candidates)} types ({len(done)} cached)", flush=True)

    for i, dtype in enumerate(candidates):
        row = {"type": dtype}
        t0 = time.time()
        try:
            task = solve(
                ["sequences::megahit_assembly", {"type": dtype, "parents": [ASSEMBLY]}],
                max_iter=args.max_iter, max_refine=args.max_refine, seed=args.seed,
            )
            row["complete"] = bool(task.ok)
            row["steps"] = len(task.plan.steps)
            row["dropped"] = sorted(task.plan.dropped_targets)
        except Exception as e:  # a type nothing produces raises rather than drops
            row["complete"] = False
            row["error"] = f"{type(e).__name__}: {e}"[:200]
        row["seconds"] = round(time.time() - t0, 2)
        with cache.open("a") as fh:
            fh.write(json.dumps(row) + "\n")
        print(f"  [{i+1}/{len(candidates)}] {dtype} "
              f"{'ok' if row['complete'] else 'no'} {row['seconds']}s", flush=True)


def cmd_list(args) -> None:
    for n, targets in rungs(args.probe_cache, args.rungs):
        print(f"ladder-{n}: {n} targets, last={_type_of(targets[-1])}")


def cmd_emit(args) -> None:
    args.outdir.mkdir(parents=True, exist_ok=True)
    index = []
    for n, targets in rungs(args.probe_cache, args.rungs):
        payload = encode(targets, max_iter=args.max_iter,
                         max_refine=args.max_refine, seed=args.seed)
        out = args.outdir / f"ladder-{n}.json"
        out.write_text(json.dumps(payload))
        row = {
            "rung": n,
            "file": out.name,
            "nodes": len(payload["nodes"]),
            "transforms": len(payload["transforms"]),
            "properties": payload["n_properties"],
            "given_groups": len(payload["given"]),
            "target_requires": len(payload["transforms"][payload["target_index"]]["requires"]),
            "targets": [_type_of(t) for t in targets],
        }
        index.append(row)
        print(f"ladder-{n}.json  nodes={row['nodes']} transforms={row['transforms']} "
              f"props={row['properties']} requires={row['target_requires']}", flush=True)
    (args.outdir / "index.json").write_text(json.dumps(index, indent=2))


def cmd_verify(args) -> None:
    for n, targets in rungs(args.probe_cache, args.verify):
        t0 = time.time()
        task = solve(targets, max_iter=args.max_iter,
                     max_refine=args.max_refine, seed=args.seed)
        print(json.dumps({
            "rung": n,
            "complete": bool(task.ok),
            "steps": len(task.plan.steps),
            "dropped": sorted(task.plan.dropped_targets),
            "seconds": round(time.time() - t0, 2),
        }), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("outdir", type=Path, help="where ladder-<N>.json is written")
    ap.add_argument("--probe-cache", type=Path, default=None,
                    help="JSONL reachability cache (default: <outdir>/../probe.jsonl)")
    ap.add_argument("--probe", action="store_true", help="run/resume the reachability probe")
    ap.add_argument("--emit", action="store_true", help="encode the rungs to <outdir>")
    ap.add_argument("--list", action="store_true", help="print the rungs and stop")
    ap.add_argument("--verify", type=int, nargs="*", default=None,
                    help="solve these rungs (target counts) and report")
    ap.add_argument("--rungs", type=int, nargs="*", default=None,
                    help="restrict --emit/--list to these target counts")
    ap.add_argument("--max-iter", type=int, default=256)
    ap.add_argument("--max-refine", type=int, default=8)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    if args.probe_cache is None:
        args.probe_cache = args.outdir.parent / "probe.jsonl"

    if args.probe:
        cmd_probe(args)
    if args.list:
        cmd_list(args)
    if args.verify is not None:
        cmd_verify(args)
    if args.emit or not (args.probe or args.list or args.verify is not None):
        cmd_emit(args)


if __name__ == "__main__":
    main()
