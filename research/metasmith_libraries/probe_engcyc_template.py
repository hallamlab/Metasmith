#!/usr/bin/env python3
"""Plan engcyc_metagenomics for a short-read sample, a hybrid sample and a mixed study.

Opens nothing and runs nothing. A short-read sample goes to a case with the template's
targets, a hybrid sample to a case with the hybrid targets, and a mixed study plans both
cases at once. Prints, per study, whether it planned, its steps, its planning time and any
dropped targets.

    python research/metasmith_libraries/probe_engcyc_template.py [--max-iter N] [short|hybrid|mixed ...]

The default budget is the one `build_templates.py` plans with.
"""
import sys
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))
import engcyc_metagenomics as E  # noqa: E402
from metasmith.python_api import Plan  # noqa: E402

STUDIES = {
    "short": [("short_1", False)],
    "hybrid": [("hybrid_1", True)],
    "mixed": [("short_1", False), ("hybrid_1", True)],
}


def plan(name, samples, max_iter=512):
    def study(lib, root):
        for sample, long_reads in samples:
            E.add_sample(lib, root, sample, long_reads=long_reads)

    template = E.template_for(f"probe_engcyc_{name}", study)
    (base,) = template.cases

    def case(label, targets, hybrid):
        picked = [f"{s}_reads_metadata.json" for s, long_reads in samples if long_reads == hybrid]
        return replace(base, name=label, input_paths=picked, target_types=targets) if picked else None

    cases = [c for c in (case("short", E.TARGETS, False), case("hybrid", E.HYBRID_TARGETS, True)) if c]
    return Plan(cases, template.transform_libraries, template.resource_libraries, max_iter=max_iter)


def main(argv):
    max_iter = 512
    if argv[:1] == ["--max-iter"]:
        max_iter, argv = int(argv[1]), argv[2:]
    for name in argv or STUDIES:
        t0 = time.time()
        task = plan(name, STUDIES[name], max_iter=max_iter)
        dt = time.time() - t0
        p = task.plan
        print(f"[{name}] max_iter={max_iter} ok={task.ok} steps={len(p.steps)} plan={dt:.1f}s "
              f"dropped={sorted(p.dropped_targets)}")
        counts = Counter(Path(s.transform._path).stem for s in p.steps)
        print("  " + ", ".join(f"{k}x{v}" if v > 1 else k for k, v in sorted(counts.items())))
        for h in (p.hints or [])[:10]:
            print(f"  hint {getattr(h, 'kind', '?')}: {getattr(h, 'message', h)}"[:600])


if __name__ == "__main__":
    main(sys.argv[1:])
