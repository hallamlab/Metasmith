#!/usr/bin/env python3
"""Solve engcyc_metagenomics for a short-read sample, a hybrid sample and a mixed study.

Opens nothing and runs nothing. Prints, per study shape, whether it solved, its steps, its
solve time and any dropped targets.

    python research/metasmith_libraries/probe_engcyc_template.py [--max-iter N] [short|hybrid|mixed ...]

The default budget is the one `build_templates.py` solves with.
"""
import sys
import time
from collections import Counter
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))
import engcyc_metagenomics as E  # noqa: E402

STUDIES = {
    "short": lambda lib, study: E.add_sample(lib, study, "short_1"),
    "hybrid": lambda lib, study: E.add_sample(lib, study, "hybrid_1", long_reads=True),
    "mixed": lambda lib, study: (E.add_sample(lib, study, "short_1"),
                                 E.add_sample(lib, study, "hybrid_1", long_reads=True)),
}


def main(argv):
    max_iter = 256
    if argv[:1] == ["--max-iter"]:
        max_iter, argv = int(argv[1]), argv[2:]
    for name in argv or STUDIES:
        spec = E.spec_for(f"probe_engcyc_{name}", STUDIES[name])
        t0 = time.time()
        task = spec.Solve(max_iter=max_iter)
        dt = time.time() - t0
        plan = task.plan
        print(f"[{name}] max_iter={max_iter} ok={task.ok} steps={len(plan.steps)} solve={dt:.1f}s "
              f"dropped={sorted(plan.dropped_targets)}")
        counts = Counter(Path(s.transform._path).stem for s in plan.steps)
        print("  " + ", ".join(f"{k}x{v}" if v > 1 else k for k, v in sorted(counts.items())))
        for h in (plan.hints or [])[:10]:
            print(f"  hint {getattr(h, 'kind', '?')}: {getattr(h, 'message', h)}"[:600])


if __name__ == "__main__":
    main(sys.argv[1:])
