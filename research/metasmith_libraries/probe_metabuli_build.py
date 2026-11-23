#!/usr/bin/env python3
"""Solve the Metabuli database from nothing, and a short-read sample to the Metabuli lane with no database given.

Opens nothing and runs nothing.

    python research/metasmith_libraries/probe_metabuli_build.py [db|lane ...]
"""
import sys
import time
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))
import _authoring as A  # noqa: E402
from metasmith.python_api import DEFERRED, Spec  # noqa: E402

TYPE_LIBS = ("sequences.yml", "ref.yml", "taxonomy.yml")


def short_read_sample(lib):
    name = lib.AddValue("s1_name.txt", "s1", "sequences::sample_name")
    meta = lib.AddValue("s1_reads_metadata.json", {"parity": "paired", "length_class": "short"},
                        "sequences::read_metadata", parents={name})
    pair = lib.AddValue("s1_read_pair.txt", "s1", "sequences::read_pair", parents={meta})
    lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
    lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})


CASES = {
    "db": (None, None, ["ref::metabuli_ref"]),
    "lane": (short_read_sample, "sequences::read_metadata", ["taxonomy::metabuli"]),
}


def main(argv):
    for case in argv or CASES:
        given, sample_type, targets = CASES[case]

        def inputs(lib, given=given):
            for tl in TYPE_LIBS:
                lib.AddTypeLibrary(A.TYPES / tl)
            if given:
                given(lib)
        spec = Spec(
            input_library=A.deferred_inputs(f"probe_metabuli_{case}", inputs, rebuild=True),
            sample_type=sample_type,
            shared_input_paths=[],
            target_types=targets,
            transform_libraries=A.transforms("logistics", "assembly", "metagenomics"),
            resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
        )
        t0 = time.time()
        task = spec.Solve(max_iter=512)
        plan = task.plan
        print(f"[{case}] ok={task.ok} steps={len(plan.steps)} solve={time.time() - t0:.1f}s dropped={sorted(plan.dropped_targets)}")
        for s in plan.steps:
            print(f"  {Path(s.transform._path).stem}")
        for h in (plan.hints or [])[:10]:
            print(f"  hint {getattr(h, 'kind', '?')}: {getattr(h, 'message', h)}"[:500])


if __name__ == "__main__":
    main(sys.argv[1:])
