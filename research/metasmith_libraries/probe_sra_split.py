#!/usr/bin/env python3
"""Solve a short accession to an assembly (against a FASTQ pair) and a long accession to long-read taxonomy.

Opens nothing and runs nothing.

    python research/metasmith_libraries/probe_sra_split.py [pair|short|long ...]
"""
import sys
import time
from collections import Counter
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))
import _authoring as A  # noqa: E402
from metasmith.python_api import DEFERRED, Plan, Spec  # noqa: E402

TYPE_LIBS = ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml", "taxonomy.yml", "ncbi.yml")
REFS = ("ref::metaphlan_db", "ref::sylph_db", "ref::kraken2_db", "ref::ganon2_db")
ASSEMBLY = ["sequences::assembly"]
LONG_TAXONOMY = ["taxonomy::metaphlan_profile", "annotation::strainphlan_consensus_markers",
                 "taxonomy::sylph_profile", "taxonomy::kraken2_report", "taxonomy::ganon2_report"]


def sample(lib, kind):
    length = "long" if kind == "long" else "short"
    name = lib.AddValue("s1_name.txt", "s1", "sequences::sample_name")
    meta = lib.AddValue("s1_reads_metadata.json", {"parity": "paired", "length_class": length},
                        "sequences::read_metadata", parents={name})
    if kind == "pair":
        pair = lib.AddValue("s1_read_pair.txt", "s1", "sequences::read_pair", parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    else:
        lib.AddValue("s1.acc", "SRR5585544", f"ncbi::sra_{kind}_read_accession", parents={meta})


CASES = {"pair": ASSEMBLY, "short": ASSEMBLY, "long": LONG_TAXONOMY}


def main(argv):
    for kind in argv or CASES:
        def inputs(lib, kind=kind):
            for tl in TYPE_LIBS:
                lib.AddTypeLibrary(A.TYPES / tl)
            sample(lib, kind)
            for t in REFS:
                lib.AddItem(DEFERRED, t)
        spec = Spec(
            input_library=A.deferred_inputs(f"probe_sra_{kind}", inputs, rebuild=True),
            sample_type="sequences::read_metadata",
            shared_input_paths=[],
            target_types=CASES[kind],
        )
        t0 = time.time()
        task = Plan([spec], transform_libraries=A.transforms("logistics", "assembly", "metagenomics"),
                    resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"], max_iter=512)
        plan = task.plan
        print(f"[{kind}] ok={task.ok} steps={len(plan.steps)} solve={time.time() - t0:.1f}s dropped={sorted(plan.dropped_targets)}")
        print("  " + ", ".join(f"{k}x{v}" if v > 1 else k
                               for k, v in sorted(Counter(Path(s.transform._path).stem for s in plan.steps).items())))
        for h in (plan.hints or [])[:10]:
            print(f"  hint {getattr(h, 'kind', '?')}: {getattr(h, 'message', h)}"[:500])


if __name__ == "__main__":
    main(sys.argv[1:])
