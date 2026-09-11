#!/usr/bin/env python3
"""Does a plan of a DIFFERENT SHAPE hit shards a previous plan wrote?

`68074d5e` showed the plan key is stable for the SAME plan. That is not the
property the campaign rests on. What it needs is reuse across a CHANGED plan:
swap the binner, keep the assembly, and have the QC and assembly members hit.

Reading `caching/invocation.py`, a member key folds the transform, its protocol
signature, and the ids it consumed -- and `structural_slot_id` folds neither step
order nor sample id. So the shared prefix SHOULD hit. Should is not a measurement,
so this runs the two plans and reads `cache_hits.jsonl`.

Two arms over one sample, differing only in the binner. Arm A populates, arm B
measures. A hit in arm B is one line per member in the hits log, and the task
lands in the trace as `<name>_cached` because the twin process runs locally.
"""

import argparse
import json
import sys
from pathlib import Path

import run_cami_metag as D
from metasmith.python_api import (
    Agent, Source, Runtime, DataInstanceLibrary,
    TargetBuilder, Resources, Size, Duration,
)


def probe_targets(binner: str):
    """The shortest plan that still crosses QC, assembly, coverage and a binner."""
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    t.Add(f"sequences::{binner}_bin_fasta", parents=[asm])
    t.Add(f"binning::{binner}_contig_to_bin_table", parents=[asm])
    return t


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binner", required=True, choices=["metabat2", "semibin2"])
    p.add_argument("--sample", default=None, help="default: the first enumerated")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stage-only", action="store_true")
    p.add_argument("--on-exist", default="update", choices=["update", "clear"])
    a = p.parse_args()

    samples = D.enumerate_samples()
    if not samples:
        print("no samples found", file=sys.stderr)
        return 1
    if a.sample:
        samples = [s for s in samples if s[0] == a.sample]
        if not samples:
            print(f"sample {a.sample} not found", file=sys.stderr)
            return 1
    samples = samples[:1]
    sid = samples[0][0]

    inputs = D.build_inputs(samples)
    containers = DataInstanceLibrary.Load(D.MLIB / "resources" / "env")
    smith = (Agent(home=Source.FromLocal(D.CACHE_DIR / "dryrun_home"),
                   runtime=Runtime.APPTAINER)
             if a.dry_run else D.get_agent())
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[containers, inputs],
        transforms=D.build_transforms(),
        targets=probe_targets(a.binner),
    )
    if not task.ok:
        D._report_plan_failure(task)

    key = task.GetKey()
    print(f"arm={a.binner} sample={sid} key={key} steps={len(task.plan.steps)}")
    for s in task.plan.steps:
        print(f"  {s.order:>2}. {Path(s.transform._path).stem}")

    keys_file = D.CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[f"cacheprobe_{a.binner}"] = key
    keys_file.write_text(json.dumps(keys, indent=2))

    if a.dry_run:
        print("(dry-run)")
        return 0

    smith.StageWorkflow(task, on_exist=a.on_exist, verify_external_paths=False)
    if a.stage_only:
        print(f"(stage-only; {key})")
        return 0

    smith.RunWorkflow(
        task=task, config_file=D.make_slurm_config(),
        params=dict(slurmAccount=D.SLURM_ACCOUNT,
                    executor=dict(queueSize=100),
                    process=dict(tries=2, array=10)),
        resource_overrides={
            "bbduk":   Resources(memory=Size.GB(64), cpus=16),
            "megahit": Resources(memory=Size.GB(128), cpus=32,
                                 duration=Duration(hours=12)),
        },
    )
    print(f"submitted {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
