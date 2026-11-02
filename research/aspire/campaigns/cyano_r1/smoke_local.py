#!/usr/bin/env python3
"""Run the reads-to-filtered-table lane on two samples, locally, in docker.

    python research/aspire/campaigns/cyano_r1/smoke_local.py READS_DIR [plan|run]

READS_DIR holds ANA1_R1.fastq.gz and the other three halves. The SILVA steps and every
analysis wait for the sockeye run: the reference is too heavy for a workstation. The
table's depth filter is off, since two samples cannot meet a study's threshold.
"""

import sys
import time
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))
sys.path.insert(0, str(REPO / "research" / "aspire"))

from _driver import provision_dev_overlay_local  # noqa: E402
from aspire_asv_pipeline import read_metadata  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, Resources, Runtime, Size, Source, TransformInstanceLibrary,
    record_library,
)

MLIB = REPO / "src" / "metasmith_libraries"
SAMPLES = ["ANA1", "MIC1"]
CONTAINER = "docker://quay.io/hallamlab/metasmith:0.23.0-bf27ebf"
TARGETS = ["aspire::asv_filtered_counts", "aspire::asv_filtered_seqs", "aspire::read_counts",
           "aspire::fastp_report_json"]


def build_inputs(work: Path, reads: Path) -> DataInstanceLibrary:
    params = yaml.safe_load((HERE / "params.yml").read_text())
    params["table_filter"]["min_sample_sum"] = 0
    (work / "params.yml").write_text(yaml.safe_dump(params, sort_keys=False))
    sheet = "".join(line for i, line in enumerate((HERE / "study_metadata.tsv").read_text().splitlines(True))
                    if i == 0 or line.split("\t")[0] in SAMPLES)

    lib = DataInstanceLibrary(work / "inputs.xgdb")
    lib.Purge()
    for t in ("aspire.yml", "amplicon.yml", "sequences.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / t)
    study = lib.AddValue("study_metadata.tsv", sheet, "aspire::study_metadata")
    lib.AddItem(work / "params.yml", "aspire::params", parents={study})
    for sid in SAMPLES:
        meta = lib.AddValue(f"{sid}.read_metadata.json", read_metadata(sid, "paired"),
                            "sequences::read_metadata", parents={study})
        pair = lib.AddValue(f"{sid}.read_pair.txt", sid, "sequences::read_pair", parents={meta})
        for mate, dtype in (("R1", "zipped_forward_short_reads"), ("R2", "zipped_reverse_short_reads")):
            lib.AddItem(reads / f"{sid}_{mate}.fastq.gz", f"sequences::{dtype}", parents={pair})
    return record_library(lib)


def main():
    reads = Path(sys.argv[1]).resolve()
    mode = sys.argv[2] if len(sys.argv) > 2 else "plan"
    work = reads.parent / "smoke_work"
    work.mkdir(exist_ok=True)

    home = work / "agent_home"
    smith = Agent(home=Source.FromLocal(home), runtime=Runtime.DOCKER, container=CONTAINER)
    smith.Deploy()
    provision_dev_overlay_local(home)

    inputs = build_inputs(work, reads)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("aspire::study_metadata")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / n) for n in ("env", "lib")],
        transforms=[TransformInstanceLibrary.Load(MLIB / "transforms" / n) for n in ("aspire", "logistics")],
        targets=TARGETS,
    )
    assert task.ok, f"did not solve: {sorted(task.plan.dropped_targets)}"
    print(f"plan: {len(task.plan.steps)} steps, key={task.GetKey()}")
    if mode != "run":
        return

    smith.StageWorkflow(task, on_exist="clear")
    smith.RunWorkflow(
        task=task,
        config_file=smith.GetNxfConfigPresets()["local"],
        params=dict(executor=dict(cpus=16, queueSize=4), process=dict(tries=1)),
        resource_overrides={"*": Resources(memory=Size.GB(8), cpus=4)},
    )
    results = smith.GetResultSource(task).GetPath()
    start = time.time()
    while not (results / "_metadata").exists():
        if time.time() - start > 3600:
            raise TimeoutError("the smoke run did not finish within an hour")
        time.sleep(10)
    smith.CheckWorkflow(task)
    for path, dtype, _ in DataInstanceLibrary.Load(results).Iterate():
        print(f"{dtype}: {results / path}")


if __name__ == "__main__":
    main()
