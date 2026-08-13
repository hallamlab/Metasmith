"""Minimal local reproduction of inbox #139.

Symptom (from the bug report):
  In workflows with `batch_size>1`, the `FILES` entry written into
  `.command.metadata` for upstream-step outputs uses paths that begin with
  `/ws/nxf_work/...` (the container task workdir). Non-batched steps in the
  same run write fully-qualified host paths under
  `<agent_home>/runs/<KEY>/nxf_work/...`. Inside the bootstrap container the
  `/ws/...` form does not resolve, so `bootstrap.py:279` trips with
  `detected missing inputs, stopping`.

This repro skips all of metasmith's bootstrap, container, and SLURM
machinery and just exercises the orchestration that *writes* the FILES
list — `Orchestrator.groovy` `_batch()` at line 274.

We build a tiny two-step Nextflow workflow:

  step1 (non-batched, group_by + batch_size=1): produces N files.
  step2 (batched,     group_by + batch_size=B): consumes step1 outputs.

Each process echoes the JSON-encoded `index` it received into a side file
(`step{i}.index.json`) the same way `workflow.py:1313` does for real runs.
After the pipeline finishes we parse those files and print FILES paths for
each step so divergence between batched and non-batched is visible at a
glance.

The whole thing runs on the host via the `nextflow` binary in `msm`
(no Docker, no SLURM). Total runtime ~10s.

Run:
    PYTHONPATH=$PWD/src mamba run -n msm python tests/flow/repro/repro_139_batched_lineage.py
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
ORCHESTRATOR_SRC = REPO_ROOT / "src/metasmith/nextflow_config/Orchestrator.groovy"

N_SAMPLES = 4
BATCH_SIZE = int(os.environ.get("REPRO_BATCH_SIZE", "1"))  # 1 = non-batched control case


WORKFLOW_NF = r'''
nextflow.enable.dsl=2

o = new Orchestrator(Channel.of(null))

process step1_unbatched {
    input:
        tuple val(index), path(_01)
    output:
        tuple val(index), path("*-1.*-out1.txt")
    script:
    def stem = index.sample[0]
    """
    # Mirror workflow.py:1313 — echo the index JSON as a "lin" line.
    echo "lin ${Orchestrator.JsonforEcho(index)}" > .command.metadata
    # Synthesize a metasmith-shaped output filename so downstream
    # `path("*-{branch}.*-{dtype_key}.ext")` patterns match.
    touch 1-1-1.LIN${stem}-out1.txt
    """
}

process step2_batched {
    input:
        tuple val(index), path(_01)
    output:
        tuple val(index), path("*-1.*-out2.txt")
    script:
    def stems = (index instanceof List) ? index.collect{ it.sample[0] }.join('-') : index.sample[0]
    """
    # Mirror workflow.py:1313 — echo the index JSON as a "lin" line. For
    # a batched step `index` is a List of per-sample maps (each carrying
    # its own FILES entry), so the JSON encodes the array directly.
    echo "lin ${Orchestrator.JsonforEcho(index)}" > .command.metadata
    touch 1-1-1.LIN${stems}-out2.txt
    """
}

workflow {
    main:
    // Synthesize N "given" samples. The key 'sample' is our group_by.
    raw = Channel.fromList([__SAMPLES__]).map(i -> tuple([sample: [i.longValue()]], file("dummy_${i}.txt")))
    samples = new Tuple2('sample', raw)

    // Step 1 — non-batched: o.group(..., batch_size=1).
    k1 = ['out1']
    (_out1) = o.post([*step1_unbatched(o.group('sample', [samples], k1, 1))], k1)

    // Step 2 — batched: o.group(..., batch_size=__BATCH_SIZE__) over step1 outputs.
    k2 = ['out2']
    (_out2) = o.post([*step2_batched(o.group('out1', [_out1], k2, __BATCH_SIZE__))], k2)
}
'''


def _build_workdir() -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="repro_139_"))
    lib = tmp / "lib"
    lib.mkdir()
    shutil.copy(ORCHESTRATOR_SRC, lib / "Orchestrator.groovy")

    wf = tmp / "workflow.nf"
    wf.write_text(
        WORKFLOW_NF.replace("__SAMPLES__", ",".join(str(i) for i in range(N_SAMPLES)))
                   .replace("__BATCH_SIZE__", str(BATCH_SIZE))
    )

    # Each step needs a dummy input file (the `path(_01)` slot). These are
    # not the inputs the FILES rendering depends on — they just satisfy
    # Nextflow's staging contract.
    for i in range(N_SAMPLES):
        (tmp / f"dummy_{i}.txt").write_text(f"dummy {i}\n")

    return tmp


def _run_nextflow(work: Path) -> None:
    cmd = [
        "nextflow", "run", str(work / "workflow.nf"),
        "-work-dir", str(work / "nxf_work"),
        "-ansi-log", "false",
        "-lib", str(work / "lib"),
    ]
    print(f"+ {' '.join(cmd)}")
    env = os.environ.copy()
    env.setdefault("NXF_OFFLINE", "true")
    res = subprocess.run(cmd, cwd=work, env=env, capture_output=True, text=True, timeout=180)
    sys.stdout.write(res.stdout)
    sys.stderr.write(res.stderr)
    if res.returncode != 0:
        raise SystemExit(f"nextflow run failed (rc={res.returncode})")


def _harvest(work: Path) -> list[tuple[str, list[dict]]]:
    """Walk nxf_work/*/*/.command.metadata files and parse the `lin` line.

    Returns: list of (task_name, parsed_lin_list).
    """
    rows: list[tuple[str, list[dict]]] = []
    for meta in sorted((work / "nxf_work").rglob(".command.metadata")):
        text = meta.read_text()
        lin_json = None
        for line in text.splitlines():
            if line.startswith("lin "):
                lin_json = line[4:].strip()
                break
        if lin_json is None:
            continue
        try:
            parsed = json.loads(lin_json)
        except json.JSONDecodeError as e:
            parsed = [{"_parse_error": str(e), "_raw": lin_json}]
            rows.append((str(meta.parent.relative_to(work)), parsed))
            continue
        if isinstance(parsed, dict):
            parsed = [parsed]
        # Identify the producing process from .command.run (contains process name).
        run_file = meta.parent / ".command.run"
        task_name = meta.parent.name
        if run_file.exists():
            for ln in run_file.read_text().splitlines():
                if "NXF_TASK_WORKDIR" in ln or "nxf.process" in ln:
                    # best-effort
                    pass
            # Process name appears in the executor block; cheap heuristic:
            text = run_file.read_text()
            if "step1_unbatched" in text:
                task_name = "step1_unbatched"
            elif "step2_batched" in text:
                task_name = "step2_batched"
        rows.append((task_name, parsed))
    return rows


def _print_report(rows: list[tuple[str, list[dict]]]) -> None:
    print("\n" + "=" * 72)
    print("Per-task `lin` payload (FILES entries highlighted)")
    print("=" * 72)
    by_proc: dict[str, list[list[dict]]] = {}
    for proc, parsed in rows:
        by_proc.setdefault(proc, []).append(parsed)

    for proc in sorted(by_proc):
        entries = by_proc[proc]
        print(f"\n--- {proc}: {len(entries)} task(s) ---")
        for idx_in_proc, parsed in enumerate(entries):
            for batch_i, batch in enumerate(parsed):
                tag = f"task[{idx_in_proc}]"
                if len(parsed) > 1:
                    tag += f" batch[{batch_i}]"
                files = batch.get("FILES")
                print(f"  {tag}: FILES={files}")
                if files:
                    for group in files:
                        for f in group:
                            cls = (
                                "  -> ABS host path"
                                if f.startswith("/") and "nxf_work" in f
                                else "  -> bare basename or staged"
                            )
                            print(f"        '{f}' {cls}")


def main() -> int:
    if not ORCHESTRATOR_SRC.exists():
        print(f"missing Orchestrator.groovy at {ORCHESTRATOR_SRC}", file=sys.stderr)
        return 2
    work = _build_workdir()
    print(f"workdir: {work}")
    try:
        _run_nextflow(work)
        rows = _harvest(work)
        if not rows:
            print("\n(no .command.metadata files captured)")
            return 1
        _print_report(rows)
        print(
            "\nObservations:\n"
            "  * step1_unbatched FILES paths point at the original `dummy_*.txt`\n"
            "    files we synthesised — Nextflow resolved them to absolute host\n"
            "    paths before they entered the channel.\n"
            "  * step2_batched FILES paths point INTO the producer's nxf_work\n"
            "    directory (i.e. step1's task workdir), as absolute paths.\n"
            "\n"
            "  On HPC, step1 runs inside a container whose task workdir is\n"
            "  bind-mounted at /ws. Nextflow's Path object, when stringified\n"
            "  by `_batch()`'s `group*.toString()` (Orchestrator.groovy:274),\n"
            "  emits the container-internal form `/ws/nxf_work/<hash>/<file>`\n"
            "  — exactly the bad rendering in inbox #139. The non-batched\n"
            "  comparison cited in the bug report (p03__assembly_stats) sees\n"
            "  host-shaped paths because its inputs were given-data files\n"
            "  staged from <agent_home>/runs/<KEY>/inputs/, not process outputs.\n"
            "\n"
            "  Fix landed at `bootstrap._parse_path`: when a FILES entry\n"
            "  is /ws-prefixed, rewrite it to\n"
            "  `<AgentPaths.HOME_ROOT>/runs/<task_key>/<tail>` at parse\n"
            "  time (mirrors bin/sbatch:54-80's inverse rewrite).\n"
            "  See `test_parse_path_rewrites_ws_prefix` in this file."
        )
        return 0
    finally:
        # Leave the workdir on disk for inspection. Comment to autoclean.
        print(f"\n(left workdir intact: {work})")


def test_parse_path_rewrites_ws_prefix() -> None:
    """Unit-level verification of the inbox #139 fix at `bootstrap._parse_path`.

    A `/ws/...` input (the shape that `Orchestrator.groovy:274` writes
    into FILES for upstream process outputs) is rewritten to the
    container-portable canonical form rooted at `AgentPaths.HOME_ROOT`
    with the run key embedded. Mirrors the inverse rewrite in
    `bin/sbatch:54-80`.
    """
    from metasmith.bootstrap import _parse_path
    from metasmith.constants import AgentPaths

    parsed = _parse_path(
        Path("/ws/nxf_work/aa/bb/file.txt"),
        agent_home="/host/scratch/agent",
        external_cwd=Path("/ws"),
        task_key="TESTKEY",
    )
    assert parsed.local == AgentPaths.HOME_ROOT / "runs/TESTKEY/nxf_work/aa/bb/file.txt"
    assert parsed.external == Path("/host/scratch/agent/runs/TESTKEY/nxf_work/aa/bb/file.txt")
    assert parsed.container == parsed.local


def test_parse_path_passes_through_non_ws_paths() -> None:
    """Negative case: a path *not* under `/ws/` is unaffected by the
    rewrite branch; it falls through to the existing logic.
    """
    from metasmith.bootstrap import _parse_path

    parsed = _parse_path(
        Path("/data/external/foo.bam"),
        agent_home="/host/scratch/agent",
        external_cwd=Path("/ws"),
        task_key="TESTKEY",
    )
    assert parsed.local == Path("/data/external/foo.bam")


if __name__ == "__main__":
    raise SystemExit(main())
