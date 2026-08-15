"""Unit test for the ``fabfos.pipelines.annotation`` driver.

Calls ``annotation.generate_workflow`` directly. Only the canonical mapper is
targeted (``annotation.py`` demands ``annotation::gpr_table`` alone --
``gpr_7lane``/``gpr_table_7lane`` is a separate subtype this driver does not
build; see its module docstring), so the expected transform set is the
chosen-4 run tools plus ``gpr_4lane``.

Planning only: an empty ORF FASTA is enough. All FIVE DVC-pinned references in
``data/processed/`` -- KOfam profiles + KO list, the UniRef50 DIAMOND db, the
MNXR lookup, and ``ref::reference_label_pool`` -- are expected to resolve as
REAL defaults on a machine that has run ``dvc checkout``, so no stub should be
staged. (The pool was a standing gap when this test was written; it is now
built from Swiss-Prot 2026_02 -- see ``build_references/REFERENCES.md`` R7.)

Also covers the many-sample path: ``--orfs``/``--orfs-dir`` admit N fastas and
metasmith solves them as one unique case, which is what makes a sharded
Cyanoverse-scale workload need no driver-side loop.

Renders the resolved DAG to ``tests/artifacts/annotation_dag.svg``.

Requirements: an env with ``metasmith`` + graphviz ``dot`` (e.g. ``msm``):

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/test_annotation_driver.py -v

Runs standalone too: ``python tests/test_annotation_driver.py``.
"""
from __future__ import annotations

from pathlib import Path

from metasmith.python_api import Runtime

from fabfos.pipelines import annotation, common

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

# The full lane, with nothing supplied. Three of the four annotators are sharded
# -- kofamscan, diamond_uniref50 and proteinbert take `sequences::orf_chunk` and
# produce `*_chunk` -- so each is bracketed by the chunker and its own merge back
# to the whole-proteome product `gpr_4lane` requires. `clean` takes whole ORFs and
# needs no merge. `tests/fabfos/test_cyanoverse_gpr_reuse.py` is the same lane with
# the kofam product supplied, which is what removes that annotator and its merge.
EXPECTED_TRANSFORMS = {
    "chunkOrfsForAnnotation",
    "kofamscan", "clean", "diamond_uniref50", "proteinbert",
    "merge_kofamscan", "merge_diamond_uniref50", "merge_proteinbert",
    "gpr_4lane",
}


def _given_orf_count(task) -> int:
    """How many ORF fastas the plan was actually given.

    ``WorkflowPlan`` has no ``samples``: it models the one *unique case*, and
    the per-sample fan-out shows up as one ``sequences::orfs`` instance per
    input in ``plan.given``.
    """
    return sum(1 for i in task.plan.given if i.dtype_name == "sequences::orfs")


def _plan(work: Path):
    orfs = work / "orfs.faa"
    orfs.touch()
    return annotation.generate_workflow(
        work, orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
        uniref50_db=None, mnxr_lookup=None, label_pool=None, runtime=Runtime.APPTAINER,
    )


def test_annotation_driver_plans(tmp_path):
    """``annotation.generate_workflow`` resolves and wires in the canonical 4-lane mapper."""
    agent, task, stubs = _plan(tmp_path)

    assert task.ok, f"annotation driver failed to plan: {task.plan}"

    names = common.step_transform_names(task)
    missing = EXPECTED_TRANSFORMS - names
    assert not missing, (
        f"annotation plan is missing expected transform(s): {sorted(missing)}; "
        f"plan used: {sorted(names)}"
    )

    # all five references have a real pinned default and should NOT fall back
    # to a stub on a machine that has run `dvc checkout`.
    assert not stubs, stubs

    svg = common.render_dag(task, ARTIFACTS / "annotation_dag")
    assert svg.exists() and svg.stat().st_size > 0, f"DAG SVG not written: {svg}"


def test_annotation_driver_plans_many_samples(tmp_path):
    """N ORF fastas plan as N samples over the SAME 5 transforms, no extra steps.

    The sharded Cyanoverse workload rides on this: metasmith fans out over
    samples itself, so the driver never grows a loop. If this ever collapses
    to one sample, the shards would be silently annotated as a single pool.
    """
    orfs = []
    for i in range(3):
        f = tmp_path / f"shard_{i}.faa"
        f.touch()
        orfs.append(f)

    agent, task, stubs = annotation.generate_workflow(
        tmp_path, orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
        uniref50_db=None, mnxr_lookup=None, label_pool=None, runtime=Runtime.APPTAINER,
    )

    assert task.ok, f"annotation driver failed to plan {len(orfs)} samples: {task.plan}"
    assert common.step_transform_names(task) == EXPECTED_TRANSFORMS
    assert not stubs, stubs

    # one given ORF instance per fasta -- the fan-out, not a single pooled case
    assert _given_orf_count(task) == len(orfs), (
        f"expected {len(orfs)} ORF samples, plan was given {_given_orf_count(task)}"
    )


def test_annotation_driver_accepts_a_bare_path(tmp_path):
    """A single Path still works unwrapped, so existing callers are unchanged."""
    orfs = tmp_path / "orfs.faa"
    orfs.touch()
    _, task, _ = annotation.generate_workflow(
        tmp_path, orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
        uniref50_db=None, mnxr_lookup=None, label_pool=None, runtime=Runtime.APPTAINER,
    )
    assert task.ok
    assert _given_orf_count(task) == 1


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        agent, task, stubs = _plan(Path(td))
        if not task.ok:
            raise SystemExit(f"FAILED to plan: {task.plan}")
        common.print_plan(task)
        common.report_stubs("annotation", stubs)
        svg = common.render_dag(task, ARTIFACTS / "annotation_dag")
        print(f"DAG written to: {svg}")
