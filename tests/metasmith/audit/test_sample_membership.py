"""A sample that reaches a step through the same lineage hits, whatever else changed.

Three shapes of "the input set moved":

- the trio at two samples, then at three in the same agent home;
- run 1 is `[A, B, C]`, run 2 is `[X, A, Y]` through a modified plan;
- a batched transform on `[a, b, c]`, then on `[c, d, a, b]`.

Every one of them is read from what the run executed: a `bootstrap_call` in
the virtual runtime's trace is one task, and the members it carried are its
batch bounds.
"""

from __future__ import annotations

import shutil
from collections import Counter
from pathlib import Path

import pytest

from metasmith.telemetry import TraceIndex

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.test_sample_addition import DATABASES, MERGES, PER_SAMPLE
from tests.metasmith.fixtures.trio import TARGETS, build_named_inputs, solve_trio


def _calls(virtual_runtime) -> Counter:
    """Bootstrap calls per step name."""
    return Counter(
        e["step_name"] for e in virtual_runtime.parse_trace() if e.get("type") == "bootstrap_call"
    )


def _widths(virtual_runtime, step_name: str) -> list[int]:
    return [
        e["batch_end"] - e["batch_start"]
        for e in virtual_runtime.parse_trace()
        if e.get("type") == "bootstrap_call" and e["step_name"] == step_name
    ]


def _hits(workspace: Path) -> Counter:
    trace = TraceIndex.read(workspace / "_metasmith" / "trace.jsonl")
    return Counter(ev.step_name for ev in trace.events if ev.status == "hit")


def _assert_calls(calls: Counter, expected: dict[str, int], label: str) -> None:
    wrong = {n: (calls.get(n, 0), want) for n, want in expected.items() if calls.get(n, 0) != want}
    assert not wrong, f"{label}: step -> (executed, expected): {wrong}"


class TestTrioGainsASample:
    def test_only_the_new_sample_and_the_merges_run(
        self, virtual_runtime, tmp_path, metasmith_libraries_root
    ):
        root = metasmith_libraries_root
        at = tmp_path / "inputs"
        capture_run(virtual_runtime, solve_trio(root, build_named_inputs(root, at, ["s00", "s01"])))
        clear_trace(virtual_runtime)
        capture_run(
            virtual_runtime,
            solve_trio(root, build_named_inputs(root, at, ["s00", "s01", "s02"])),
        )
        expected = {n: 1 for n in PER_SAMPLE}
        expected.update({n: 1 for n in MERGES})
        expected.update({n: 0 for n in DATABASES})
        _assert_calls(_calls(virtual_runtime), expected, "trio at 2 then 3")


class TestASampleReturnsInADifferentRun:
    """Run 1: `[A, B, C]`. Run 2: `[X, A, Y]` through a plan with fewer targets.

    Every step of the trio is per sample, the merges included (each folds one
    sample's chunks), so A hits everywhere and X and Y miss everywhere.
    """

    RUN2_TARGETS = [t for t in TARGETS if "interproscan" not in t]

    def _run_1(self, virtual_runtime, root, at):
        capture_run(virtual_runtime, solve_trio(root, build_named_inputs(root, at, ["A", "B", "C"])))
        clear_trace(virtual_runtime)

    def test_a_hits_along_its_whole_chain(self, virtual_runtime, tmp_path, metasmith_libraries_root):
        root = metasmith_libraries_root
        at = tmp_path / "inputs"
        self._run_1(virtual_runtime, root, at)
        second = capture_run(
            virtual_runtime,
            solve_trio(root, build_named_inputs(root, at, ["X", "A", "Y"]), self.RUN2_TARGETS),
        )
        per_sample = [n for n in PER_SAMPLE if n != "interproscan"]
        merges = [n for n in MERGES if n != "merge_interproscan"]
        expected = {n: 2 for n in per_sample + merges}
        expected.update({n: 0 for n in DATABASES if "InterPro" not in n})
        _assert_calls(_calls(virtual_runtime), expected, "[A,B,C] then [X,A,Y]")
        hits = _hits(second.workspace)
        cold = [n for n in per_sample if hits.get(n, 0) != 1]
        assert not cold, f"A did not hit exactly once on: {cold} (hits={dict(hits)})"

    def test_a_modified_transform_misses_from_there_down(
        self, virtual_runtime, tmp_path, metasmith_libraries_root
    ):
        root = metasmith_libraries_root
        at = tmp_path / "inputs"
        self._run_1(virtual_runtime, root, at)

        edited = tmp_path / "functionalAnnotation"
        shutil.copytree(root / "transforms" / "functionalAnnotation", edited)
        kofam = edited / "kofamscan.py"
        body = kofam.read_text(encoding="utf-8")
        marker = "def parse_kofamscan(input_path, output_path):\n"
        assert marker in body
        kofam.write_text(
            body.replace(marker, marker + "    _edited = True  # a real change to the protocol\n"),
            encoding="utf-8",
        )
        capture_run(
            virtual_runtime,
            solve_trio(
                root,
                build_named_inputs(root, at, ["X", "A", "Y"]),
                self.RUN2_TARGETS,
                transform_roots={"functionalAnnotation": edited},
            ),
        )
        expected = {"prodigal": 2, "chunkOrfsForAnnotation": 2, "diamond_uniref50": 2}
        expected.update({"kofamscan": 3, "merge_kofamscan": 3, "merge_diamond_uniref50": 2})
        expected.update({n: 0 for n in DATABASES if "InterPro" not in n})
        _assert_calls(_calls(virtual_runtime), expected, "kofamscan edited")


class TestBatchMembership:
    """`batch_size=3` on `[a, b, c]`, then on `[c, d, a, b]`."""

    @staticmethod
    def _plan(tmp_path: Path, names: list[str]):
        from metasmith.models.libraries import DataInstanceLibrary
        from metasmith.testing import mock_transforms as mt
        from tests.metasmith.flow.conftest import (
            _MOCK_TYPE_PROPERTIES,
            BuiltPlan,
            _build_transform_lib,
            _build_type_lib,
            _generate_plan,
        )

        types_path = _build_type_lib(tmp_path / "types.yml")
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.AddTypeLibrary(types_path, namespace="mock")
        for name in names:
            d = lib.location / name
            d.mkdir(parents=True, exist_ok=True)
            f = d / "assembly.txt"
            if not f.exists():
                f.write_text(f">{name}\nACGT\n", encoding="utf-8")
            lib.AddItem(Path(f"{name}/assembly.txt"), "mock::assembly")
        lib.Save()
        tr_lib = _build_transform_lib(tmp_path / "tr", types_path, mt.batched_transform(batch_size=3))
        plan = _generate_plan(
            lib, tr_lib, sample_dtype="assembly",
            target_props=[_MOCK_TYPE_PROPERTIES["bam"]], target_names=["bam"],
        )
        return BuiltPlan(plan=plan, data_library=lib, transform_libraries=[tr_lib]).as_task()

    def test_a_reshuffled_batch_submits_only_the_new_member(self, virtual_runtime, tmp_path):
        first = capture_run(virtual_runtime, self._plan(tmp_path, ["a", "b", "c"]))
        assert _calls(virtual_runtime)["batched"] == 1
        clear_trace(virtual_runtime)
        second = capture_run(virtual_runtime, self._plan(tmp_path, ["c", "d", "a", "b"]))
        widths = _widths(virtual_runtime, "batched")
        assert widths == [1], (
            f"expected one task carrying only `d`, got batch widths {widths}"
        )
        assert _hits(second.workspace)["batched"] == 3, (
            f"a, b and c were not served from their shards: {dict(_hits(second.workspace))}"
        )
        del first
