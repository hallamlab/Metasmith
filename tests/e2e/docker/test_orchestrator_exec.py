"""Orchestrator.groovy edge case tests via real Nextflow execution.

These tests run minimal Nextflow scripts that exercise the Orchestrator
class's group, batch, debatch, post, and mix methods inside Docker.
"""

import json
import subprocess
import shutil
import sys
import pytest
from pathlib import Path

from metasmith.constants import MODULE_PATH

pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]

ORCHESTRATOR_SRC = MODULE_PATH / "nextflow_config/Orchestrator.groovy"


class NxfTestRunner:
    """Helper to run minimal Nextflow scripts testing Orchestrator.groovy."""

    def __init__(self, work_dir: Path, docker_image: str):
        self.work_dir = work_dir
        self.docker_image = docker_image
        self.work_dir.mkdir(parents=True, exist_ok=True)

        # Set up lib/ with Orchestrator.groovy
        lib_dir = self.work_dir / "lib"
        lib_dir.mkdir(exist_ok=True)
        shutil.copy(ORCHESTRATOR_SRC, lib_dir / "Orchestrator.groovy")

    def run(self, nxf_script: str, timeout: int = 120) -> subprocess.CompletedProcess:
        """Run a Nextflow script inside Docker.

        Args:
            nxf_script: Nextflow script content.
            timeout: Timeout in seconds.

        Returns:
            CompletedProcess with stdout/stderr/returncode.
        """
        script_path = self.work_dir / "test.nf"
        script_path.write_text(nxf_script)

        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{self.work_dir}:/ws",
                "-w", "/ws",
                self.docker_image,
                "nextflow", "run", "test.nf",
                "-lib", "./lib",
                "-ansi-log", "false",
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result

    @staticmethod
    def assert_nxf_ok(result: subprocess.CompletedProcess):
        """Assert Nextflow succeeded, tolerating upstream bug nextflow-io/nextflow#6757.

        Under wall-clock skew (WSL2, NTP step), Nextflow's `WorkflowMetadata.invokeOnComplete`
        asserts `Duration >= 0` and throws even after the workflow body has completed
        successfully and all `publish` manifests have been written. The exit code is
        non-zero but the on-disk results are intact and parseable.
        """
        nxf_duration_bug = (
            "Duration unit cannot be a negative number" in result.stdout
            or "Duration unit cannot be a negative number" in (result.stderr or "")
        )
        if result.returncode != 0 and nxf_duration_bug:
            print(
                "WARN: tolerated upstream nextflow-io/nextflow#6757 (negative Duration "
                "assertion); workflow body completed, optional report/timeline/trace "
                "artifacts may be missing.",
                file=sys.stderr,
            )
            return
        assert result.returncode == 0, f"NXF failed: {result.stderr}"


@pytest.fixture
def nxf_runner(tmp_path, docker_image):
    """Create an NxfTestRunner for the test."""
    return NxfTestRunner(tmp_path / "nxf_test", docker_image)


class TestOrchestratorPost:
    """Test post/postIn hash and index behavior."""

    def test_post_produces_output(self, nxf_runner):
        """post() processes items and produces indexed output."""
        result = nxf_runner.run('''


workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [["a": [1]], file("${projectDir}/test.nf")],
        [["a": [2]], file("${projectDir}/lib/Orchestrator.groovy")],
    ])

    def out = (o.post([ch], ["result"]))[0]
    def (name, stream) = out
    stream.view { idx, item -> "POST: ${groovy.json.JsonOutput.toJson(idx)} ${item.name}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        assert "POST:" in result.stdout

    def test_postin_processes_inputs(self, nxf_runner):
        """postIn() uses full path hash (not just filename hash)."""
        # Write test input files
        for i in range(3):
            (nxf_runner.work_dir / f"input_{i}.txt").write_text(f"data {i}")

        result = nxf_runner.run('''


workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [[:], file("${projectDir}/input_0.txt")],
        [[:], file("${projectDir}/input_1.txt")],
        [[:], file("${projectDir}/input_2.txt")],
    ])

    def out = (o.postIn([ch], ["inp"]))[0]
    def (name, stream) = out
    stream.view { idx, item -> "POSTIN: ${groovy.json.JsonOutput.toJson(idx)} ${item.name}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("POSTIN:")]
        assert len(lines) == 3

        # Each should have an "inp" key in the index
        for line in lines:
            idx_str = line.split(" ", 1)[1].split(" ")[0]
            idx = json.loads(idx_str)
            assert "inp" in idx

    def test_post_id_is_md5_composite(self, nxf_runner):
        """post() id is the md5 of "<slot_id>::<filename>" (a hex String).

        This is the on-channel form of the canonical file_instance_id
        (LinPayload.mint_file_id). With no slot_ids supplied the channel
        name stands in for the slot_id, so the id is md5("x::test.nf").
        """
        import hashlib

        result = nxf_runner.run('''


workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [[:], file("${projectDir}/test.nf")],
    ])

    def out = (o.post([ch], ["x"]))[0]
    def (name, stream) = out
    stream.view { idx, item ->
        def hash_val = idx["x"][0]
        "HASH: ${hash_val} type=${hash_val.getClass().name}"
    }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("HASH:")]
        assert len(lines) == 1
        # It is a String, not a Long, now.
        assert "String" in lines[0]
        # And it is exactly md5("x::test.nf") — the mint_file_id composite.
        expected = hashlib.md5(b"x::test.nf").hexdigest()
        assert expected in lines[0], f"expected {expected} in {lines[0]}"

    def test_post_id_uses_slot_id_when_supplied(self, nxf_runner):
        """When slot_ids is passed, the id is md5("<slot_id>::<filename>")."""
        import hashlib

        result = nxf_runner.run('''


workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [[:], file("${projectDir}/test.nf")],
    ])

    def out = (o.post([ch], ["x"], ["deadbeef"]))[0]
    def (name, stream) = out
    stream.view { idx, item -> "HASH: ${idx["x"][0]}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("HASH:")]
        assert len(lines) == 1
        expected = hashlib.md5(b"deadbeef::test.nf").hexdigest()
        assert expected in lines[0], f"expected {expected} in {lines[0]}"


class TestOrchestratorGroup:
    """Test group behavior for combining streams."""

    def test_group_single_stream(self, nxf_runner):
        """Items grouped by key via postIn -> group flow."""
        # Write test files
        for i in range(4):
            (nxf_runner.work_dir / f"item_{i}.txt").write_text(f"item {i}")

        # Use postIn to register index history (the public API),
        # then group the resulting streams
        result = nxf_runner.run('''


workflow {
    o = new Orchestrator(Channel.fromList([null]))

    // Use postIn to register items (sets up index_history)
    ch_a_raw = Channel.fromList([
        [[:], file("${projectDir}/item_0.txt")],
        [[:], file("${projectDir}/item_1.txt")],
    ])
    ch_b_raw = Channel.fromList([
        [[:], file("${projectDir}/item_2.txt")],
        [[:], file("${projectDir}/item_3.txt")],
    ])

    def posted_a = (o.postIn([ch_a_raw], ["a"]))[0]
    def posted_b = (o.postIn([ch_b_raw], ["b"]))[0]

    def grouped = o.group("a", [posted_a, posted_b], ["target"], 1)
    grouped.view { "GROUP: ${it[0]}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("GROUP:")]
        assert len(lines) >= 1

    def test_group_does_not_split_single_key_across_mixed_streams(self, nxf_runner):
        """Mixed emissions for one key should form one complete group, not partials."""
        (nxf_runner.work_dir / "a.txt").write_text("a")
        for i in range(9):
            (nxf_runner.work_dir / f"b_{i}.txt").write_text(f"b {i}")

        result = nxf_runner.run('''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch_a_raw = Channel.fromList([
        [[:], file("${projectDir}/a.txt")],
    ])

    ch_b_raw_early = Channel.fromList([
        [[:], file("${projectDir}/b_0.txt")],
        [[:], file("${projectDir}/b_1.txt")],
        [[:], file("${projectDir}/b_2.txt")],
    ])

    ch_b_raw_late = Channel.fromList([
        [[:], file("${projectDir}/b_3.txt")],
        [[:], file("${projectDir}/b_4.txt")],
        [[:], file("${projectDir}/b_5.txt")],
        [[:], file("${projectDir}/b_6.txt")],
        [[:], file("${projectDir}/b_7.txt")],
        [[:], file("${projectDir}/b_8.txt")],
    ]).map { x ->
        sleep 100
        return x
    }

    def posted_a = (o.postIn([ch_a_raw], ["a"]))[0]
    def posted_b_early = (o.postIn([ch_b_raw_early], ["b"]))[0]
    def posted_b_late = (o.postIn([ch_b_raw_late], ["b"]))[0]
    def mixed_b = o.mix([posted_b_early, posted_b_late])

    def grouped = o.group("a", [posted_a, mixed_b], ["target"], 1)
    grouped.view { indexes, a_vals, b_vals ->
        "GROUP_MIX: idx=${indexes.size()} a=${a_vals.size()} b=${b_vals.size()}"
    }
}
''', timeout=180)
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("GROUP_MIX:")]
        assert len(lines) == 1, f"Expected 1 grouped emission, got {len(lines)}: {lines}"
        assert "idx=1" in lines[0]
        assert "a=1" in lines[0]
        assert "b=9" in lines[0]


    # --- group_by collection contract -------------------------------------
    #
    # `group_by` partitions the incoming streams by the grouping dependency's
    # instances. Each key yields ONE task member holding ALL the items matched
    # to it; `batch_size` folds N whole keys into one task and never shards
    # within a key. Four other parts of the system already encode this —
    # `checkm`/`gtdbtk` pairing `group_by=asm` with `batch_size=25`/`100`,
    # `plan_oracle` predicting `ceil(len(group_by_instances) / batch_size)`
    # tasks, and `cache_decisions` + `virtual_runtime` both chunking
    # `group_by_instances` by `batch_size`.
    #
    # `bbbb599` (2026-05-30) replaced the accumulating branch with per-relation
    # streaming dispatch that emits one result per *descendant item*, then
    # re-collected with `groupTuple(by: 0, size: batch_size, remainder: true)`.
    # Because `batch_size` is the group-COUNT axis, the bag closes after one
    # item on the default — so a collecting transform receives a fraction of
    # its input and no error is raised. Bisected against the same scripts:
    #
    #   one key / 3 descendants / bs=1   release + bbbb599^ -> 1 task x 3 files
    #                                    bbbb599 + HEAD     -> 3 tasks x 1 file
    #   two keys / 2 each   / bs=1       release            -> 2 tasks x 2 files
    #                                    HEAD               -> 4 tasks x 1 file
    #   two keys / 2 each   / bs=2       release            -> 1 task, 2 members
    #                                                          x 2 files each
    #                                    HEAD               -> 2 tasks, 2 members
    #                                                          x 1 file each
    #
    # Fixed by aggregating per by-key inside the dispatch branches, upstream of
    # the cartesian fold, so `_batch` only ever collates whole groups.

    @staticmethod
    def _collection_case(
        nxf_runner: "NxfTestRunner",
        n_keys: int,
        n_outs: int,
        batch_size: int,
    ) -> list[list[list[str]]]:
        """Run the fan-out-then-collect topology and report what each task got.

        `step1` runs once per seed (the fan-out) and emits `n_outs` files.
        The second `o.group` collects those outputs back by the SAME seed key,
        which is the ppanggolin shape: one pangenome entry parenting N
        accessions.

        Returns one entry per emitted task: the list of per-batch-member
        out1 basenames. So `[[["a", "b"]]]` is one task, one member, two
        files.
        """
        for i in range(n_keys):
            (nxf_runner.work_dir / f"seed_{i}.txt").write_text(f"seed {i}\n")

        seeds = ",\n        ".join(
            f'[[:], file("${{projectDir}}/seed_{i}.txt")]' for i in range(n_keys)
        )
        # metasmith output names are `<batch>-<i>-<branch>.<hash>-<key><ext>`;
        # `_debatch` routes on the leading batch index, so every file emitted
        # by one (unbatched) task must share the `1-` prefix.
        touches = " ".join(f"1-${{stem}}{chr(ord('a') + j)}-out1.txt" for j in range(n_outs))

        result = nxf_runner.run(f'''
process step1 {{
    input:
        tuple val(index), path(_01)
    output:
        tuple val(index), path("*-out1.txt")
    script:
    def stem = index[0].seed[0]
    """
    touch {touches}
    """
}}

workflow {{
    o = new Orchestrator(Channel.fromList([null]))

    // Two independent postIn calls over the same files: Nextflow channels are
    // single-consumer, and the real generator forks shared streams with
    // multiMap. postIn's fallback id is md5 of the path, so both copies carry
    // identical seed hashes.
    def seed_fanout = (o.postIn([Channel.fromList([
        {seeds},
    ])], ["seed"]))[0]
    def seed_collect = (o.postIn([Channel.fromList([
        {seeds},
    ])], ["seed"]))[0]

    def k1 = ["out1"]
    def _out1 = (o.post(o.asStreams(step1(o.group("seed", [seed_fanout], k1, 1))), k1))[0]

    def collected = o.group("seed", [seed_collect, _out1], ["out2"], {batch_size})
    collected.view {{ indexes, seed_vals, out1_vals ->
        // FILES is written per batch member by `_collateBatch`, positionally
        // per stream in the order they were passed to group(): [seed, out1].
        def per_member = indexes.collect {{ m -> m.FILES[1].collect {{ p -> p.split("/")[-1] }} }}
        "TASK: " + groovy.json.JsonOutput.toJson(per_member)
    }}
}}
''', timeout=180)
        NxfTestRunner.assert_nxf_ok(result)
        return [
            json.loads(l.split("TASK: ", 1)[1])
            for l in result.stdout.split("\n")
            if l.startswith("TASK:")
        ]

    def test_one_key_collects_all_its_descendants(self, nxf_runner):
        """One grouping instance + 3 descendants + batch_size=1 -> ONE task.

        The ppanggolin regression: the run handed each of two accessions to
        its own task, so ppanggolin clustered a single genome and died in
        scipy with "empty distance matrix". `release` and `bbbb599^` emit one
        task holding both; `bbbb599` onward shatters it.
        """
        tasks = self._collection_case(nxf_runner, n_keys=1, n_outs=3, batch_size=1)
        assert len(tasks) == 1, (
            f"expected the whole group in one task, got {len(tasks)} tasks: {tasks}"
        )
        assert len(tasks[0]) == 1, f"batch_size=1 means one member per task: {tasks}"
        assert len(tasks[0][0]) == 3, (
            f"the single member must carry all 3 descendants, got {tasks[0][0]}"
        )

    def test_each_key_collects_only_its_own_descendants(self, nxf_runner):
        """Two grouping instances -> two tasks, each complete and disjoint.

        Guards the other half of the contract: collecting must not merge
        across keys either.
        """
        tasks = self._collection_case(nxf_runner, n_keys=2, n_outs=2, batch_size=1)
        assert len(tasks) == 2, f"expected one task per key, got {len(tasks)}: {tasks}"
        groups = []
        for t in tasks:
            assert len(t) == 1, f"batch_size=1 means one member per task: {tasks}"
            assert len(t[0]) == 2, f"each key must collect both its files: {tasks}"
            groups.append(set(t[0]))
        assert groups[0].isdisjoint(groups[1]), (
            f"keys leaked descendants into each other: {groups}"
        )

    def test_batch_size_folds_whole_keys_never_shards_one(self, nxf_runner):
        """batch_size counts GROUPS, not members within a group.

        Two keys of two descendants at batch_size=2 is one task with two
        members, each holding its own pair — the shape `checkm` relies on
        (`group_by=asm, batch_size=25` iterating `context.AsBatch()`), and the
        shape `plan_oracle` predicts with `ceil(n_keys / batch_size)`.
        """
        tasks = self._collection_case(nxf_runner, n_keys=2, n_outs=2, batch_size=2)
        assert len(tasks) == 1, (
            f"ceil(2 keys / batch_size 2) == 1 task, got {len(tasks)}: {tasks}"
        )
        assert len(tasks[0]) == 2, f"expected 2 batch members, got {tasks[0]}"
        for member in tasks[0]:
            assert len(member) == 2, f"each member keeps its whole group: {tasks[0]}"

    def test_channel_reuse_across_group_calls(self, nxf_runner):
        """Two group() calls sharing a posted stream — second gets empty channel.

        Reproduces the core deadlock mechanism: Nextflow channels are
        single-consumer, so the first group() drains the stream and the
        second group() receives nothing.
        """
        (nxf_runner.work_dir / "a.txt").write_text("a")

        result = nxf_runner.run('''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [[:], file("${projectDir}/a.txt")],
    ])

    def posted = (o.postIn([ch], ["x"]))[0]

    // Pass the same posted stream to two group() calls
    def g1 = o.group("x", [posted], ["t1"], 1)
    def g2 = o.group("x", [posted], ["t2"], 1)

    g1.view { "G1: ${it[0]}" }
    g2.view { "G2: ${it[0]}" }
}
''', timeout=60)
        NxfTestRunner.assert_nxf_ok(result)
        g1_lines = [l for l in result.stdout.split("\n") if l.startswith("G1:")]
        g2_lines = [l for l in result.stdout.split("\n") if l.startswith("G2:")]
        assert len(g1_lines) >= 1, "G1 should have output"
        assert len(g2_lines) >= 1, "G2 should have output (fails if channel was consumed by G1)"

    def test_stream_reuse_works_in_orchestrator(self, nxf_runner):
        """Shared streams passed to multiple group() calls produce correct output.

        Reproduces the mHAnaWSi deadlock topology. Without explicit multiMap forking
        in the generated workflow, passing the same posted stream to multiple group()
        calls causes a ConcurrentModificationException because the Orchestrator's
        internal channel tracking structures are mutated concurrently.

        This test validates that the workflow generator's multiMap machinery correctly
        forks shared streams so each group() call receives an independent copy.

        Three stub processes:
        - p01: per-sample (9 items), groups by "sample"; also needs exp + container
        - p02: per-experiment (1 item), groups by "exp"; needs container
        - p03: groups by "exp"; needs p01 + p02 outputs + exp + container

        posted_exp and posted_container are referenced by all three group() calls.
        """
        for i in range(9):
            (nxf_runner.work_dir / f"sample_{i}.txt").write_text(f"sample {i}")
        (nxf_runner.work_dir / "exp.txt").write_text("experiment")
        (nxf_runner.work_dir / "container.txt").write_text("container")

        result = nxf_runner.run('''
process p01_per_sample {
    input:
        tuple val(index), path("input.txt"), path("exp.txt"), path("container.txt")
    output:
        tuple val(index), path("1-out.txt")
    script:
    """
    echo "p01 done" > 1-out.txt
    """
}

process p02_per_exp {
    input:
        tuple val(index), path("exp.txt"), path("container.txt")
    output:
        tuple val(index), path("1-out.txt")
    script:
    """
    echo "p02 done" > 1-out.txt
    """
}

process p03_merge {
    input:
        tuple val(index), path("p01_results"), path("p02_result"), path("exp.txt"), path("container.txt")
    output:
        tuple val(index), path("1-out.txt")
    script:
    """
    echo "p03 done" > 1-out.txt
    """
}

workflow {
    o = new Orchestrator(Channel.fromList([null]))

    // Create 9 sample streams
    sample_items = []
    (0..8).each { i ->
        sample_items.add([[:], file("${projectDir}/sample_${i}.txt")])
    }
    ch_samples = Channel.fromList(sample_items)

    // Shared streams: exp and container
    ch_exp = Channel.fromList([[[:], file("${projectDir}/exp.txt")]])
    ch_container = Channel.fromList([[[:], file("${projectDir}/container.txt")]])

    // Post all inputs
    def posted_samples = (o.postIn([ch_samples], ["sample"]))[0]
    def posted_exp = (o.postIn([ch_exp], ["exp"]))[0]
    def posted_container = (o.postIn([ch_container], ["container"]))[0]

    // p01: groups by sample, also needs exp + container
    def g1 = o.group("sample", [posted_samples, posted_exp, posted_container], ["p01_out"], 1)
    def p01_result = p01_per_sample(g1)
    def p01_posted = (o.post([p01_result], ["p01_out"]))[0]

    // p02: groups by exp, needs container
    // posted_exp and posted_container are reused — shared across g1, g2, g3
    def g2 = o.group("exp", [posted_exp, posted_container], ["p02_out"], 1)
    def p02_result = p02_per_exp(g2)
    def p02_posted = (o.post([p02_result], ["p02_out"]))[0]

    // p03: groups by exp, needs p01 + p02 outputs + exp + container
    def g3 = o.group("exp", [p01_posted, p02_posted, posted_exp, posted_container], ["p03_out"], 1)
    def p03_result = p03_merge(g3)

    // Render the file name (a String) rather than the index Map. Iterating
    // an index Map via Groovy's FormatHelper races with concurrent operators
    // sharing the same Map reference (verified empirically against 25.10.0
    // and 26.04.1) and surfaces as a `ConcurrentModificationException` from
    // `FormatHelper.formatMap` inside the view closure. Production-generated
    // workflows don't render index Maps via view, so this is test-side only.
    p01_result.view { "P01: ${it[1].name}" }
    p02_result.view { "P02: ${it[1].name}" }
    p03_result.view { "P03: ${it[1].name}" }
}
''', timeout=120)
        # DSL2 auto-forks shared channels so all three processes receive data correctly.
        p03_lines = [l for l in result.stdout.split("\n") if l.startswith("P03:")]
        nxf_ok = result.returncode == 0 or (
            "Duration unit cannot be a negative number" in result.stdout
        )
        assert nxf_ok and len(p03_lines) >= 1, (
            f"Workflow failed or p03 got no output (got {len(p03_lines)} lines, "
            f"rc={result.returncode}).\n"
            f"stdout:\n{result.stdout[-500:]}"
        )


class TestOrchestratorBatch:
    """Test batch/debatch operations."""

    def test_batch_collates_correctly(self, nxf_runner):
        """_batch(3, channel) groups into batches of 3."""
        for i in range(6):
            (nxf_runner.work_dir / f"b_{i}.txt").write_text(f"batch {i}")

        result = nxf_runner.run('''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [["k": [1]], file("${projectDir}/b_0.txt")],
        [["k": [2]], file("${projectDir}/b_1.txt")],
        [["k": [3]], file("${projectDir}/b_2.txt")],
        [["k": [4]], file("${projectDir}/b_3.txt")],
        [["k": [5]], file("${projectDir}/b_4.txt")],
        [["k": [6]], file("${projectDir}/b_5.txt")],
    ])

    def batched = o._batch(3, ch)
    batched.view { "BATCH: indexes=${it[0].size()} files=${it[1].size()}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("BATCH:")]
        assert len(lines) == 2  # 6 items / 3 per batch = 2 batches

    def test_batch_adds_files_key(self, nxf_runner):
        """Batch adds 'FILES' key to index."""
        for i in range(3):
            (nxf_runner.work_dir / f"f_{i}.txt").write_text(f"file {i}")

        result = nxf_runner.run('''


workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [["k": [1]], file("${projectDir}/f_0.txt")],
        [["k": [2]], file("${projectDir}/f_1.txt")],
        [["k": [3]], file("${projectDir}/f_2.txt")],
    ])

    def batched = o._batch(3, ch)
    batched.view { "FILES: ${it[0].collect { i -> i.containsKey('FILES') }}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("FILES:")]
        assert len(lines) >= 1
        # All indexes should have FILES key
        assert "true" in lines[0].lower()

    @staticmethod
    def _run_two_step_files_check(nxf_runner: "NxfTestRunner", batch_size: int) -> list[str]:
        """Inbox #139 reproduction helper.

        Runs a two-process pipeline (step1 produces a path() output; step2
        consumes it via `o.group(..., batch_size)`). step2 echoes the index
        it received as JSON, so the test can inspect the FILES paths that
        `_batch()` wrote.

        Returns the deduplicated list of distinct path strings observed in
        every step2 invocation's `index['FILES']`. With the consumer-side
        fix in place, these still contain `/ws/...` strings (the producer
        is unchanged); callers should route them through
        `bootstrap._parse_path` to verify resolution.
        """
        for i in range(3):
            (nxf_runner.work_dir / f"seed_{i}.txt").write_text(f"seed {i}\n")

        result = nxf_runner.run(f'''


process step1 {{
    input:
        tuple val(index), path(_01)
    output:
        tuple val(index), path("*-1.*-out1.txt")
    script:
    """
    touch 1-1-1.HASH${{index.seed[0]}}-out1.txt
    """
}}

process step2 {{
    input:
        tuple val(index), path(_01)
    output:
        path "index.json"
    script:
    """
    echo '${{Orchestrator.JsonforEcho(index)}}' > index.json
    """
}}

workflow {{
    o = new Orchestrator(Channel.fromList([null]))

    seed_raw = Channel.fromList([
        [["seed": [1L]], file("${{projectDir}}/seed_0.txt")],
        [["seed": [2L]], file("${{projectDir}}/seed_1.txt")],
        [["seed": [3L]], file("${{projectDir}}/seed_2.txt")],
    ])
    seed = new Tuple2("seed", seed_raw)

    k1 = ["out1"]
    (_out1) = o.post(o.asStreams(step1(o.group("seed", [seed], k1, 1))), k1)

    k2 = ["out2"]
    step2(o.group("out1", [_out1], k2, {batch_size}))
}}
''')
        NxfTestRunner.assert_nxf_ok(result)
        index_files = sorted(nxf_runner.work_dir.rglob("work/*/*/index.json"))
        assert index_files, (
            f"step2 produced no index.json with batch_size={batch_size}; "
            f"stdout tail:\n{result.stdout[-1500:]}"
        )
        observed: list[str] = []
        for ip in index_files:
            raw = ip.read_text().strip()
            # Bash echo wraps Groovy's escaped quotes (\") in the JSON; unescape.
            parsed = json.loads(raw.replace('\\"', '"'))
            if not isinstance(parsed, list):
                parsed = [parsed]
            for idx in parsed:
                for group in idx.get("FILES", []):
                    for p in group:
                        observed.append(p)
        return observed

    @staticmethod
    def _assert_files_resolve(files: list[str]) -> None:
        """Inbox #139 fix verification.

        For each FILES path captured from a step's index, run it through
        `bootstrap._parse_path` and assert the resulting `local` view is
        the container-canonical form rooted at `AgentPaths.HOME_ROOT`
        with the supplied task key embedded. This is the assertion shape
        for a consumer-side fix: raw FILES still contain `/ws/...`
        strings; `_parse_path` rewrites them on read.
        """
        from pathlib import Path

        from metasmith.bootstrap import _parse_path
        from metasmith.constants import AgentPaths

        assert files, "no FILES paths to verify"
        task_key = "TEST"
        for p in files:
            parsed = _parse_path(
                Path(p),
                agent_home="/host/scratch/agent",
                external_cwd=Path("/ws"),
                task_key=task_key,
            )
            assert parsed.local.is_absolute() and parsed.local.is_relative_to(
                AgentPaths.HOME_ROOT
            ), f"_parse_path did not canonicalize {p!r}: local={parsed.local}"
            assert f"runs/{task_key}/" in str(parsed.local), (
                f"_parse_path lost the task_key in {parsed.local} (from {p!r})"
            )
            assert not str(parsed.local).startswith(str(AgentPaths.WORK_ROOT) + "/"), (
                f"_parse_path left /ws prefix in {parsed.local} (from {p!r})"
            )

    def test_batched_files_resolve_through_parse_path(self, nxf_runner):
        """Regression for inbox #139 — batched case.

        When step2 consumes step1's `path()` output through `_batch(N>1, …)`,
        `Orchestrator.groovy:274` renders each Path via `*.toString()`.
        Inside the producer container the workdir is bound at `/ws`, so
        the rendered string is `/ws/work/<hash>/<file>` — a path that
        doesn't resolve inside the downstream consumer's own container
        (its `/ws` is its own task dir). On HPC this trips
        `bootstrap.py:279` with "detected missing inputs, stopping".

        The fix in `bootstrap._parse_path` rewrites `/ws/<tail>` →
        `<AgentPaths.HOME_ROOT>/runs/<task_key>/<tail>` at parse time
        (mirroring `bin/sbatch:54-80`'s inverse rewrite). The raw FILES
        strings still contain `/ws/...`; this test confirms they
        resolve correctly when read.
        """
        files = self._run_two_step_files_check(nxf_runner, batch_size=2)
        self._assert_files_resolve(files)

    def test_unbatched_files_resolve_through_parse_path(self, nxf_runner):
        """Regression for inbox #139 — non-batched case.

        `o.group` always routes through `_batch` regardless of batch_size,
        so the `/ws`-prefix in FILES is *not* batched-only. The reporter's
        claim that non-batched works (msg #139, `p03__assembly_stats`)
        cannot be due to batch size alone; their non-batched comparison
        case must have been a given input (CSV-staged via `o.postIn` +
        `in()`), not a process output. The fix in `bootstrap._parse_path`
        covers both.
        """
        files = self._run_two_step_files_check(nxf_runner, batch_size=1)
        self._assert_files_resolve(files)

    def test_batch_debatch_roundtrip(self, nxf_runner):
        """Items survive batch -> process -> debatch cycle."""
        for i in range(4):
            (nxf_runner.work_dir / f"r_{i}.txt").write_text(f"roundtrip {i}")

        result = nxf_runner.run('''


process passthrough {
    input:
        tuple val(index), path("*")
    output:
        tuple val(index), path("*.out")
    script:
    """
    i=1; for f in *.txt; do cp "\\$f" "\\${i}-copy.out"; i=\\$((i+1)); done
    """
}

workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [["k": [1]], file("${projectDir}/r_0.txt")],
        [["k": [2]], file("${projectDir}/r_1.txt")],
        [["k": [3]], file("${projectDir}/r_2.txt")],
        [["k": [4]], file("${projectDir}/r_3.txt")],
    ])

    def batched = o._batch(2, ch)
    def debatched = o._debatch(o.asStreams(passthrough(batched)))
    debatched[0].view { idx, item -> "ROUNDTRIP: ${groovy.json.JsonOutput.toJson(idx)}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("ROUNDTRIP:")]
        # After debatch, FILES key should be removed
        for line in lines:
            idx_str = line.split(": ", 1)[1]
            idx = json.loads(idx_str)
            assert "FILES" not in idx


class TestLinWire:
    """The `lin` envelope a batched task actually puts on the wire.

    This is the one seam the fast suite structurally cannot cover: its
    harnesses synthesize payloads with `json.dumps` and never go through the
    Groovy emitter, which is how R5's version desync failed every
    containerized task with a green fast run. The script below emits the
    exact expression `nextflow_codegen` compiles into every process.
    """

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "cfd0236's emitter sends `entries:index[0]`, so a batch_size=N "
            "task puts only member 0 on the wire"
        ),
    )
    def test_batched_task_puts_every_member_on_the_wire(self, nxf_runner):
        """A 3-member batch emits 3 lineage maps with 3 distinct FILES groups.

        Reproduced on real Nextflow: the task's `index` has size 3 (three
        `_collateBatch` members) while the envelope carries one FILES group
        holding one file. `bootstrap` then wraps that single map — the
        `for batch, batch_lineage in enumerate(lineages)` loop runs once —
        so a `checkm`-shaped transform at `batch_size=25` processes one
        assembly and stages twenty-four it never opens.
        """
        from metasmith.models.lineage import LinPayload
        from metasmith.models.workflow.nextflow_codegen import LIN_ECHO_EXPR

        n = 3
        for i in range(n):
            (nxf_runner.work_dir / f"seed_{i}.txt").write_text(f"seed {i}\n")
        seeds = ",\n        ".join(
            f'[[:], file("${{projectDir}}/seed_{i}.txt")]' for i in range(n)
        )

        result = nxf_runner.run(f'''
process step1 {{
    input:
        tuple val(index), path(_01)
    output:
        tuple val(index), path("*-out1.txt")
    script:
    def stem = index[0].seed[0]
    """
    touch 1-${{stem}}-out1.txt
    """
}}

process step2 {{
    input:
        tuple val(index), path(_01)
    output:
        path "lin.json"
    script:
    """
    echo "{LIN_ECHO_EXPR}" > lin.json
    """
}}

workflow {{
    o = new Orchestrator(Channel.fromList([null]))

    def seed = (o.postIn([Channel.fromList([
        {seeds},
    ])], ["seed"]))[0]

    def k1 = ["out1"]
    def _out1 = (o.post(o.asStreams(step1(o.group("seed", [seed], k1, 1))), k1))[0]

    step2(o.group("out1", [_out1], ["out2"], {n}))
}}
''', timeout=180)
        NxfTestRunner.assert_nxf_ok(result)

        lin_files = sorted(nxf_runner.work_dir.rglob("work/*/*/lin.json"))
        assert len(lin_files) == 1, (
            f"expected {n} keys at batch_size={n} to fold into one task, "
            f"got {len(lin_files)}"
        )
        # `echo` renders Groovy's bash-escaped quotes; undo them the same way
        # `bootstrap` does when it reads the `lin` line back.
        raw = lin_files[0].read_text().strip().replace('\\"', '"')
        payload = LinPayload.from_json(raw)
        assert isinstance(payload.entries, list), (
            f"the wire must carry a list of per-member maps, got "
            f"{type(payload.entries).__name__}: {raw}"
        )
        assert len(payload.entries) == n, (
            f"batch of {n} members put {len(payload.entries)} lineage map(s) "
            f"on the wire: {raw}"
        )
        groups = [m.get(LinPayload.FILES_KEY) for m in payload.entries]
        assert all(g for g in groups), f"a member carried no FILES: {groups}"
        assert len({json.dumps(g) for g in groups}) == n, (
            f"members must carry their own inputs, got duplicates: {groups}"
        )


class TestOrchestratorMix:
    """Test stream mixing."""

    def test_mix_merges_streams(self, nxf_runner):
        """mix() combines preserving all items."""
        result = nxf_runner.run('''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch_a = Channel.fromList([
        [["a": [1]], file("${projectDir}/test.nf")],
    ])
    ch_b = Channel.fromList([
        [["a": [2]], file("${projectDir}/test.nf")],
    ])

    def mixed = o.mix([["x", ch_a], ["x", ch_b]])
    def (name, stream) = mixed
    stream.view { "MIX: ${it[0]}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("MIX:")]
        assert len(lines) == 2

    def test_mix_preserves_name(self, nxf_runner):
        """mix() keeps name from streams[0]."""
        result = nxf_runner.run('''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch_a = Channel.fromList([
        [[:], file("${projectDir}/test.nf")],
    ])
    ch_b = Channel.fromList([
        [[:], file("${projectDir}/test.nf")],
    ])

    def mixed = o.mix([["first_name", ch_a], ["second_name", ch_b]])
    def (name, stream) = mixed
    println "NAME: ${name}"
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        assert "NAME: first_name" in result.stdout


class TestOrchestratorPublish:
    """Test publish behavior."""

    def test_publish_outputs_json_index(self, nxf_runner):
        """publish() converts index to JSON string."""
        result = nxf_runner.run('''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [["a": [1], "b": [2]], file("${projectDir}/test.nf")],
    ])

    def published = o.publish(["x", ch])
    published.view { json_idx, item -> "PUB: ${json_idx}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("PUB:")]
        assert len(lines) == 1
        # Should be valid JSON
        json_str = lines[0].split("PUB: ")[1]
        parsed = json.loads(json_str)
        assert "a" in parsed
        assert "b" in parsed


class TestPathStringification:
    """Capture the empirical shape of `Path.toString()` inside a Nextflow
    process when the container workdir is bound at `/ws`.

    Background: `Orchestrator.groovy:274` (`_batch`) renders upstream
    process outputs via `values*.toString()`. The downstream consumer
    receives the rendered strings in `index['FILES']` and routes them
    through `bootstrap._parse_path`. The shape that `toString()`
    actually emits is runtime-dependent — Docker emits absolute
    `/ws/<tail>` (this test), while apptainer-local has been observed
    to emit relative `../ws/<tail>` (the bug deferred to the path
    overhaul; not exercised here because apptainer is not available
    in CI — see `tests/path_overhaul/test_parse_path_apptainer_relative.py`
    for the unit-level reproduction).
    """

    def test_docker_emits_absolute_ws_prefix(self, nxf_runner):
        """Inside a Docker-bound container with workdir `/ws`,
        `Path.toString()` on a workflow-generated path starts with
        `/ws/`. This is the shape `Orchestrator.groovy:274`'s
        `*.toString()` produces for upstream outputs.
        """
        result = nxf_runner.run('''
process produce {
    output:
        path "out.txt"
    script:
    """
    echo hello > out.txt
    """
}

workflow {
    produce()
    produce.out.view { p -> "STRSHAPE: ${p.toString()}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("STRSHAPE:")]
        assert len(lines) == 1, f"expected exactly one STRSHAPE line, got: {lines}"
        rendered = lines[0].split("STRSHAPE: ", 1)[1]
        # Docker stringification is absolute and `/ws/`-rooted.
        assert rendered.startswith("/ws/"), (
            f"Docker emitted unexpected toString shape: {rendered!r}. "
            f"If this changes, `bootstrap._parse_path` case-1 (the inbox "
            f"#139 fix) needs to be re-validated."
        )
        # No `..` segments — pre-condition for `_parse_path` case-1's
        # `relative_to(WORK_ROOT)` to succeed.
        assert ".." not in rendered.split("/"), (
            f"Docker emitted `..` segment unexpectedly: {rendered!r}."
        )
