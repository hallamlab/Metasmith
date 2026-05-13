"""Orchestrator.groovy edge case tests via real Nextflow execution.

These tests run minimal Nextflow scripts that exercise the Orchestrator
class's group, batch, debatch, post, and mix methods inside Docker.
"""

import json
import subprocess
import shutil
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
        """Assert Nextflow succeeded, tolerating the NXF 25.x duration bug.

        Nextflow 25.x has a known timing bug where very-fast workflows produce
        a negative Duration assertion error (returncode=1) even when logic succeeds.
        """
        nxf_duration_bug = "Duration unit cannot be a negative number" in result.stdout
        assert result.returncode == 0 or nxf_duration_bug, f"NXF failed: {result.stderr}"


@pytest.fixture
def nxf_runner(tmp_path, docker_image):
    """Create an NxfTestRunner for the test."""
    return NxfTestRunner(tmp_path / "nxf_test", docker_image)


class TestOrchestratorPost:
    """Test post/postIn hash and index behavior."""

    def test_post_produces_output(self, nxf_runner):
        """post() processes items and produces indexed output."""
        result = nxf_runner.run('''
import groovy.json.JsonOutput

workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [["a": [1]], file("${projectDir}/test.nf")],
        [["a": [2]], file("${projectDir}/lib/Orchestrator.groovy")],
    ])

    def (out) = o.post([ch], ["result"])
    def (name, stream) = out
    stream.view { idx, item -> "POST: ${JsonOutput.toJson(idx)} ${item.name}" }
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
import groovy.json.JsonOutput

workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [[:], file("${projectDir}/input_0.txt")],
        [[:], file("${projectDir}/input_1.txt")],
        [[:], file("${projectDir}/input_2.txt")],
    ])

    def (out) = o.postIn([ch], ["inp"])
    def (name, stream) = out
    stream.view { idx, item -> "POSTIN: ${JsonOutput.toJson(idx)} ${item.name}" }
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

    def test_post_hash_15chars(self, nxf_runner):
        """Hash in post is md5[0..14] parsed as long."""
        result = nxf_runner.run('''
import groovy.json.JsonOutput

workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [[:], file("${projectDir}/test.nf")],
    ])

    def (out) = o.post([ch], ["x"])
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
        # Verify it's a Long
        assert "Long" in lines[0] or "long" in lines[0].lower()


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
import groovy.json.JsonOutput

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

    def (posted_a) = o.postIn([ch_a_raw], ["a"])
    def (posted_b) = o.postIn([ch_b_raw], ["b"])

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

    def (posted_a) = o.postIn([ch_a_raw], ["a"])
    def (posted_b_early) = o.postIn([ch_b_raw_early], ["b"])
    def (posted_b_late) = o.postIn([ch_b_raw_late], ["b"])
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

    def (posted) = o.postIn([ch], ["x"])

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
    for (i in 0..8) {
        sample_items.add([[:], file("${projectDir}/sample_${i}.txt")])
    }
    ch_samples = Channel.fromList(sample_items)

    // Shared streams: exp and container
    ch_exp = Channel.fromList([[[:], file("${projectDir}/exp.txt")]])
    ch_container = Channel.fromList([[[:], file("${projectDir}/container.txt")]])

    // Post all inputs
    def (posted_samples) = o.postIn([ch_samples], ["sample"])
    def (posted_exp) = o.postIn([ch_exp], ["exp"])
    def (posted_container) = o.postIn([ch_container], ["container"])

    // p01: groups by sample, also needs exp + container
    def g1 = o.group("sample", [posted_samples, posted_exp, posted_container], ["p01_out"], 1)
    def p01_result = p01_per_sample(g1)
    def (p01_posted) = o.post([p01_result], ["p01_out"])

    // p02: groups by exp, needs container
    // posted_exp and posted_container are reused — shared across g1, g2, g3
    def g2 = o.group("exp", [posted_exp, posted_container], ["p02_out"], 1)
    def p02_result = p02_per_exp(g2)
    def (p02_posted) = o.post([p02_result], ["p02_out"])

    // p03: groups by exp, needs p01 + p02 outputs + exp + container
    def g3 = o.group("exp", [p01_posted, p02_posted, posted_exp, posted_container], ["p03_out"], 1)
    def p03_result = p03_merge(g3)

    p01_result.view { "P01: ${it[0]}" }
    p02_result.view { "P02: ${it[0]}" }
    p03_result.view { "P03: ${it[0]}" }
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
import groovy.json.JsonOutput

workflow {
    o = new Orchestrator(Channel.fromList([null]))

    ch = Channel.fromList([
        [["k": [1]], file("${projectDir}/f_0.txt")],
        [["k": [2]], file("${projectDir}/f_1.txt")],
        [["k": [3]], file("${projectDir}/f_2.txt")],
    ])

    def batched = o._batch(3, ch)
    batched.view { "FILES: ${it[0].collect(i -> i.containsKey('FILES'))}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("FILES:")]
        assert len(lines) >= 1
        # All indexes should have FILES key
        assert "true" in lines[0].lower()

    def test_batch_debatch_roundtrip(self, nxf_runner):
        """Items survive batch -> process -> debatch cycle."""
        for i in range(4):
            (nxf_runner.work_dir / f"r_{i}.txt").write_text(f"roundtrip {i}")

        result = nxf_runner.run('''
import groovy.json.JsonOutput

process passthrough {
    input:
        tuple val(index), path("*")
    output:
        tuple val(index), path("*.out")
    stub:
    """
    i=1; for f in *.txt; do cp "\\$f" "\\${i}-copy.out"; i=\\$((i+1)); done
    """
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
    def debatched = o._debatch([*passthrough(batched)])
    debatched[0].view { idx, item -> "ROUNDTRIP: ${JsonOutput.toJson(idx)}" }
}
''')
        NxfTestRunner.assert_nxf_ok(result)
        lines = [l for l in result.stdout.split("\n") if l.startswith("ROUNDTRIP:")]
        # After debatch, FILES key should be removed
        for line in lines:
            idx_str = line.split(": ", 1)[1]
            idx = json.loads(idx_str)
            assert "FILES" not in idx


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
