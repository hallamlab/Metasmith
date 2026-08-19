from pathlib import Path

import pytest

from metasmith.models.workflow.nextflow_codegen import AssertBindPathsAreShellSafe


class TestBindPathValidation:
    def test_ordinary_paths_pass(self):
        AssertBindPathsAreShellSafe(
            [Path("/data/reads"), Path("/ref/gtdb-r220"), Path("/x/a.b_c-d")],
            "some_step",
        )

    def test_empty_passes(self):
        AssertBindPathsAreShellSafe([], "some_step")

    @pytest.mark.parametrize("bad", [
        "/data/two words",
        "/data/a  b",
        "/data/tab\there",
        "/leading /space",
    ])
    def test_whitespace_is_refused(self, bad):
        with pytest.raises(ValueError) as e:
            AssertBindPathsAreShellSafe([Path("/data/fine"), Path(bad)], "my_step")
        assert str(Path(bad)) in str(e.value)
        assert "my_step" in str(e.value)
        assert "whitespace" in str(e.value)

    @pytest.mark.parametrize("ok", ["/data/*.fna", "/data/[ab]/x", "/data/a'b"])
    def test_other_shell_metacharacters_are_left_alone(self, ok):
        AssertBindPathsAreShellSafe([Path(ok)], "my_step")
