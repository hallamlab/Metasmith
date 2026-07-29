"""Bind paths must survive the shell round trip they are subjected to.

The external-bind list is `echo`ed into `.command.binds`, `cat`ed back, and
interpolated unquoted into the container command line, where the shell word-
splits it into argv. A path containing a space therefore arrives as several
arguments, and two consecutive spaces collapse to one at the `echo` -- neither
is recoverable downstream, and both produce a mount that is silently wrong
rather than an error. So compilation refuses such a path up front.

Supporting them for real would mean carrying binds as a bash array through
`MakeRunCommand`'s `custom_bind_param` contract and both rendered templates;
until then, a named error beats a broken mount.
"""

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
        "/data/a  b",       # collapses to one space at the echo
        "/data/tab\there",
        "/leading /space",
    ])
    def test_whitespace_is_refused(self, bad):
        with pytest.raises(ValueError) as e:
            AssertBindPathsAreShellSafe([Path("/data/fine"), Path(bad)], "my_step")
        # The error has to name the offending path -- it is often a common
        # *parent* of the files the user listed, so "one of your inputs" would
        # leave them hunting.
        assert str(Path(bad)) in str(e.value)
        assert "my_step" in str(e.value)
        assert "whitespace" in str(e.value)

    @pytest.mark.parametrize("ok", ["/data/*.fna", "/data/[ab]/x", "/data/a'b"])
    def test_other_shell_metacharacters_are_left_alone(self, ok):
        # These reach the container command line intact; refusing them would
        # reject working setups.
        AssertBindPathsAreShellSafe([Path(ok)], "my_step")
