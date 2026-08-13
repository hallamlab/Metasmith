"""Apptainer-local Nextflow emits upstream-output paths as
`Path('../ws/work/<hash>/<file>')` (relative-to-pwd) rather than the
absolute `/ws/<tail>` form Docker emits. Today `bootstrap._parse_path`
case-3 falls through to `external = external_cwd/p` without
normalization, leaving a literal `..` segment in `cp.external`,
`cp.local`, and `cp.container`.

This test feeds the relative shape into `_parse_path` and asserts the
three views are absolute and `..`-free. Fails today; will pass once the
path overhaul lands.

Reference: session debrief #133 ("Issue 3 (apptainer local-path
mangling) deferred to a path-overhaul"); audit Category E (naked joins).
"""
from pathlib import Path


def test_parse_path_normalizes_relative_ws_prefix() -> None:
    from metasmith.bootstrap import _parse_path
    from metasmith.constants import AgentPaths

    # apptainer-local: cwd is a real host path; the relative `../ws/...`
    # walks up out of the per-task workdir into a sibling work directory.
    external_cwd = Path("/host/agent/runs/TESTKEY/nxf_work/cd/ef0123")
    parsed = _parse_path(
        Path("../ws/work/aa/bb/output.fa"),
        agent_home="/host/agent",
        external_cwd=external_cwd,
        task_key="TESTKEY",
    )

    # All three views must be absolute and `..`-free.
    assert parsed.local.is_absolute(), f"local not absolute: {parsed.local}"
    assert parsed.external.is_absolute(), f"external not absolute: {parsed.external}"
    assert parsed.container.is_absolute(), f"container not absolute: {parsed.container}"
    assert ".." not in parsed.local.parts, f"local contains '..': {parsed.local}"
    assert ".." not in parsed.external.parts, f"external contains '..': {parsed.external}"
    assert ".." not in parsed.container.parts, f"container contains '..': {parsed.container}"

    # Resolve to the same canonical form as the absolute /ws/<tail> case:
    # rooted at HOME_ROOT/runs/<key>/<tail> on the local view, and at
    # <agent_home>/runs/<key>/<tail> on the external view.
    assert parsed.local == AgentPaths.HOME_ROOT / "runs/TESTKEY/work/aa/bb/output.fa"
    assert parsed.external == Path("/host/agent/runs/TESTKEY/work/aa/bb/output.fa")
