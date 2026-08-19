from pathlib import Path


def test_parse_path_normalizes_relative_ws_prefix() -> None:
    from metasmith.bootstrap import _parse_path
    from metasmith.constants import AgentPaths

    external_cwd = Path("/host/agent/runs/TESTKEY/nxf_work/cd/ef0123")
    parsed = _parse_path(
        Path("../ws/work/aa/bb/output.fa"),
        agent_home="/host/agent",
        external_cwd=external_cwd,
        task_key="TESTKEY",
    )

    assert parsed.local.is_absolute(), f"local not absolute: {parsed.local}"
    assert parsed.external.is_absolute(), f"external not absolute: {parsed.external}"
    assert parsed.container.is_absolute(), f"container not absolute: {parsed.container}"
    assert ".." not in parsed.local.parts, f"local contains '..': {parsed.local}"
    assert ".." not in parsed.external.parts, f"external contains '..': {parsed.external}"
    assert ".." not in parsed.container.parts, f"container contains '..': {parsed.container}"

    assert parsed.local == AgentPaths.HOME_ROOT / "runs/TESTKEY/work/aa/bb/output.fa"
    assert parsed.external == Path("/host/agent/runs/TESTKEY/work/aa/bb/output.fa")
