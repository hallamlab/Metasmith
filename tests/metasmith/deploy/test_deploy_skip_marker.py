from pathlib import Path

from metasmith.constants import AgentPaths, MODULE_PATH


def _deploy_block() -> str:
    for src in sorted((MODULE_PATH / "agents").rglob("*.py")):
        text = src.read_text()
        if "def Deploy(" not in text:
            continue
        start = text.index("def Deploy(")
        end = text.find("\n    def ", start + 1)
        return text[start:] if end == -1 else text[start:end]
    raise AssertionError(f"no `def Deploy(` found under {MODULE_PATH / 'agents'}")


def test_no_home_dir_short_circuit():
    block = _deploy_block()
    assert '[[ -e "{self.home.GetPath()}" ]]' not in block, (
        "Deploy() must not skip purely because the home directory exists"
    )


def test_no_marker_file_short_circuit():
    block = _deploy_block()
    marker = AgentPaths.to_definition(Path("/probe"))
    assert marker == Path("/probe/lib/agent.yml"), \
        "AgentPaths.to_definition contract changed; update this test"
    assert "already deployed" not in block, (
        "Deploy() must not short-circuit on the lib/agent.yml marker; "
        "let rsync -au + per-artifact existence checks handle reuse"
    )


def test_relay_extraction_is_artifact_gated():
    block = _deploy_block()
    relay = AgentPaths.to_relay(Path("/probe"))
    assert relay == Path("/probe/relay/msm_relay"), \
        "AgentPaths.to_relay contract changed; update this test"
    assert "AgentPaths.to_relay(resolved_agent_home)" in block, (
        "relay extraction must derive its gate path from AgentPaths.to_relay, "
        "applied to the *resolved* home -- self.home.GetPath() may still carry "
        "a literal ~ that a quoted bash check would never expand"
    )
    assert "deploy_from_container" in block, \
        "this test pins the relay-extract step; if it's gone, re-think the gate"


def test_assertive_overrides_relay_gate():
    block = _deploy_block()
    assert "not assertive" in block, (
        "assertive flag must still be consulted alongside the relay-present check"
    )
