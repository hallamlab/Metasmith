"""Pin Agent.Deploy()'s SIF→sandbox unpack step.

When the host's apptainer ships no setuid `starter-suid`, apptainer
falls back to squashfuse_ll for SIF mounts. That path deadlocks under
msm_relay's fork chain on WSL2 (Bug E.2). Deploy() now probes for the
missing setuid helper and, if absent, unpacks the SIF to a sandbox
directory once. MakeRunCommand(local=True) then picks the sandbox over
the SIF.

These are source-pattern tests, matching ``test_deploy_skip_marker.py``.
"""
from metasmith.constants import MODULE_PATH


def _deploy_block() -> str:
    text = (MODULE_PATH / "agents.py").read_text()
    start = text.index("def Deploy(")
    end = text.index("\n    def ", start + 1)
    return text[start:end]


def test_deploy_emits_sandbox_probe_and_build():
    block = _deploy_block()
    # The probe must surface through MakeNeedsSandboxProbe so the helper
    # stays in one place (containers.py) and the deploy step is just a
    # caller. If a future edit inlines the probe text here, this test
    # breaks deliberately.
    assert "MakeNeedsSandboxProbe" in block
    assert "MakeBuildSandboxCommand" in block
    # The conditional must check the probe's sentinel AND skip when the
    # sandbox dir already exists — a partial deploy should self-heal but
    # a healthy one should not re-extract.
    assert '"needs-sandbox"' in block
    assert "! -d" in block


def test_deploy_sandbox_step_is_artifact_gated():
    """Skip semantics: if the sandbox dir is already present we don't
    rebuild it. Matches the relay-binary skip idiom one step below.
    """
    block = _deploy_block()
    assert "GetSandboxPath" in block, (
        "deploy step must derive its skip gate from Container.GetSandboxPath"
    )


def test_assertive_clears_sandbox():
    """``assertive=True`` must invalidate the sandbox so a forced
    redeploy actually rebuilds it. Otherwise users have no way to
    refresh a corrupted unpack short of `rm -rf` by hand.
    """
    block = _deploy_block()
    # The pattern is `rm -rf <sandbox>` inserted ahead of the conditional
    # only when `assertive` is true. Looser substring match keeps this
    # robust to f-string formatting choices.
    assert "rm -rf" in block
    assert "assertive" in block
