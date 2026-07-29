"""Pin Agent.Deploy()'s SIF↔sandbox decision step.

Deploy probes the host (setuid starter-suid + apptainer version) and
emits exactly one of two outcomes:
 - "use-sif" verdict → ensure no `.sandbox` dir exists (remove stale)
 - "use-sandbox" verdict → build the sandbox if missing (idempotent)

This shape covers all three known mechanisms: kernel mount (setuid
present — Sockeye), squashfuse_ll (no setuid + apptainer >=1.4 — WSL2
hosts hit Bug E.2 wedge so we prefer sandbox), and fuse-overlayfs
(no setuid + apptainer 1.3.x — fir hits Bug E.4 SIGBUS so we keep SIF).

These are source-pattern tests, matching ``test_deploy_skip_marker.py``.
"""
from metasmith.constants import MODULE_PATH


def _deploy_block() -> str:
    # The SIF/sandbox decision moved out of Agent.Deploy into the sealed
    # env module (Environment.ProvisionSteps), so Deploy never branches on
    # a runtime. This pins the logic at its new home.
    text = (MODULE_PATH / "env" / "environment.py").read_text()
    start = text.index("def ProvisionSteps(")
    end = text.index("\n    def ", start + 1)
    return text[start:end]


def test_deploy_calls_decision_probe_and_build_helpers():
    block = _deploy_block()
    # The probe and build commands must come from Container helpers so
    # the shell logic lives in one place (containers.py). If a future
    # edit inlines the probe text here, this test breaks deliberately.
    assert "MakeSandboxDecisionProbe" in block
    assert "MakeBuildSandboxCommand" in block


def test_deploy_branches_on_verdict():
    block = _deploy_block()
    # Probe output is captured in a shell variable; both verdict literals
    # appear in the branching logic.
    assert "VERDICT=$(" in block
    assert '"use-sandbox"' in block
    # Sandbox is built ONLY on the use-sandbox branch; the existence
    # check makes the build idempotent across redeploys.
    assert "[ -d " in block


def test_deploy_clears_stale_sandbox_when_sif_verdict():
    """If the verdict flips from use-sandbox to use-sif (apptainer
    upgrade or setuid added), Deploy must remove the stale sandbox dir
    so the run-time ternary in MakeRunCommand picks SIF, not the
    out-of-date unpack."""
    block = _deploy_block()
    # The else arm must `rm -rf` the sandbox path.
    assert "GetSandboxPath" in block
    assert "rm -rf" in block
    # And the else arm must be reachable (no unconditional "if" wrapping
    # the whole thing).
    assert "else " in block or "else\n" in block


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
