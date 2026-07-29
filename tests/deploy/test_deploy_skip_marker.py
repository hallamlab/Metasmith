"""Pin Agent.Deploy()'s update semantics.

History: Deploy() used to short-circuit when ``<home>`` already existed,
which silently broke deploys whose home dir had been pre-created by an
unrelated step (e.g. the APPTAINER harness pre-places
``agent_home/container_images/<sif>``). The home-dir short-circuit was
replaced with a marker-file check, then removed entirely once it became
clear the underlying transport (``Logistics`` uses ``rsync -auP``) and
the container pull (``[ -e {sif} ] || pull``) already provide the right
"don't re-do unchanged work" semantics. The only remaining truly
expensive step — extracting ``msm_relay`` from the container — is now
gated on the actual artifact (``<home>/relay/msm_relay``), so a partial
deploy self-heals on the next call without ``assertive=True``.

These are source-pattern tests (no container spin-up). They mirror the
pinning style of ``test_container_tag.py`` — what we're protecting is a
behavioral contract that's easy to regress with a one-line edit.
"""
from pathlib import Path

from metasmith.constants import AgentPaths, MODULE_PATH


def _deploy_block() -> str:
    # Find Deploy wherever in the package it lives. This used to read
    # `agents.py` by name; `agents` became a package and the path stopped
    # existing, which at least failed loudly -- a source-pattern test that
    # silently reads the wrong file passes on an absence it never checked.
    for src in sorted((MODULE_PATH / "agents").rglob("*.py")):
        text = src.read_text()
        if "def Deploy(" not in text:
            continue
        start = text.index("def Deploy(")
        # Deploy is now the last method in its module, so "up to the next
        # method" has to fall back to end-of-file or the block comes out empty
        # -- and every assertion here that pins an *absence* would pass on it.
        end = text.find("\n    def ", start + 1)
        return text[start:] if end == -1 else text[start:end]
    raise AssertionError(f"no `def Deploy(` found under {MODULE_PATH / 'agents'}")


def test_no_home_dir_short_circuit():
    """Forbid the historical regression: skipping Deploy purely because
    the home directory exists. APPTAINER harness pre-creates
    ``agent_home/container_images/<sif>`` and that would silently no-op
    the deploy.
    """
    block = _deploy_block()
    assert '[[ -e "{self.home.GetPath()}" ]]' not in block, (
        "Deploy() must not skip purely because the home directory exists"
    )


def test_no_marker_file_short_circuit():
    """Forbid the intermediate marker-file fix that we since replaced.
    The marker check was redundant once we trusted rsync's mtime delta
    and per-artifact existence guards. Re-introducing it would just
    create another way to mismatch "marker says deployed" vs. "actual
    binary is missing".
    """
    block = _deploy_block()
    marker = AgentPaths.to_definition(Path("/probe"))
    assert marker == Path("/probe/lib/agent.yml"), \
        "AgentPaths.to_definition contract changed; update this test"
    # The marker path itself may still appear (it's written by
    # _remote_file), but it must NOT appear as a short-circuit guard.
    assert "already deployed" not in block, (
        "Deploy() must not short-circuit on the lib/agent.yml marker; "
        "let rsync -au + per-artifact existence checks handle reuse"
    )


def test_relay_extraction_is_artifact_gated():
    """The one truly expensive step left in Deploy() is extracting
    ``msm_relay`` from the container (``deploy_from_container``). It
    must be gated on the relay binary's actual presence so a partial
    deploy (sif present, relay missing) self-heals on the next call.
    """
    block = _deploy_block()
    relay = AgentPaths.to_relay(Path("/probe"))
    assert relay == Path("/probe/relay/msm_relay"), \
        "AgentPaths.to_relay contract changed; update this test"
    assert "AgentPaths.to_relay(self.home.GetPath())" in block, (
        "relay extraction must derive its gate path from AgentPaths.to_relay"
    )
    assert "deploy_from_container" in block, \
        "this test pins the relay-extract step; if it's gone, re-think the gate"


def test_assertive_overrides_relay_gate():
    """``assertive=True`` must force the relay re-extraction even when
    the binary is already present. Otherwise users have no way to
    refresh a stale container without ``rm -rf``.
    """
    block = _deploy_block()
    assert "not assertive" in block, (
        "assertive flag must still be consulted alongside the relay-present check"
    )
