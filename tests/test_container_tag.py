"""Regression tests for the source-controlled container tag (issue 1).

`Agent.container` defaults to the image pushed to quay by the release.
The wiring is `version.txt` -> `constants.VERSION` -> `constants.CONTAINER_TAG`
(with '+' -> '-' for Docker tag compliance) -> `Agent.container`.

If any link in that chain drifts, fresh deploys can pull a stale or
non-existent tag. These tests pin the chain.
"""

from pathlib import Path

import yaml

from metasmith.agents import Agent
from metasmith.constants import CONTAINER_TAG, MODULE_PATH, VERSION
from metasmith.coms.containers import ContainerRuntime
from metasmith.models.remote import Source


def test_version_txt_is_loaded_as_VERSION():
    raw = (MODULE_PATH / "version.txt").read_text().strip()
    assert VERSION == raw


def test_container_tag_is_pep440_to_docker_translation():
    # CONTAINER_TAG is VERSION with PEP 440 '+' replaced by Docker '-'.
    # Anything else (e.g. dropping the hash) would silently revert the fix.
    assert CONTAINER_TAG == VERSION.replace("+", "-")
    assert "+" not in CONTAINER_TAG


def test_agent_default_container_uses_CONTAINER_TAG(tmp_path):
    agent = Agent(home=Source.FromLocal(tmp_path), runtime=ContainerRuntime.APPTAINER)
    assert agent.container == f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"


def test_agent_yml_round_trip_preserves_container(tmp_path):
    # 'compiled container version on agent deploy' = the literal string
    # Deploy() writes into lib/agent.yml and that StageAndRunTransform reads
    # back via Agent.Load(). Pack/Unpack is exactly that round-trip.
    agent = Agent(home=Source.FromLocal(tmp_path), runtime=ContainerRuntime.APPTAINER)
    packed = agent.Pack()
    assert packed["container"] == f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"

    agent_yml = tmp_path / "agent.yml"
    agent.Save(agent_yml)
    loaded = Agent.Load(agent_yml)
    assert loaded.container == agent.container
    # Spot-check the on-disk YAML directly so we catch any Pack/Unpack
    # transformation that silently rewrites the container string.
    on_disk = yaml.safe_load(agent_yml.read_text())
    assert on_disk["container"] == f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"


def test_docker_builder_uses_same_tag():
    """get_docker_tag() (used by the test build path and by dev.sh's
    successor) must produce the same tag Agent.container expects."""
    from metasmith.testing.docker_builder import get_docker_tag, get_git_version

    assert get_git_version() == VERSION
    assert get_docker_tag() == f"quay.io/hallamlab/metasmith:{CONTAINER_TAG}"
