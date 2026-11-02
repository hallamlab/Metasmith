import re
from pathlib import Path

import pytest
import yaml

from metasmith.agents import Agent
from metasmith._build_hash import compute_build_hash
from metasmith.constants import (
    BUILD_HASH,
    CONTAINER_TAG,
    FULL_VERSION,
    MODULE_PATH,
    VERSION,
)
from metasmith.env import Runtime
from metasmith.models.remote import Source


def test_version_txt_is_pure_semver():
    raw = (MODULE_PATH / "version.txt").read_text().strip()
    assert VERSION == raw
    assert "+" not in VERSION, "version.txt must not contain a build hash; that's build_hash.txt's job"
    assert re.fullmatch(r"\d+\.\d+\.\d+", VERSION), \
        f"version.txt should be MAJOR.MINOR.PATCH, got [{VERSION}]"


def test_build_hash_when_present_is_short_hex():
    if BUILD_HASH:
        assert re.fullmatch(r"[0-9a-f]+", BUILD_HASH), \
            f"build_hash.txt should be hex, got [{BUILD_HASH}]"


def test_full_version_composition():
    expected = f"{VERSION}+{BUILD_HASH}" if BUILD_HASH else VERSION
    assert FULL_VERSION == expected


def test_container_tag_is_full_version_with_plus_translated():
    assert CONTAINER_TAG == FULL_VERSION.replace("+", "-")
    assert "+" not in CONTAINER_TAG, "Docker tags reject '+'"


def test_agent_default_container_uses_CONTAINER_TAG(tmp_path):
    agent = Agent(home=Source.FromLocal(tmp_path), runtime=Runtime.APPTAINER)
    assert agent.container == f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"


def test_agent_yml_round_trip_preserves_container(tmp_path):
    agent = Agent(home=Source.FromLocal(tmp_path), runtime=Runtime.APPTAINER)
    packed = agent.Pack()
    assert packed["container"] == f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"

    agent_yml = tmp_path / "agent.yml"
    agent.Save(agent_yml)
    loaded = Agent.Load(agent_yml)
    assert loaded.container == agent.container
    on_disk = yaml.safe_load(agent_yml.read_text())
    assert on_disk["container"] == f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"


def test_docker_builder_uses_same_tag():
    from metasmith.testing.docker_builder import get_docker_tag, get_full_version

    assert get_full_version() == FULL_VERSION
    assert get_docker_tag() == f"quay.io/hallamlab/metasmith:{CONTAINER_TAG}"


def test_build_hash_is_deterministic():
    h1 = compute_build_hash()
    h2 = compute_build_hash()
    assert h1 == h2
    assert re.fullmatch(r"[0-9a-f]{7}", h1), f"expected 7-char hex, got [{h1}]"


def test_a_source_tree_refuses_an_unpublished_default_image(monkeypatch):
    import metasmith.agents.agent as agent_mod
    monkeypatch.setattr(agent_mod, "BUILD_HASH", "")
    monkeypatch.setattr(agent_mod, "_registry_has_tag", lambda ref: False)
    with pytest.raises(ValueError, match="container="):
        agent_mod._assert_container_resolvable(agent_mod.DEFAULT_CONTAINER, Runtime.APPTAINER, False)


@pytest.mark.parametrize("published", [True, None])
def test_a_published_or_unknown_default_image_deploys(monkeypatch, published):
    import metasmith.agents.agent as agent_mod
    monkeypatch.setattr(agent_mod, "BUILD_HASH", "")
    monkeypatch.setattr(agent_mod, "_registry_has_tag", lambda ref: published)
    agent_mod._assert_container_resolvable(agent_mod.DEFAULT_CONTAINER, Runtime.APPTAINER, False)


def test_an_explicit_image_or_a_built_package_is_used_as_given(monkeypatch):
    import metasmith.agents.agent as agent_mod
    monkeypatch.setattr(agent_mod, "BUILD_HASH", "")
    monkeypatch.setattr(agent_mod, "_registry_has_tag", lambda ref: False)
    agent_mod._assert_container_resolvable("docker://example/metasmith:dev", Runtime.APPTAINER, False)
    agent_mod._assert_container_resolvable(agent_mod.DEFAULT_CONTAINER, Runtime.MAMBA, False)
    monkeypatch.setattr(agent_mod, "BUILD_HASH", "abc1234")
    agent_mod._assert_container_resolvable(agent_mod.DEFAULT_CONTAINER, Runtime.APPTAINER, False)


def test_a_local_docker_agent_uses_the_default_tag_once_it_is_built_here(monkeypatch):
    import metasmith.agents.agent as agent_mod
    monkeypatch.setattr(agent_mod, "BUILD_HASH", "")
    monkeypatch.setattr(agent_mod, "_registry_has_tag", lambda ref: False)
    monkeypatch.setattr(agent_mod, "_local_docker_image", lambda ref: True)
    agent_mod._assert_container_resolvable(agent_mod.DEFAULT_CONTAINER, Runtime.DOCKER, False, local_home=True)
    with pytest.raises(ValueError, match="container="):
        agent_mod._assert_container_resolvable(agent_mod.DEFAULT_CONTAINER, Runtime.DOCKER, False, local_home=False)
