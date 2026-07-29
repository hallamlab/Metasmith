"""Regression tests for the source-controlled container tag.

`Agent.container` defaults to the image pushed to quay by the release.
The wiring is:
    version.txt   (semver only)         → constants.VERSION
    build_hash.txt (content hash)       → constants.BUILD_HASH
                                            ↓
                              constants.FULL_VERSION  ("0.18.2+abc1234", canonical)
                                            ↓ (single + → - site)
                              constants.CONTAINER_TAG ("0.18.2-abc1234", Docker form)
                                            ↓
                              Agent.container default

If any link drifts, fresh deploys can pull a stale or non-existent tag.
These tests pin the chain.
"""
import re
from pathlib import Path

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
    """version.txt holds only the PEP 440 release segment — no '+', no '-'."""
    raw = (MODULE_PATH / "version.txt").read_text().strip()
    assert VERSION == raw
    assert "+" not in VERSION, "version.txt must not contain a build hash; that's build_hash.txt's job"
    assert re.fullmatch(r"\d+\.\d+\.\d+", VERSION), \
        f"version.txt should be MAJOR.MINOR.PATCH, got [{VERSION}]"


def test_build_hash_when_present_is_short_hex():
    """If build_hash.txt is present, it should be a short hex string."""
    if BUILD_HASH:
        assert re.fullmatch(r"[0-9a-f]+", BUILD_HASH), \
            f"build_hash.txt should be hex, got [{BUILD_HASH}]"


def test_full_version_composition():
    """FULL_VERSION is VERSION+BUILD_HASH (PEP 440 local form) when hash
    is set, or bare VERSION otherwise."""
    expected = f"{VERSION}+{BUILD_HASH}" if BUILD_HASH else VERSION
    assert FULL_VERSION == expected


def test_container_tag_is_full_version_with_plus_translated():
    """CONTAINER_TAG is the single +→- translation site."""
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
    """get_full_version() / get_docker_tag() in testing.docker_builder must
    produce the same tag Agent.container expects."""
    from metasmith.testing.docker_builder import get_docker_tag, get_full_version

    assert get_full_version() == FULL_VERSION
    assert get_docker_tag() == f"quay.io/hallamlab/metasmith:{CONTAINER_TAG}"


def test_build_hash_is_deterministic():
    """compute_build_hash is stable across calls on an unchanged source tree."""
    h1 = compute_build_hash()
    h2 = compute_build_hash()
    assert h1 == h2
    assert re.fullmatch(r"[0-9a-f]{7}", h1), f"expected 7-char hex, got [{h1}]"
