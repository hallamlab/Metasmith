"""Characterization of the `agent.yml` on-disk format (the deploy contract).

`agent.yml` is what a deployed agent home carries; a silent format break
strands already-deployed homes. Today the format is top-level `runtime`
(enum name) + `container` (image uri). The upcoming refactor moves runtime
selection into an `Environment` written at `Agent.Deploy(...)`; these tests
pin the *current* shape so the migration path (legacy load) stays guarded.

After the refactor: a hand-authored legacy file like the one below MUST
still load into an equivalent `Agent`.
"""

from pathlib import Path

import yaml

from metasmith.agents import Agent
from metasmith.coms.containers import ContainerRuntime
from metasmith.models.remote import Source


def _legacy_yaml(tmp_path: Path, *, runtime: str, container: str) -> Path:
    """A minimal `agent.yml` in today's format, authored by hand (not Pack)."""
    home = Source.FromLocal(tmp_path)
    data = {
        "setup_commands": [],
        "home": home.Pack(),
        "container": container,
        "runtime": runtime,
    }
    p = tmp_path / "agent.yml"
    p.write_text(yaml.dump(data))
    return p


def test_legacy_apptainer_loads(tmp_path):
    p = _legacy_yaml(
        tmp_path,
        runtime="APPTAINER",
        container="docker://quay.io/hallamlab/metasmith:9.9.9-deadbee",
    )
    agent = Agent.Load(p)
    assert agent.runtime == ContainerRuntime.APPTAINER
    assert agent.container == "docker://quay.io/hallamlab/metasmith:9.9.9-deadbee"


def test_legacy_docker_loads(tmp_path):
    p = _legacy_yaml(
        tmp_path,
        runtime="DOCKER",
        container="docker://quay.io/hallamlab/metasmith:9.9.9-deadbee",
    )
    agent = Agent.Load(p)
    assert agent.runtime == ContainerRuntime.DOCKER
    assert agent.container == "docker://quay.io/hallamlab/metasmith:9.9.9-deadbee"


def test_pack_round_trip_preserves_runtime_and_container(tmp_path):
    agent = Agent(
        home=Source.FromLocal(tmp_path),
        runtime=ContainerRuntime.DOCKER,
        container="docker://quay.io/hallamlab/metasmith:9.9.9-deadbee",
    )
    packed = agent.Pack()
    # The serialized form is enum *name* for runtime, raw uri for container.
    assert packed["runtime"] == "DOCKER"
    assert packed["container"] == "docker://quay.io/hallamlab/metasmith:9.9.9-deadbee"

    reloaded = Agent.Unpack(agent.Pack())
    assert reloaded.runtime == ContainerRuntime.DOCKER
    assert reloaded.container == agent.container
