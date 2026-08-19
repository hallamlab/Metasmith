from pathlib import Path

import yaml

from metasmith.agents import Agent
from metasmith.env import Runtime
from metasmith.models.remote import Source


def _legacy_yaml(tmp_path: Path, *, runtime: str, container: str) -> Path:
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
    assert agent.runtime == Runtime.APPTAINER
    assert agent.container == "docker://quay.io/hallamlab/metasmith:9.9.9-deadbee"


def test_legacy_docker_loads(tmp_path):
    p = _legacy_yaml(
        tmp_path,
        runtime="DOCKER",
        container="docker://quay.io/hallamlab/metasmith:9.9.9-deadbee",
    )
    agent = Agent.Load(p)
    assert agent.runtime == Runtime.DOCKER
    assert agent.container == "docker://quay.io/hallamlab/metasmith:9.9.9-deadbee"


def test_pack_round_trip_preserves_runtime_and_container(tmp_path):
    agent = Agent(
        home=Source.FromLocal(tmp_path),
        runtime=Runtime.DOCKER,
        container="docker://quay.io/hallamlab/metasmith:9.9.9-deadbee",
    )
    packed = agent.Pack()
    assert packed["runtime"] == "DOCKER"
    assert packed["container"] == "docker://quay.io/hallamlab/metasmith:9.9.9-deadbee"

    reloaded = Agent.Unpack(agent.Pack())
    assert reloaded.runtime == Runtime.DOCKER
    assert reloaded.container == agent.container


def test_legacy_file_without_native_defaults_false(tmp_path):
    p = _legacy_yaml(tmp_path, runtime="APPTAINER", container="docker://x:1")
    agent = Agent.Load(p)
    assert agent.native is False


def test_mamba_native_round_trip(tmp_path):
    from metasmith.env import Runtime
    agent = Agent(
        home=Source.FromLocal(tmp_path),
        runtime=Runtime.MAMBA,
        native=True,
    )
    packed = agent.Pack()
    assert packed["runtime"] == "MAMBA"
    assert packed["native"] is True

    reloaded = Agent.Unpack(agent.Pack())
    assert reloaded.runtime == Runtime.MAMBA
    assert reloaded.native is True
    assert reloaded._environment().needs_relay is False


def test_legacy_file_without_default_preset_loads(tmp_path):
    p = _legacy_yaml(tmp_path, runtime="APPTAINER", container="docker://x:1")
    agent = Agent.Load(p)
    assert agent.default_preset is None


def test_an_agent_that_names_no_preset_writes_no_key(tmp_path):
    agent = Agent(home=Source.FromLocal(tmp_path))
    assert "default_preset" not in agent.Pack()


def test_default_preset_round_trips(tmp_path):
    agent = Agent(home=Source.FromLocal(tmp_path), default_preset="slurm")
    assert agent.Pack()["default_preset"] == "slurm"
    assert Agent.Unpack(agent.Pack()).default_preset == "slurm"


def test_legacy_file_without_default_params_loads(tmp_path):
    p = _legacy_yaml(tmp_path, runtime="APPTAINER", container="docker://x:1")
    agent = Agent.Load(p)
    assert agent.default_params == {}


def test_an_agent_with_no_params_writes_no_key(tmp_path):
    agent = Agent(home=Source.FromLocal(tmp_path))
    assert "default_params" not in agent.Pack()


def test_default_params_round_trip_as_a_mapping(tmp_path):
    agent = Agent(
        home=Source.FromLocal(tmp_path),
        default_params={"slurmAccount": "st-you-1", "process_tries": 3},
    )
    p = tmp_path / "agent.yml"
    agent.Save(p)
    packed = yaml.safe_load(p.read_text())["default_params"]
    assert packed == {"slurmAccount": "st-you-1", "process_tries": 3}
    assert Agent.Load(p).default_params == packed
