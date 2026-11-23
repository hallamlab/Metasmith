from pathlib import Path

import pytest
import yaml

from metasmith.agents import Agent
from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.remote import Source

pytestmark = pytest.mark.docker


def _assert_agent_structure(home: Path):
    assert (home / "msm").exists(), "msm launcher should exist"
    assert AgentPaths.to_bootstrap(home).exists(), "msm_bootstrap should exist"
    assert AgentPaths.to_definition(home).exists(), "agent.yml should exist"
    with open(AgentPaths.to_definition(home)) as f:
        data = yaml.safe_load(f)
    assert {"home", "container", "runtime"} <= set(data)


def test_deploy_creates_the_agent_structure_and_a_redeploy_keeps_it(local_agent_home, docker_image):
    agent = Agent(
        home=Source.FromLocal(local_agent_home),
        container=docker_image,
        runtime=Runtime.DOCKER,
    )
    agent.Deploy(assertive=True)
    _assert_agent_structure(local_agent_home)
    agent.Deploy(assertive=False)
    _assert_agent_structure(local_agent_home)
