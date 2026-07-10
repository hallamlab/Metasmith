"""Shared pytest fixtures for Metasmith tests."""

import sys
from pathlib import Path

import pytest

# Ensure tests import this checkout's source tree first.
SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from metasmith.models.solver import Transform, Endpoint


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "network: requires LIVESHELL_REMOTE_HOST (ssh-reachable host) env var",
    )


@pytest.fixture
def make_transform():
    """Factory fixture to create Transform objects."""
    def _make_transform():
        return Transform()
    return _make_transform


@pytest.fixture
def make_endpoint():
    """Factory fixture to create Endpoint objects."""
    def _make_endpoint(properties):
        return Endpoint(properties=properties)
    return _make_endpoint
