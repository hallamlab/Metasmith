"""Shared pytest fixtures for Metasmith tests."""

import pytest
from metasmith.models.solver import Transform, Endpoint


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
