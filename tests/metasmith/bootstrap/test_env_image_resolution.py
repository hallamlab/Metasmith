import pytest

from metasmith.env import Runtime
from metasmith.models.libraries import ResolveEnvImage

GENERIC = "container: docker://quay.io/biocontainers/diamond:2.1.8--h43eeafb_0\nconda: diamond\n"
CONTAINER_ONLY = "container: docker://quay.io/biocontainers/diamond:2.1.8--h43eeafb_0\n"
CONDA_ONLY = "conda: diamond\n"
LEGACY_URI = "docker://quay.io/biocontainers/diamond:2.1.8--h43eeafb_0\n"


def test_docker_selects_container():
    assert ResolveEnvImage(GENERIC, Runtime.DOCKER) == \
        "docker://quay.io/biocontainers/diamond:2.1.8--h43eeafb_0"


def test_apptainer_selects_container():
    assert ResolveEnvImage(GENERIC, Runtime.APPTAINER) == \
        "docker://quay.io/biocontainers/diamond:2.1.8--h43eeafb_0"


def test_mamba_selects_conda():
    assert ResolveEnvImage(GENERIC, Runtime.MAMBA) == "diamond"


def test_missing_conda_under_mamba_errors():
    with pytest.raises(AssertionError) as e:
        ResolveEnvImage(CONTAINER_ONLY, Runtime.MAMBA, "diamond.env")
    assert "conda" in str(e.value) and "diamond.env" in str(e.value)


def test_missing_container_under_docker_errors():
    with pytest.raises(AssertionError) as e:
        ResolveEnvImage(CONDA_ONLY, Runtime.DOCKER, "diamond.env")
    assert "container" in str(e.value)


def test_legacy_bare_uri_resolves_verbatim_for_container_runtimes():
    for rt in (Runtime.DOCKER, Runtime.APPTAINER):
        assert ResolveEnvImage(LEGACY_URI, rt) == \
            "docker://quay.io/biocontainers/diamond:2.1.8--h43eeafb_0"
