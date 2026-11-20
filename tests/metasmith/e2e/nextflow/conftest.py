import pytest

from tests.metasmith.e2e.nextflow.harness import Workspace, host_nextflow


@pytest.fixture
def ws(tmp_path) -> Workspace:
    if host_nextflow() is None:
        pytest.skip("no nextflow on PATH or beside this python; run under `mamba run -n msm`")
    return Workspace(tmp_path / "ws")
