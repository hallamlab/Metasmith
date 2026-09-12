import pytest

import metasmith.coms.api as api_mod
from metasmith.coms.api import Api
from metasmith.models.libraries import ExecutionResult


@pytest.fixture
def _body():
    return {"workspace": "/ws/runs/K", "step_index": "1", "host": "h"}


@pytest.mark.parametrize("success, expected", [(True, 0), (False, 1)])
def test_execute_transform_exit_status_follows_the_result(monkeypatch, _body, success, expected):
    monkeypatch.setattr(
        api_mod, "StageAndRunTransform",
        lambda *a, **k: ExecutionResult(success),
    )
    with pytest.raises(SystemExit) as e:
        Api().execute_transform(_body)
    assert e.value.code == expected


def test_a_successful_step_does_not_exit_truthy(monkeypatch, _body):
    monkeypatch.setattr(api_mod, "StageAndRunTransform", lambda *a, **k: ExecutionResult(True))
    with pytest.raises(SystemExit) as e:
        Api().execute_transform(_body)
    assert e.value.code == 0
    assert not isinstance(e.value.code, bool)
