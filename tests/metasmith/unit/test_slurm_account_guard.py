from __future__ import annotations

from metasmith.agents.workflow_ops import GetNxfConfigPresets, SlurmAccountProblem, SplitParamKeys


def _slurm() -> str:
    return GetNxfConfigPresets()["slurm"].read_text()


def test_the_shipped_slurm_preset_alone_is_refused():
    assert "slurmAccount" in SlurmAccountProblem(_slurm(), {})


def test_a_real_account_param_passes():
    assert SlurmAccountProblem(_slurm(), {"slurmAccount": "def-hallam"}) is None


def test_the_examples_count_as_placeholders():
    for placeholder in ("", "  ", "st-you-1", "<slurm_account>"):
        assert SlurmAccountProblem(_slurm(), {"slurmAccount": placeholder})


def test_a_preset_edited_to_carry_the_account_passes():
    text = _slurm().replace("slurmAccount = '<slurm_account>'", "slurmAccount = 'def-hallam'")
    assert SlurmAccountProblem(text, {}) is None


def test_cluster_options_replace_the_account():
    given = SplitParamKeys({"process_clusterOptions": "--account=def-hallam --nodes=1"})
    assert SlurmAccountProblem(_slurm(), given) is None
    given = SplitParamKeys({"process_clusterOptions": "--account=st-you-1"})
    assert SlurmAccountProblem(_slurm(), given)


def test_the_local_preset_needs_no_account():
    assert SlurmAccountProblem(GetNxfConfigPresets()["local"].read_text(), {}) is None
