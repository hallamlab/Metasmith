"""A run param nothing reads is said to be unread, by name."""

from __future__ import annotations

from types import SimpleNamespace

from metasmith.agents.workflow_ops import _plan_param_keys, _unread_params
from metasmith.env.dispatch_scan import ScanSource

PRESET = "process { queue = params.partition }\nexecutor { cpus = params.executor.cpus }\n"


def _task(*sources: str):
    steps = [SimpleNamespace(transform=SimpleNamespace(_env_scan=ScanSource(s))) for s in sources]
    return SimpleNamespace(plan=SimpleNamespace(steps=steps))


def test_a_typo_is_unread_and_a_real_key_is_not():
    keys = _plan_param_keys(_task("x = context.params.get('long_read_platform', 'ont')\n"))
    given = {"long_read_platfrom": "pacbio", "long_read_platform": "pacbio", "partition": "x"}
    assert _unread_params(given, keys, PRESET) == ["long_read_platfrom"]


def test_engine_keys_and_both_spellings_of_a_nested_key_are_read():
    keys = _plan_param_keys(_task("x = context.params['gtdbtk_pplacer_cpus']\n"))
    given = {"executor": {"cpus": 4}, "process_array": 0, "gtdbtk": {"pplacer": {"cpus": 8}},
             "gtdbtk_pplacer_cpus": 8, "slurmAccount": "def-x"}
    assert _unread_params(given, keys, PRESET) == []


def test_a_key_read_through_a_variable_makes_the_plan_unlistable():
    assert _plan_param_keys(_task("k = 'a'\nx = context.params.get(k)\n")) is None


def test_an_f_string_key_reads_every_key_with_its_prefix():
    keys = _plan_param_keys(_task('x = context.params.get(f"dram_{k}_url", u)\n'))
    assert _unread_params({"dram_kegg_url": "u", "dran_kegg_url": "u"}, keys, PRESET) == ["dran_kegg_url"]
