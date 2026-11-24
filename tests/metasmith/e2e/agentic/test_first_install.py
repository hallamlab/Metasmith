from __future__ import annotations

from pathlib import Path

import pytest

from tests.metasmith.e2e.agentic import first_install as fi
from tests.metasmith.e2e.agentic.hosts import HOSTS, run_tag


def test_run_tag_keys_host() -> None:
    assert run_tag(4, "sockeye") == "run-4-sockeye"


def test_hosts_do_not_collide() -> None:
    assert len({h.gui_port for h in HOSTS.values()}) == len(HOSTS)
    assert "gcc/9.4.0" in HOSTS["sockeye"].modules
    assert HOSTS["fir"].login_nodes


@pytest.mark.parametrize("subset,needle", [(None, "default targets"), ("keep", "keeping only"), ("drop", "deselecting the targets listed")])
def test_prompt_names_host_templates_and_subset(subset, needle) -> None:
    h = HOSTS["fir"]
    text = fi.build_prompt("gui", Path("/sb"), "/scratch/x/run-4-fir", "sra", h,
                           "engcyc_prep_references", "engcyc_metagenomics", subset)
    assert "`ssh fir`" in text and "port 8092" in text
    assert "engcyc_prep_references workflow" in text and "engcyc_metagenomics workflow" in text
    assert needle in text
    assert "TARGETS.txt" in text if subset else "TARGETS.txt" not in text


def test_read_subset_drops_comments(tmp_path: Path) -> None:
    f = tmp_path / "t.txt"
    f.write_text("# c\ntaxonomy::gtdbtk\n\n sequences::orfs \n")
    assert fi.read_subset(f) == "taxonomy::gtdbtk\nsequences::orfs\n"
