"""The key Groovy obtains for a member is the key Python mints for it.

There is one key implementation, `metasmith.caching.invocation`, and the
Orchestrator reaches it by running it. This test runs the Orchestrator under
the dev image's Nextflow on a fixed set of members and compares what it prints
to what the module returns in-process.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from metasmith.constants import MODULE_PATH
from metasmith.models.workflow.payload import build_entry

pytestmark = [pytest.mark.docker, pytest.mark.slow]

ORCHESTRATOR_SRC = MODULE_PATH / "nextflow_config/Orchestrator.groovy"

TK, SIG = "cf::tr", "sig-1"
SLOTS = ["seed", "ref"]


def _members() -> list[dict]:
    return [
        build_entry([
            ("seed", [("/w/1/s1.txt", {"seed": ["id-s1"], "root": ["r0"]})]),
            ("ref", [("/w/1/db", {"ref": ["id-db"]})]),
        ]),
        build_entry([
            ("seed", [("/w/1/s2.txt", {"seed": ["id-s2"], "root": ["r0"]})]),
            ("ref", [("/w/1/db", {"ref": ["id-db"]})]),
        ]),
        build_entry([  # no own id: uncacheable
            ("seed", [("/w/1/s3.txt", {"root": ["r0"]})]),
            ("ref", [("/w/1/db", {"ref": ["id-db"]})]),
        ]),
    ]


MAIN_NF = '''
workflow {
    def o = new Orchestrator(Channel.fromList([null]))
    def spec = new groovy.json.JsonSlurper().parse(new File("spec.json"))
    def helper = ["python", "-m", "metasmith.caching.invocation"]
    def rows = o.probeMembers(helper, spec)
    rows.each { row -> println("MEMBER " + row.join("|")) }
}
'''


def test_groovy_and_python_agree_on_every_member_key(tmp_path, docker_image):
    try:
        from metasmith.caching import invocation as inv
    except ImportError:
        pytest.fail("metasmith.caching.invocation does not exist: the key has no single implementation")
    members = _members()
    expected = []
    for m in members:
        consumed = inv.consumed_of(m, SLOTS)
        expected.append(inv.member_key(TK, SIG, consumed).hex() if consumed is not None else "-")

    (tmp_path / "lib").mkdir()
    shutil.copy(ORCHESTRATOR_SRC, tmp_path / "lib" / "Orchestrator.groovy")
    (tmp_path / "main.nf").write_text(MAIN_NF)
    (tmp_path / "spec.json").write_text(json.dumps({
        "tk": TK, "sig": SIG, "slk": SLOTS,
        "cache_root": str(tmp_path / "cache"), "members": members,
    }))
    res = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{tmp_path}:{tmp_path}", "-w", str(tmp_path),
         docker_image, "nextflow", "run", "main.nf", "-lib", "./lib", "-ansi-log", "false"],
        capture_output=True, text=True, timeout=600,
    )
    assert res.returncode == 0, res.stdout[-3000:] + res.stderr[-3000:]
    rows = [l.split(" ", 1)[1].split("|") for l in res.stdout.splitlines() if l.startswith("MEMBER ")]
    assert len(rows) == len(members), res.stdout
    got = [r[0] for r in rows]
    assert got == expected, f"groovy keys {got} != python keys {expected}"
    assert [r[1] for r in rows] == ["miss", "miss", "-"], rows
