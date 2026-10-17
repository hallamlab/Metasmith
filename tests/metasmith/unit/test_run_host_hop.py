import os
import subprocess

from metasmith.agents.run_host import RUN_HOST_FUNCTIONS
from metasmith.constants import AgentPaths

# Stands in for ssh as it behaves: without -n it reads its stdin to the end.
FAKE_SSH = """#!/bin/bash
for a; do [ "$a" = -n ] && { echo HOPPED; exit 0; }; done
cat > /dev/null
echo HOPPED
"""


def test_a_hop_leaves_the_rest_of_a_script_fed_on_stdin(tmp_path):
    # A LiveShell feeds its remote bash on stdin. On fir, a hop without -n swallowed
    # every line after it, so a probe never printed its END and waited out its timeout.
    bin_dir = tmp_path/"bin"
    bin_dir.mkdir()
    (bin_dir/"ssh").write_text(FAKE_SSH)
    (bin_dir/"ssh").chmod(0o755)
    (tmp_path/AgentPaths.RUN_HOST_FILE).write_text("some-other-login-node\n")
    script = f"{RUN_HOST_FUNCTIONS}\nmsm_on_run_host true\necho AFTER\n"
    res = subprocess.run(
        ["bash", "-s"], input=script, capture_output=True, text=True, cwd=tmp_path,
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}, timeout=30,
    )
    assert res.stdout.split() == ["HOPPED", "AFTER"]
