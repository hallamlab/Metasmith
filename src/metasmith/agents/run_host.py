"""Shell functions that answer questions about a run on the host that launched it.

A cluster's login nodes share the filesystem but not their process tables, and an
ssh to the cluster lands on whichever node round-robin DNS picks. A pid in
PID.lock or RUN.pgid only means something on the node in RUN.host, so every
liveness check, kill and scan goes there. A run whose driver is a Slurm job also
has RUN.slurmjob, which answers when that node cannot be reached.
"""
from __future__ import annotations

from ..constants import AgentPaths

HOP_CONNECT_TIMEOUT_S = 10

# Each function runs from the run's workspace. A state is LIVE, GONE or UNKNOWN:
# UNKNOWN means the question could not be asked, and must never be read as GONE.
RUN_HOST_FUNCTIONS = rf"""
msm_on_run_host() {{
    _msm_h=$(head -n1 ./{AgentPaths.RUN_HOST_FILE} 2>/dev/null)
    if [ -z "$_msm_h" ] || [ "$_msm_h" = "$(hostname)" ]; then
        bash -c "$1"
    else
        # -n: a script fed to bash on stdin, as a LiveShell does, would otherwise lose
        # everything after this line to the hop.
        ssh -n -o BatchMode=yes -o ConnectTimeout={HOP_CONNECT_TIMEOUT_S} "$_msm_h" "$1"
    fi
}}
msm_slurm_state() {{
    _msm_j=$(head -n1 ./{AgentPaths.RUN_SLURM_JOB_FILE} 2>/dev/null)
    [ -n "$_msm_j" ] || {{ echo UNKNOWN; return; }}
    for _msm_s in "$(squeue -h -j "$_msm_j" -o %T 2>/dev/null | head -n1)" \
                  "$(sacct -n -X -j "$_msm_j" -o State 2>/dev/null | head -n1 | tr -d ' ')"; do
        case "$_msm_s" in
            PENDING*|RUNNING*|REQUEUED*|RESIZING*|SUSPENDED*|CONFIGURING*|COMPLETING*|STAGE_OUT*|SIGNALING*)
                echo LIVE; return ;;
            [A-Z]*) echo GONE; return ;;
        esac
    done
    echo UNKNOWN
}}
msm_pgid_state() {{
    _msm_o=$(msm_on_run_host "kill -0 -$1 2>/dev/null && echo LIVE || echo GONE")
    _msm_rc=$?
    if [ "$_msm_rc" != 255 ] && [ -n "$_msm_o" ]; then echo "$_msm_o"; return; fi
    if [ -s ./{AgentPaths.RUN_SLURM_JOB_FILE} ]; then msm_slurm_state; return; fi
    echo UNKNOWN
}}
msm_group_state() {{
    [ -s "$1" ] || {{ echo NONE; return; }}
    _msm_g=$(head -n1 "$1")
    case "$_msm_g" in
        ''|*[!0-9]*) echo NONE ;;
        *) msm_pgid_state "$_msm_g" ;;
    esac
}}
"""


def OnRunHost(script: str, workspace) -> str:
    """Wrap `script` to run whole on the run's host, printing `UNREACHABLE|<host>` when it cannot."""
    return "\n".join([
        f'_msm_h=$(head -n1 "{workspace}/{AgentPaths.RUN_HOST_FILE}" 2>/dev/null)',
        'if [ -n "$_msm_h" ] && [ "$_msm_h" != "$(hostname)" ]; then',
        f'    ssh -o BatchMode=yes -o ConnectTimeout={HOP_CONNECT_TIMEOUT_S} "$_msm_h" bash -s <<\'__MSM_ON_RUN_HOST__\'',
        script,
        "__MSM_ON_RUN_HOST__",
        '    [ $? = 255 ] && echo "UNREACHABLE|$_msm_h"',
        "else",
        script,
        "fi",
    ])


# Refuses a launch while either of a run's process groups is alive, wherever it runs:
# a second nextflow in the same workspace exits on its own lock, and its exit removed
# the first run's PID.lock, which made the first run's supervisor kill its own run.
LAUNCH_GUARD = RUN_HOST_FUNCTIONS + rf"""
for _msm_f in ./{AgentPaths.PID_LOCK_FILE} ./{AgentPaths.RUN_PGID_FILE}; do
    case "$(msm_group_state $_msm_f)" in
        LIVE)
            echo "a run is still alive in this workspace ($_msm_f names a live process group on [$(head -n1 ./{AgentPaths.RUN_HOST_FILE} 2>/dev/null)]); cancel it before launching again" >&2
            exit 3 ;;
        UNKNOWN)
            echo "cannot tell whether the run named in $_msm_f is alive: its host [$(head -n1 ./{AgentPaths.RUN_HOST_FILE} 2>/dev/null)] does not answer; cancel it, or remove $_msm_f if that host is gone" >&2
            exit 3 ;;
    esac
done
"""
