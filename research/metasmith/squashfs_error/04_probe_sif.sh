#!/bin/bash
# Runs ON the target host. Walks a ladder of ways to execute the shipped SIF,
# with the sandbox arm deliberately unavailable, and reports which rung (if any)
# wedges on the FUSE rootfs reader.
#
#   ./04_probe_sif.sh <AGENT_HOME> [SIF]
#
# env: TMO   per-stage patience in seconds (default 180)
#      FANOUT  concurrency for the last stage (default 8)
#
# The rungs differ only in what the container launch is a descendant of, which
# is the whole hypothesis: `squashfuse_ll` is a sibling of the container payload
# under apptainer's starter, so its lifecycle is entangled with whatever process
# group launched it.
#
#   1  bare        apptainer exec, straight from the login shell
#   2  wrapper     the deployed `msm` script (same, plus the agent's binds)
#   3  relay       the launch is dispatched by the relay daemon (fork + SIGCHLD
#                  ignored + nohup'd launcher) instead of by our shell
#   4  nested      a container bounces out through the relay to launch another
#                  container — the production shape, two readers alive at once
#   5  fanout      N of rung 4 at once, because the reported wedge was flaky
#
# A stage that overruns TMO is NOT killed before its evidence is taken: the one
# datum that separates a stopped reader from a deadlocked one is the reader's
# STAT plus its per-thread wchan, and both are gone once the process is reaped.
set -uo pipefail

AGENT_HOME=${1:?usage: 04_probe_sif.sh <AGENT_HOME> [SIF]}
SIF=${2:-}
TMO=${TMO:-180}
FANOUT=${FANOUT:-8}

HOST=$(hostname)
LOGS="$AGENT_HOME/_probe_logs"
mkdir -p "$LOGS"
RELAY="$AGENT_HOME/relay/msm_relay"
RELAY_WS="$AGENT_HOME/relay/$HOST"

if [ -z "$SIF" ]; then
    SIF=$(ls -1 "$AGENT_HOME"/container_images/*.sif 2>/dev/null | head -1)
fi
[ -e "$SIF" ] || { echo "FATAL: no sif found (looked in $AGENT_HOME/container_images)"; exit 1; }
SANDBOX="${SIF%.sif}.sandbox"

pass=0; fail=0; hung=0

diag() {
    echo "    ### fuse connections (waiting>0 means the reader is not answering)"
    for d in /sys/fs/fuse/connections/*/; do
        [ -e "$d/waiting" ] || continue
        echo "      $(basename "$d"): waiting=$(cat "$d/waiting" 2>/dev/null) congestion_threshold=$(cat "$d/congestion_threshold" 2>/dev/null)"
    done
    echo "    ### processes of interest"
    ps -eo pid,ppid,pgid,sid,stat,wchan:24,etime,args \
        | grep -Ei 'squashfuse|apptainer|starter|msm_relay|metasmith' | grep -v grep | sed 's/^/      /'
    echo "    ### squashfuse_ll per-thread state"
    for p in $(pgrep -x squashfuse_ll 2>/dev/null; pgrep -f squashfuse_ll 2>/dev/null | sort -u); do
        [ -d "/proc/$p" ] || continue
        echo "      pid $p stat=$(awk '{print $3}' "/proc/$p/stat" 2>/dev/null) cmd=$(tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null)"
        for t in /proc/"$p"/task/*; do
            echo "        tid $(basename "$t") stat=$(awk '{print $3}' "$t/stat" 2>/dev/null) wchan=$(cat "$t/wchan" 2>/dev/null)"
        done
    done
    echo "    ### uninterruptible (D) tasks"
    ps -eo pid,stat,wchan:28,args | awk '$2 ~ /D/ {print "      "$0}'
}

# Run a payload, wait up to TMO, and if it is still alive take the diagnostics
# BEFORE killing it.
stage() {
    local name=$1; shift
    local log="$LOGS/$name.log"
    echo ""
    echo "== stage [$name] =================================================="
    echo "   \$ $*" | head -3
    local t0 rc elapsed
    t0=$(date +%s)
    setsid bash -c "$*" >"$log" 2>&1 &
    local pid=$!
    local pgid=$pid
    while kill -0 "$pid" 2>/dev/null; do
        elapsed=$(( $(date +%s) - t0 ))
        if [ "$elapsed" -ge "$TMO" ]; then
            echo "   !! STILL RUNNING after ${elapsed}s -- taking evidence, then killing"
            diag
            kill -TERM -"$pgid" 2>/dev/null
            sleep 3
            kill -KILL -"$pgid" 2>/dev/null
            wait "$pid" 2>/dev/null
            hung=$((hung+1))
            echo "   VERDICT: HUNG  (log tail below)"
            tail -15 "$log" | sed 's/^/      /'
            return 99
        fi
        sleep 2
    done
    wait "$pid"; rc=$?
    elapsed=$(( $(date +%s) - t0 ))
    if [ "$rc" -eq 0 ]; then
        pass=$((pass+1))
        echo "   VERDICT: ok in ${elapsed}s"
    else
        fail=$((fail+1))
        echo "   VERDICT: FAILED rc=$rc in ${elapsed}s"
    fi
    if grep -q "usage: metasmith" "$log"; then
        echo "   (metasmith help present)"
    fi
    tail -6 "$log" | sed 's/^/      /'
    return "$rc"
}

echo "################ host facts ################"
echo "host          : $HOST"
echo "kernel        : $(uname -r)"
echo "/proc/version : $(cat /proc/version)"
APPTAINER_BIN=$(readlink -f "$(command -v apptainer)")
echo "apptainer     : $(apptainer --version)  at $APPTAINER_BIN"
SUID="$(dirname "$APPTAINER_BIN")/../libexec/apptainer/bin/starter-suid"
if [ -u "$SUID" ]; then echo "starter-suid  : PRESENT and setuid -> kernel squashfs mount, no FUSE"
elif [ -e "$SUID" ]; then echo "starter-suid  : present but NOT setuid -> FUSE reader"
else echo "starter-suid  : absent -> FUSE reader"; fi
SQ="$(dirname "$APPTAINER_BIN")/../libexec/apptainer/bin/squashfuse_ll"
echo "squashfuse_ll : $([ -e "$SQ" ] && echo "$SQ" || echo absent)"
echo "fusermount3   : $(command -v fusermount3 || echo absent)"
echo "libfuse       : $(ldconfig -p 2>/dev/null | grep -o 'libfuse3\.so[^ ]*' | head -2 | tr '\n' ' ')"
echo "sif           : $SIF ($(stat -c %s "$SIF") bytes)"
if [ -e "$SANDBOX" ]; then
    echo "sandbox       : PRESENT at $SANDBOX -- the run command's ternary would prefer it"
    if mountpoint -q "$SANDBOX"; then echo "                (and it is a live mount)"; fi
    echo "                removing it so this run cannot silently take the sandbox arm"
    mountpoint -q "$SANDBOX" && fusermount3 -u "$SANDBOX" 2>/dev/null
    rm -rf "$SANDBOX"
fi
echo "sandbox       : absent -> forced SIF"
echo "fuse conns at start:"
for d in /sys/fs/fuse/connections/*/; do [ -e "$d/waiting" ] && echo "  $(basename "$d") waiting=$(cat "$d/waiting")"; done

echo ""
echo "################ ladder ################"

stage 1_bare "apptainer exec '$SIF' metasmith --help"

stage 2_wrapper "'$AGENT_HOME/msm' --help"

# The relay daemon must be up for rungs 3-5. Never capture its stdout: the
# forked daemon inherits the pipe and never closes it, so a captured `start`
# blocks forever.
echo ""
echo "== starting relay daemon =="
( cd "$AGENT_HOME" && ./relay/msm_relay start >"$LOGS/relay_start.log" 2>&1 </dev/null )
"$RELAY" --io "$RELAY_WS" status || true

stage 3_relay "'$RELAY' --io '$RELAY_WS' bounce \"apptainer exec '$SIF' metasmith --help\""

NESTED_BOUNCE="/app/msm_relay.x86_64-linux --io /msm_home/relay/$HOST bounce \\\"apptainer exec '$SIF' metasmith --help\\\""
stage 4_nested "apptainer exec --bind '$AGENT_HOME':/msm_home --bind /tmp:/tmp '$SIF' bash -c \"$NESTED_BOUNCE\""

FAN=""
for i in $(seq 1 "$FANOUT"); do
    FAN="$FAN apptainer exec --bind '$AGENT_HOME':/msm_home --bind /tmp:/tmp '$SIF' bash -c \"$NESTED_BOUNCE\" >/dev/null 2>&1 &"
done
stage 5_fanout "$FAN wait"

echo ""
echo "################ summary ################"
echo "ok=$pass failed=$fail hung=$hung   (logs in $LOGS)"
echo "fuse conns at end:"
for d in /sys/fs/fuse/connections/*/; do [ -e "$d/waiting" ] && echo "  $(basename "$d") waiting=$(cat "$d/waiting")"; done
[ "$hung" -eq 0 ] && [ "$fail" -eq 0 ] && echo "RESULT: no wedge reproduced on forced-SIF" || echo "RESULT: something did not complete -- see above"
