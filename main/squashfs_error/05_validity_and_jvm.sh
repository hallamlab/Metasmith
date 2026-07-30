#!/bin/bash
# Runs ON the target host. Two jobs.
#
# (a) VALIDITY. A green rung in 04 is meaningless unless the exec really read
#     its rootfs through `squashfuse_ll`. Apptainer will silently convert a SIF
#     to a temporary sandbox when it cannot use FUSE, printing only an INFO
#     line, and then nothing about the run touches the code path under
#     suspicion. So: hold a container open and look for the reader process, the
#     /dev/fuse connection, and a `fuse.squashfuse` mount in the container's own
#     mountinfo.
#
# (b) THE JVM PAYLOAD. The original wedge was reported as a nextflow hang, and
#     the JVM is the heaviest rootfs reader in the image (thousands of small
#     reads across many threads). `metasmith --help` is a much gentler probe, so
#     repeat the relay rungs with `nextflow -version` before believing a
#     negative.
#
#   ./05_validity_and_jvm.sh <AGENT_HOME> [SIF]
set -uo pipefail

AGENT_HOME=${1:?usage: 05_validity_and_jvm.sh <AGENT_HOME> [SIF]}
SIF=${2:-}
TMO=${TMO:-240}
FANOUT=${FANOUT:-8}
export METASMITH_APPTAINER_ROOTFS=sif

HOST=$(hostname)
LOGS="$AGENT_HOME/_probe_logs"
mkdir -p "$LOGS"
RELAY="$AGENT_HOME/relay/msm_relay"
RELAY_WS="$AGENT_HOME/relay/$HOST"
[ -z "$SIF" ] && SIF=$(ls -1 "$AGENT_HOME"/container_images/*.sif 2>/dev/null | head -1)
[ -e "$SIF" ] || { echo "FATAL: no sif"; exit 1; }
SANDBOX="${SIF%.sif}.sandbox"
[ -e "$SANDBOX" ] && { echo "FATAL: sandbox present at $SANDBOX — this run would not be forced-SIF"; exit 1; }

if [ -n "${SKIP_VALIDITY:-}" ]; then
    echo "################ (a) validity: SKIPPED (SKIP_VALIDITY set) ################"
else
echo "################ (a) validity: is squashfuse_ll actually in the chain? ################"
# Hold a container open on the host, in the same shape metasmith uses.
apptainer exec --no-home --cleanenv "$SIF" bash -c 'cat /proc/self/mountinfo > /tmp/msm_probe_mountinfo; sleep 25' \
    >"$LOGS/validity_hold.log" 2>&1 &
HOLD=$!
sleep 8

echo "-- reader process"
ps -eo pid,ppid,pgid,sid,stat,etime,args | grep -E 'squashfuse|starter|apptainer' | grep -v grep | sed 's/^/   /'
echo "-- /dev/fuse connections"
found_conn=0
for d in /sys/fs/fuse/connections/*/; do
    [ -e "$d/waiting" ] || continue
    found_conn=1
    echo "   $(basename "$d"): waiting=$(cat "$d/waiting")"
done
[ "$found_conn" -eq 0 ] && echo "   (none)"
echo "-- container's own view of its rootfs"
grep -E 'fuse|squash| / ' /tmp/msm_probe_mountinfo 2>/dev/null | head -5 | sed 's/^/   /'
echo "-- apptainer INFO lines from that exec (a 'temporary sandbox' line invalidates everything)"
cat "$LOGS/validity_hold.log" | sed 's/^/   /'

SQ_SEEN=$(pgrep -c -f squashfuse_ll 2>/dev/null || echo 0)
wait $HOLD 2>/dev/null
echo ""
if [ "${SQ_SEEN:-0}" -gt 0 ]; then
    echo "VALIDITY: OK — a squashfuse_ll reader was serving the rootfs ($SQ_SEEN process(es))"
else
    echo "VALIDITY: FAILED — no squashfuse_ll seen; apptainer may have unpacked instead."
    echo "          Every green rung below (and in 04) is then about a path that was never taken."
fi
fi

echo ""
echo "################ (b) the JVM payload through the relay ################"
pass=0; hung=0; fail=0
diag() {
    echo "    ### fuse connections"
    for d in /sys/fs/fuse/connections/*/; do [ -e "$d/waiting" ] && echo "      $(basename "$d") waiting=$(cat "$d/waiting")"; done
    echo "    ### processes"
    ps -eo pid,ppid,pgid,sid,stat,wchan:24,etime,args | grep -Ei 'squashfuse|apptainer|starter|msm_relay|java|nextflow' | grep -v grep | sed 's/^/      /'
    echo "    ### squashfuse_ll per-thread state (stopped vs deadlocked)"
    for p in $(pgrep -f squashfuse_ll 2>/dev/null); do
        [ -d "/proc/$p" ] || continue
        echo "      pid $p stat=$(awk '{print $3}' "/proc/$p/stat")"
        for t in /proc/"$p"/task/*; do
            echo "        tid $(basename "$t") stat=$(awk '{print $3}' "$t/stat" 2>/dev/null) wchan=$(cat "$t/wchan" 2>/dev/null)"
        done
    done
    echo "    ### uninterruptible (D) tasks"
    ps -eo pid,stat,wchan:28,args | awk '$2 ~ /D/ {print "      "$0}'
}
stage() {
    local name=$1; shift
    local log="$LOGS/$name.log"
    echo ""
    echo "== stage [$name] =="
    local t0 rc elapsed
    t0=$(date +%s)
    setsid bash -c "$*" >"$log" 2>&1 &
    local pid=$!
    while kill -0 "$pid" 2>/dev/null; do
        elapsed=$(( $(date +%s) - t0 ))
        if [ "$elapsed" -ge "$TMO" ]; then
            echo "   !! STILL RUNNING after ${elapsed}s -- evidence first, then kill"
            diag
            kill -TERM -"$pid" 2>/dev/null; sleep 3; kill -KILL -"$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
            hung=$((hung+1)); echo "   VERDICT: HUNG"; tail -15 "$log" | sed 's/^/      /'
            return 99
        fi
        sleep 2
    done
    wait "$pid"; rc=$?
    elapsed=$(( $(date +%s) - t0 ))
    if [ "$rc" -eq 0 ]; then pass=$((pass+1)); echo "   VERDICT: ok in ${elapsed}s"
    else fail=$((fail+1)); echo "   VERDICT: FAILED rc=$rc in ${elapsed}s"; fi
    tail -4 "$log" | sed 's/^/      /'
}

( cd "$AGENT_HOME" && ./relay/msm_relay start >"$LOGS/relay_start.log" 2>&1 </dev/null )

stage 6_jvm_bare "apptainer exec --cleanenv --home /tmp '$SIF' nextflow -version"
stage 7_jvm_relay "'$RELAY' --io '$RELAY_WS' bounce \"apptainer exec --cleanenv --home /tmp '$SIF' nextflow -version\""

NESTED="/app/msm_relay.x86_64-linux --io /msm_home/relay/$HOST bounce \\\"apptainer exec --cleanenv --home /tmp '$SIF' nextflow -version\\\""
stage 8_jvm_nested "apptainer exec --bind '$AGENT_HOME':/msm_home --bind /tmp:/tmp '$SIF' bash -c \"$NESTED\""

FAN=""
for i in $(seq 1 "$FANOUT"); do
    FAN="$FAN apptainer exec --bind '$AGENT_HOME':/msm_home --bind /tmp:/tmp '$SIF' bash -c \"$NESTED\" >/dev/null 2>&1 &"
done
stage 9_jvm_fanout "$FAN wait"

echo ""
echo "################ summary ################"
echo "ok=$pass failed=$fail hung=$hung  (logs in $LOGS)"
# `msm_relay bounce` returns 0 whatever the dispatched command did -- it streams
# the child's output but does not propagate its exit code. So for the bounced
# rungs, "ok" means "the launch completed and did not wedge", and the log is the
# only place the payload's own success is visible. Check it explicitly.
echo ""
echo "-- payload success, read out of the logs (bounce hides the child's rc)"
for s in 6_jvm_bare 7_jvm_relay 8_jvm_nested; do
    f="$LOGS/$s.log"
    [ -e "$f" ] || continue
    if grep -q 'N E X T F L O W' "$f"; then verdict="banner printed"
    elif grep -q Exception "$f"; then verdict="JVM ran, threw: $(grep -m1 -oE '[A-Za-z.]*Exception[^ ]*' "$f")"
    else verdict="no banner, no exception"; fi
    printf "   %-14s %s\n" "$s" "$verdict"
done
