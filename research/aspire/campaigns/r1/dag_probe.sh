#!/bin/bash
# One-shot state probe for THIS run. Lives on fir as a FILE so that the caller's
# command line never contains the strings the probe greps for -- the pgrep
# self-match bug (RUN_LOG) bit twice when the pattern was inline.
#
# Emits one compact signature line plus zero or more ALARM lines.
# The run key is read from a file, not baked in: the restart re-plans to 25
# steps under a NEW key, and the Monitor invoking this probe must survive
# that without being torn down and re-armed.
KEY=$(cat /scratch/phyberos/gmcf3495/.current_run_key 2>/dev/null)
[ -n "$KEY" ] || KEY=KMQ5eomS
RUN=/scratch/phyberos/gmcf3495/metasmith/runs/$KEY

# --- cluster maintenance reservation: the thing gating the whole run ---
resv=$(scontrol show reservation CDUMaintenance2 2>/dev/null | grep -o 'State=[A-Z]*' | head -1 | cut -d= -f2)
[ -z "$resv" ] && resv=GONE

# --- our jobs only: attribute every queue line by WorkDir, not by user ---
run=0; pend=0; other=0
while IFS='|' read -r id st; do
    [ -z "$id" ] && continue
    wd=$(scontrol show job "${id%%_*}" 2>/dev/null | grep -m1 -o 'WorkDir=[^ ]*')
    case "$wd" in
        *"$KEY"*) case "$st" in RUNNING) run=$((run+1));; PENDING) pend=$((pend+1));; esac ;;
        *) other=$((other+1)) ;;
    esac
done < <(squeue -u phyberos -r -h -o '%i|%T')

# --- head process for THIS run (java + the metasmith driver beside it) ---
nf=$(pgrep -u phyberos -c -f "runs/${KEY}/nxf_work" 2>/dev/null || echo 0)

prod=$(find "$RUN/results" -mindepth 2 -maxdepth 2 -type f 2>/dev/null | wc -l)

# Deliberately NOT reporting the co-tenant job count here. It is computed
# above so the queue attribution stays honest, but emitting it put another
# run's churn into the signature the Monitor dedups on, which woke us for
# state that is not ours. The signature carries only THIS run.
echo "SIG key=$KEY resv=$resv run=$run pend=$pend head=$nf products=$prod"

# --- alarms: every terminal state we would act on ---
if [ "$nf" -eq 0 ]; then
    echo "ALARM head process for $KEY is GONE -- run stopped; cancel orphans before -resume"
fi
# Scope by WorkDir, exactly as the queue counts are. The phyberos account is
# shared and a concurrent fabfos run (kecQUgYv) contributes its own failures:
# 7 non-success rows today, only 3 of them this run's. sacct carries WorkDir,
# so the same attribution rule works here as in the queue loop above.
# LOCAL date -- fir is UTC-7 and sacct rejects a UTC date that is still "future".
sacct -u phyberos -S "$(date +%F)" -X -n -P -o JobID,JobName,State,ExitCode,WorkDir 2>/dev/null \
  | awk -F'|' -v k="$KEY" '$5 ~ k' \
  | grep -E 'FAILED|TIMEOUT|OUT_OF_ME|CANCELLED|NODE_FAIL|BOOT_FAIL' \
  | grep -v 'CANCELLED by 0' | cut -d'|' -f1-4 | head -10 | sed 's/^/ALARM sacct /'
grep -ohE 'Task .* ignored|terminated with an error exit status' \
  "$RUN"/_metasmith/logs.*/nxf.log 2>/dev/null | tail -5 | sed 's/^/ALARM nxf /'
