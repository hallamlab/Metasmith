#!/bin/bash
# Mirrors the adaptive de-synchronization logic in the agents.py bootstrap
# heredoc (start jitter sized to the SLURM array, + exponential-with-jitter
# backoff). Exposed as pure computation (no real sleeps) so the bounds are
# deterministically testable. Keep in sync with agents.py `run_container`
# preamble: `_win = min(300, 3*count)`, `_delay in [0,_win]`,
# `msm_backoff` cap `_b = min(60, 1<<attempt)`.
set -u
case "${1:-}" in
  window)   # echo the jitter window for a given array task count
    c="$2"
    if [ "$c" -le 1 ]; then echo 0; exit 0; fi
    w=$(( c * 3 )); [ "$w" -gt 300 ] && w=300; echo "$w" ;;
  delay)    # echo a sampled jitter delay for a given array task count
    c="$2"
    if [ "$c" -le 1 ]; then echo 0; exit 0; fi
    w=$(( c * 3 )); [ "$w" -gt 300 ] && w=300
    echo $(( RANDOM % (w + 1) )) ;;
  backoff_cap)  # echo the backoff sleep CAP for a given attempt (pre-jitter)
    a="$2"; b=$(( 1 << a )); [ "$b" -gt 60 ] && b=60; echo "$b" ;;
  *) echo "usage: $0 window|delay|backoff_cap N" >&2; exit 2 ;;
esac
