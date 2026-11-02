# LiveShell vs TmuxShell — head-to-head results

_ssh host: `mira`_  
_tmux: `tmux 3.2a`_

## Tally

| Shell | PASS | FAIL | HANG | ERROR | SKIP |
|---|---|---|---|---|---|
| LiveShell | 23 | 0 | 0 | 0 | 0 |
| TmuxShell | 22 | 1 | 0 | 0 | 0 |

## Per-scenario

| Scenario | ssh? | LiveShell | TmuxShell |
|---|---|---|---|
| exit_zero |  | PASS (0.00s) | PASS (0.02s) |
| exit_nonzero |  | PASS (0.00s) | PASS (0.02s) |
| exit_arbitrary |  | PASS (0.01s) | PASS (0.03s) |
| collision_frame |  | PASS (0.00s) | PASS (0.02s) |
| collision_legacy_marker |  | PASS (0.00s) | PASS (0.02s) |
| stderr_only_no_hang |  | PASS (0.00s) | PASS (0.02s) |
| silent_prompt |  | PASS (0.00s) | PASS (0.02s) |
| mixed_streams |  | PASS (0.00s) | PASS (0.02s) |
| no_cpu_poll |  | PASS (3.00s) | PASS (3.02s) |
| no_fd_leak |  | PASS (0.22s) | PASS (0.29s) |
| async_single |  | PASS (0.11s) | PASS (0.12s) |
| async_batch_last |  | PASS (1.51s) | PASS (1.53s) |
| streaming_incremental |  | PASS (0.91s) | PASS (0.93s) |
| subshell_roundtrip |  | PASS (0.26s) | PASS (0.33s) |
| subshell_two_levels |  | PASS (0.51s) | PASS (0.65s) |
| subshell_exception_pops |  | PASS (0.26s) | PASS (0.34s) |
| subshell_mixed_streams |  | PASS (0.26s) | PASS (0.33s) |
| subshell_repeated_crossing |  | PASS (2.54s) | PASS (3.17s) |
| subshell_chatty_exit |  | PASS (0.26s) | PASS (0.33s) |
| ssh_oneshot_rc0 | yes | PASS (0.28s) | PASS (0.26s) |
| ssh_oneshot_rc1 | yes | PASS (0.26s) | PASS (0.38s) |
| ssh_rsync_compound | yes | PASS (0.98s) | PASS (1.14s) |
| ssh_persistent_subshell | yes | PASS (0.54s) | FAIL (40.92s) |

## Failure / hang details

- **ssh_persistent_subshell** / TmuxShell: FAIL — r1=['echo remote_$(hostname); printf \'^^SUB 0a086bc180ab8ec1c7bbacca11350444 9sYdlTJ8lXOO %s^^\\n\' "$?"', '\x1b[?2004h\x1b[01;32m20:48:40\x1b[00m:[\x1b[01;34m~\x1b[00m]🍑 echo remote_$(hostname); printf \'\x07SUB 0a086bc180ab8ec1c7bbacca11350444 9sYdlTJ8lXOO %s\x07\\n\' "$?"', '\x1b[?2004l\rremote_pavilion', 'SUB 0a086bc180ab8ec1c7bbacca11350444 9sYdlTJ8lXOO 0']/None r2_rc=None back=['local_back']
