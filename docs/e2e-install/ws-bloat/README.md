# ws-bloat

> on sockeye, the fs should be limited somewhow as well .we dont want auto clean up of ws, but we do need some mechanism to prevent it getting bloated. error  on test start if ws exists?

> you delete after, otherwise wirks

## Design (decided 2026-11-19)

- **One test root, one run at a time.** Each run gets its own directory under `$SCRATCH/msm_e2e/` on nibi. The harness refuses to start while any run directory exists there. The altair side follows the same rule for its conda env and client directory.
- **No automatic cleanup.** The supervising agent deletes a run's workspace by exact path once it has read the report, and not before.
- **A cap while the run is live.** Lustre project quotas cannot be set by a user, so a watcher on altair samples the run directory every few minutes. It logs files and GB per top-level subdirectory, and kills the run past 600K files or 600 GB. nibi's limits are 1M files and 1 TB per user.
- **Reference databases.** Run 1 downloads them inside its run directory, as a new user's first run would. Whether later runs reuse a persistent copy is decided after run 1.
