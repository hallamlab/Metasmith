# fir-maxjobs: stack

## Purpose

Confirm and locate what caused Alliance to cap phyberos on fir at max_jobs=1 (Ticket#0331813). Locate only; no fix yet.

Procedure, from dfs-interview § Work the stack:

1. Run `stack.py <folder> current` for the top topic. Ask only its own questions.
2. Dispatch read-only subagents to survey what already exists for the topic.
3. Record each difference between the survey and the user's description.
4. Ask the user about each difference with an unclear answer.
5. Put one question per message to the user, or a small set of closely related questions. Give your recommendation with it.
6. Queue the other questions with `stack.py <folder> ask`.
7. Match each answer to the message it replies to. Ask when a reply could answer more than one message.
8. Record each answer in the topic's `README.md`. Copy the user's words from `quote.py`.
9. Write the result as the current design.
10. Tick each answered question with `stack.py <folder> answer`.
11. Run `stack.py <folder> close` when the topic's question queue is empty.
12. Commit.
13. Compact only when the closed topic ran long, with many exchanges or large reads. After a short topic, present the next topic in the same turn.
14. To compact, run `reflection(verb="compact", args={followup:...})`. Write a followup that invokes `dfs-interview` by name. Name the discussion folder, the closed topic, its commit and the new top topic in it.

## Current topic

**db-handling**. Its result: `db-handling/README.md`.

## Stack

- **db-handling** (2026-11-10): we should talk about how reference databases are handled. we'd want to do read-only and local copy if possible.
  - [ ] Adopt this design: reference DBs are read-only on Lustre (chmod a-w), declared per transform as 'random' (copied to $SLURM_TMPDIR at task start) or 'stream' (read in place); drop --dbmem; batch more chunks per job so one copy serves many?
- **mechanism** (2026-11-10): no need for fix, we just need to confirm and locate issue for now.
  - [ ] Confirm with a one-off strace of emapper (with and without --dbmem) on a small query, or accept the sqlite reasoning as is?
- **nibi-exposure** (2026-11-10): we just need to confirm and locate issue for now.
  - [ ] Check whether a nibi eggnog run without --dbmem is live now, which risks the same cap on nibi?
- **reply-to-wade** (2026-11-10): please read [Ticket#0331813] Account limited by number of parallel jobs = 1 email for context.
- **scratch-contracts** (2026-11-10): as part of the next task, we shuold also make a pass at metasmith contracts so that scratch work is done locally. this is implied by design, but some may still be writing to lustre scratch. jobs should only ideally write once at the end, and read once in the beginning.
- **fix-brief** (2026-11-10): we should create journal brief as deliverable for next task to fix all of this.

## Parked

## Closed

- **culprit** (closed 2026-11-10): `culprit/README.md`
- **lustre-reads** (closed 2026-11-10): `db-handling/lustre-reads/README.md`
- **dbmem** (closed 2026-11-10): `db-handling/dbmem/README.md`
