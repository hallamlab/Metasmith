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

**fix-brief**. Its result: `fix-brief/README.md`.

## Stack

- **mechanism** (2026-11-10): no need for fix, we just need to confirm and locate issue for now.
  - [ ] Confirm with a one-off strace of emapper (with and without --dbmem) on a small query, or accept the sqlite reasoning as is?
- **reply-to-wade** (2026-11-10): please read [Ticket#0331813] Account limited by number of parallel jobs = 1 email for context.
- **fix-brief** (2026-11-10): we should create journal brief as deliverable for next task to fix all of this.
  - [ ] Post the brief to a new scope metasmith/engine/local-staging (branched from feat/dev, the fix spans engine + libraries), rather than engine/dev?

## Parked

## Closed

- **culprit** (closed 2026-11-10): `culprit/README.md`
- **lustre-reads** (closed 2026-11-10): `db-handling/lustre-reads/README.md`
- **dbmem** (closed 2026-11-10): `db-handling/dbmem/README.md`
- **metapathways** (closed 2026-11-10): `db-handling/metapathways/README.md`
- **db-handling** (closed 2026-11-10): `db-handling/README.md`
- **job-specs** (closed 2026-11-10): `job-specs/README.md`
- **nibi-exposure** (closed 2026-11-10): `nibi-exposure/README.md`
- **scratch-contracts** (closed 2026-11-10): `scratch-contracts/README.md`
