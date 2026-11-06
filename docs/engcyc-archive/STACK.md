# engcyc-archive: stack

## Purpose

Bring every Engcyc manifest dataset's raw data onto chinook in one documented shape, and settle the archive decisions still open: the remaining batches, the external datasets' home and the duplicate cleanup.

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

**external-home**. Its result: `external-home/README.md`.

## Stack

- **external-home** (2026-11-06): and we need to decide where to place the the external datasets like CAMI, tara oceans, etc
  - [ ] home: one folder per external dataset under the chinook external-data folder, as CAMI is now?
  - [ ] keep-or-point: keep a copy of public reads, or keep an accession manifest only?
  - [ ] lost-sets: the ocean sets and the earlier Engcyc build are not on the backup volume; is sharknado itself still readable?
- **duplicates** (2026-11-06): and we should scan for duplicates and propose old locations to remove (do this now)
- **derived-archive** (2026-11-06): For those, we will want to do a separate archive, like how Cyanoverse is working right now. And that has like a structured file tree that we would want to adopt as a paradigm. We don't have to do exactly because the tools don't match, but you know, the idea needs to flow.
  - [ ] jgi-verify: Do the annotations IMG serves today match the copies on the backup volume? Measure the compressed size of the derived output. Keep it, or write a README that points to the download?

## Parked

## Closed

- **remaining-datasets** (closed 2026-11-06): `remaining-datasets/README.md`
