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

**remaining-datasets**. Its result: `remaining-datasets/README.md`.

## Stack

- **remaining-datasets** (2026-11-06): status on the rest of the files to archive?
  - [ ] route-and-room: Move the backup-volume batches by Globus straight from its read-only endpoint, gzip only the uncompressed reads on altair, smallest batch first? Does chinook have room for about 36 TB more?
  - [ ] centre-extras: Keep the centres' annotation, alignment and assembly output beside the reads (about 5 TB), or move raw reads only?
  - [ ] chinook-elsewhere: Rename the 8 sets in personal or project folders on chinook into received-data entries, or write their manifest.csv in place?
  - [ ] off-chinook: Upload the 14 sets that live only on fir, Sockeye or altair as received-data entries?
- **external-home** (2026-11-06): and we need to decide where to place the the external datasets like CAMI, tara oceans, etc
- **duplicates** (2026-11-06): and we should scan for duplicates and propose old locations to remove (do this now)

## Parked

## Closed
