# kalen-spirulina-page: stack

## Purpose

Rework Kalen's Spirulina timeline page by walking from the ASV table he holds to each figure and the methods, so every number on the page is explained and the figures show what he needs to read.

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

**asv-table**. Its result: `asv-table/README.md`.

## Stack

- **asv-table** (2026-11-06): we need to walk through this from the asv table shown here
  - [ ] Does the page carry its own per-ASV table, the one every figure derives from?
- **dot-plot** (2026-11-06): try tree on left for the dot plot (full?)
- **shared-asvs** (2026-11-06): for the shared ASVs, need to visualize percent of total, since 6, 10, etc is low?
  - **bar-taxa** (2026-11-06): and need to know what asvs (taxonomically) are those bars
  - **percent** (2026-11-06): percent of sample.. need discussion
- **methods** (2026-11-06): and need to discuss how the pipeline was used. aspire? steps? methods below?
  - [ ] Fix the broken methods heading in Kalen's copy (kalen_page.py inserts a literal \g<1> and drops the h2).

## Parked

## Closed
