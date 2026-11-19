# e2e-install: stack

## Purpose

Make metasmith installable and runnable from its instructions and API with minimal technical skill, proven by a context-siloed agentic E2E test that installs and runs metasmith on sockeye on one sample, reporting issues until there are none.

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

(none: the stack is empty)

## Stack

## Parked

## Closed

- **test-env** (closed 2026-11-17): `test-env/README.md`
- **software-suite** (closed 2026-11-18): `software-suite/README.md`
- **ws-bloat** (closed 2026-11-19): `ws-bloat/README.md`
- **watch-list** (closed 2026-11-19): `e2e-test/watch-list/README.md`
- **scratch-blowup** (closed 2026-11-19): `e2e-test/scratch-blowup/README.md`
- **e2e-test** (closed 2026-11-19): `e2e-test/README.md`
