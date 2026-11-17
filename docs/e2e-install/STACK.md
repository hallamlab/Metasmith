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

**software-suite**. Its result: `software-suite/README.md`.

## Stack

- **software-suite** (2026-11-10): before this, we need to decide on the software suite. in the future we can test with a subset of that suite.
  - [ ] Is the suite metagenomics_from_paired_reads, and what happens to metabuli (~1.4 TB DB vs ~600 GB free on sockeye scratch)?
  - [ ] Keep all three binners (comebin 48 cpu/192 GB) or trim to metabat2 + semibin2?
- **ws-bloat** (2026-11-10): on sockeye, the fs should be limited somewhow as well .we dont want auto clean up of ws, but we do need some mechanism to prevent it getting bloated. error  on test start if ws exists?
  - [ ] Do the reference DBs live in one shared pre-staged location the test reuses, or does each test run download its own?
  - [ ] Earlier: 'we dont want auto clean up of ws'. Now: 'set up and torn down for each test'. Which parts are torn down per test, and which persist (e.g. reference DBs, user-visible workspace)?
- **e2e-test** (2026-11-10): whats needed is an agentic E2E test that is siloed from additional context (maybe haiku?) that just isntalls and runs metasmith on sockeye on 1 sample E2E. reporting issues and guesses until there are none.
  - **watch-list** (2026-11-17): Please watch for things like container pulls and correct cache allocation. Should also report with a series of steps take so that we can check for any ambiguities or strangeness that was required.
  - **scratch-blowup** (2026-11-17): a beta tester reports that as is, metasmith just fills up the scratch space without finishing (millions of files) so this is the first error to check for

## Parked

## Closed

- **test-env** (closed 2026-11-17): `test-env/README.md`
