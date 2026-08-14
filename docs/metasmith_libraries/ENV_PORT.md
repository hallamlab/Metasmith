# The `ExecWithEnv` port

`ExecWithContainer` is gone from the engine. It is listed in `_FORBIDDEN_CALLS`
in `metasmith/env/dispatch_scan.py`, so a transform still calling it is rejected
statically rather than failing at run time. Every chain in `logistics`,
`assembly`, `metagenomics` and `functionalAnnotation` now reads

    context.ExecWithEnv().ifContainerDo(env=<env dep>, cmd=..., ...)

110 chains across 87 files. `dispatch_scan` reports zero violations and zero
chains whose `env=` could not be resolved to a module-level `Dependency`.

## Why the commands were not touched

The port rewrites the call head and its own `image=` keyword. Nothing else.
Command bodies are byte-identical.

That is not tidiness. `RemoveLeadingIndent` derives its strip width from the
first non-empty line of `cmd` and applies it to every line, so re-indenting a
command — including the incidental re-indent that comes from moving it — changes
what the shell receives. It fails silently, as a corrupt script rather than a
syntax error. `transforms/metagenomics/taxonomy/centrifuger.py` carries the
warning in-file.

## Why there are no `ifVirtualEnvDo` arms yet

The arms are a portability *claim*: a chain declaring only a container arm is
what makes "can this tool run without a container?" answerable as no, rather
than unknown. An arm that has never been run answers it wrongly.

Nothing in this line of work executes the mamba path — it runs under Apptainer
on fir — so no arm added here could have been verified. Adding 59 of them would
also have meant hoisting each inline `cmd=f"""..."""` out of its call, which is
precisely the re-indent the section above is about, across transforms that then
process 34 libraries.

So the analysis is recorded instead of guessed at. Of 110 chains:

| library | eligible | passes `binds`/`args` | tool has no `conda:` |
|---|---:|---:|---:|
| logistics | 12 | 1 | 15 |
| assembly | 22 | 0 | 2 |
| metagenomics | 22 | 1 | 2 |
| functionalAnnotation | 3 | 21 | 9 |
| **total** | **59** | **23** | **28** |

*Eligible* means both: the tool's `resources/env/<tool>.env` declares `conda:`
(49 of 70 do — an assertion authored by the env migration, which ships
`envs/gen_tool_envs.py` and per-tool recipes, not an inference), and the
container arm passes neither `binds=` nor `args=`. Those two exist only because
there is a mount namespace; a chain that needs them has to be read by hand
before it can claim to run without one. `functionalAnnotation` is mostly in that
column because its tools take large reference databases as binds.

Whoever picks the mamba path up: the 59 are the mechanical set, and
`dev.sh --create-envs` builds the conda environments to test them against.

Reproduce the table with `venv_arms.py report <lib>...` (see the run's scratch
project).
