# The `ExecWithEnv` port

`ExecWithContainer` is gone from the engine. It is listed in `_FORBIDDEN_CALLS`
in `metasmith/env/dispatch_scan.py`, so a transform still calling it is rejected
statically rather than failing at run time. Every chain in `logistics`,
`assembly`, `metagenomics` and `functionalAnnotation` now reads

    context.ExecWithEnv().ifContainerDo(env=<env dep>, cmd=..., ...)

`dispatch_scan` reports zero violations and zero chains whose `env=` could not
be resolved to a module-level `Dependency`.

## What goes in this file

Why the environment dispatch is shaped the way it is, and what an `ifVirtualEnvDo` arm has to be
true for. Counts of chains, arms and eligible tools belong to `venv_arms.py`, which recomputes
them.

## Why the commands were not touched

The port rewrites the call head and its own `image=` keyword. Nothing else.
Command bodies are byte-identical.

That is not tidiness. `RemoveLeadingIndent` derives its strip width from the
first non-empty line of `cmd` and applies it to every line, so re-indenting a
command — including the incidental re-indent that comes from moving it — changes
what the shell receives. It fails silently, as a corrupt script rather than a
syntax error. `transforms/metagenomics/taxonomy/centrifuger.py` carries the
warning in-file.

## What an `ifVirtualEnvDo` arm claims

The arms are a portability *claim*: a chain declaring only a container arm is
what makes "can this tool run without a container?" answerable as no, rather
than unknown. An arm that has never been run answers it wrongly, so one is added
where the mamba path can actually be exercised — not as a formality across the
whole library.

A chain is eligible for one when both hold: the tool's `resources/env/<tool>.env`
declares `conda:`, and the container arm passes neither `binds=` nor `args=`.
Those two exist only because there is a mount namespace; a chain that needs them
has to be read by hand before it can claim to run without one — which is why
`functionalAnnotation`, whose tools take large reference databases as binds, is
largely ineligible. Adding an arm to a chain whose `cmd=f"""..."""` is inline
means hoisting it out of the call, which is exactly the re-indent the section
above is about: a command body moves byte-identical or not at all.

`venv_arms.py report <lib>...` recomputes the eligible set per library — the
script sits beside this file, and is run from a library root.
`dev/libraries.sh --create-envs` builds the environments locally to test an arm
against; `metasmith workflow setup-env` (the GUI's `setup environment`) builds
the ones a given workflow needs on the agent that will run it, and names the
tools that ship no `conda:` entry at all.
