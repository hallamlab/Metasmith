# The `ExecWithEnv` port

## What goes in this file

Why launching a tool is one call and not two, and what the engine does instead of
asking the author. Per-library counts belong to `dispatch_scan`, which recomputes them.

## One call

A transform body names what to run and the environment to run it in:

    context.ExecWithEnv(env=<env dep>, cmd=..., binds=..., args=..., exports=...)

and nothing about the runtime. `ExecWithContainer` and the two arms that preceded this
— `ifContainerDo` and `ifVirtualEnvDo` — are in `_FORBIDDEN_CALLS` in
`metasmith/env/dispatch_scan.py`, so a body still calling any of them is rejected
statically rather than failing at run time.

## Why the arms went

The arms asked the author to restate, per runtime, something the engine already knew.
Across the repository 122 bodies declared both, and 121 of those passed a byte-identical
command to each; the one that did not was an image missing a package, wearing a body's
clothes. Meanwhile `ResolveEnvImage` already selects `container:` or `conda:` by runtime,
`MakeBindsParam` already renders nothing where there is no mount namespace, and
`GetContainerModel` already refuses a `binds=` that cannot mean anything. The split was
carrying no information, and the 136 bodies that declared only a container arm were
silently unrunnable under MAMBA — a claim nobody had made deliberately.

The eligibility rule that used to live here — a chain may declare a conda arm when its
`.env` has `conda:` and it passes no `binds=` or `args=` — was the same two checks written
a second time, in prose, for a human to apply. Both are now enforced where they can fail
honestly: the environment half by the staging preflight, in both directions, and the mount
half by the `assert not binds` in `GetContainerModel`.

`ifVirtualEnvDo` never meant a Python virtualenv. It meant "no container boundary", and
that misnomer is part of why the arm read as a portability claim rather than as dispatch.

## Where the question is answered now

`metasmith/agents/portability.py` reads the staged env manifest before a workflow runs and
refuses any step whose environment resource lacks the key this agent's runtime needs: no
`conda:` under MAMBA, no `container:` under DOCKER or APPTAINER. It names the environment,
not the transform, because the environment is what has to change. A resource that could not
be read is UNKNOWN and stays silent — otherwise every workspace staged by an older metasmith
starts failing for the wrong reason.

`dev/libraries.sh --create-envs` builds the conda environments locally to test against;
`metasmith workflow setup-env` (the GUI's `setup environment`) builds the ones a given
workflow needs on the agent that will run it.

## Why the commands were not touched

The port rewrites the call head and its keywords. Nothing else. Command bodies moved
byte-identical, verified by comparing every migrated call site against its pre-migration
source rather than by reading the diff.

That is not tidiness. `RemoveLeadingIndent` derives its strip width from the first
non-empty line of `cmd` and applies it to every line, so re-indenting a command — including
the incidental re-indent that comes from moving it — changes what the shell receives. It
fails silently, as a corrupt script rather than a syntax error.
`transforms/metagenomics/taxonomy/centrifuger.py` carries the warning in-file.
