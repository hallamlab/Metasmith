# `kbase/` -- the parity transforms

One directory per KBase task verb, from curation round 3. Round 3 read all 493 apps in the
KBase catalog against this library and named the verbs nothing here performed;
`research/kbase/curation/r3/proposals.yml` carries the proposal each of these files was
written from -- its signature, the tool it runs, and what running it buys.

**Every body in this directory is a stub.** It touches the files it declares and runs no tool.
The real command is in a comment at the top of each protocol, taken from the proposal. The
signatures are load-bearing and the bodies are not: this directory exists so the planner can
route through these steps before any of them is implemented.

This is a library ROOT, not a group -- `transforms/*/` enumerates roots and each root globs
itself recursively, so the verb directories below are ordinary subdirectories and
`ResolveParentLibrary` walks up to here from any depth. `_metadata/` is built here and
nowhere deeper.

Not to be confused with `research/kbase/library/transforms/kbase/`, which is the generated
research port of the catalog itself -- 371 stubs named `Module__app.py`, a separate root that
neither loads nor is loaded by this one.
