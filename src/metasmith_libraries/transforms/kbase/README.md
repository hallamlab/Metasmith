# `kbase/` -- the parity transforms

One directory per KBase task verb, from curation round 3. Round 3 read all 493 apps in the
KBase catalog against this library and named the verbs nothing here performed;
`research/kbase/curation/r3/proposals.yml` carries the proposal each of these files was
written from -- its signature, the tool it runs, and what running it buys.

**Every body here runs its tool, and twenty of the twenty-one have been executed.** Curation
round 5 wrote them; `research/kbase/curation/r5/runs.md` is the record of what each run
produced and what was checked in it, and `fixtures.md` beside it is the recipe for rebuilding
the inputs. `build_tree/gtdbtk_tree.py` is the exception and says so in its own body: its
reference is the GTDB release, ~110 GB, and it was checked by reading
`metagenomics/taxonomy/gtdbtk.py` -- same image, same bind, same `GTDBTK_DATA_PATH` -- rather
than by running.

Eight of these are an algorithm rather than a shell command, so the algorithm lives under
`resources/lib/` and the protocol is one `python <script> <args>` line. `lib::modelling` is
the largest: the four metabolic bodies plus the MetaNetX helpers lifted out of
`research/fabfos/benchmarks/laser/vs_gem/`.

This is a library ROOT, not a group -- `transforms/*/` enumerates roots and each root globs
itself recursively, so the verb directories below are ordinary subdirectories and
`ResolveParentLibrary` walks up to here from any depth. `_metadata/` is built here and
nowhere deeper.

Not to be confused with `research/kbase/library/transforms/kbase/`, which is the generated
research port of the catalog itself -- 371 stubs named `Module__app.py`, a separate root that
neither loads nor is loaded by this one.
