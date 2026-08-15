# What goes in this directory

Working records that outlived the work: audits, punch lists, and the reasoning behind a
decision that code cites but cannot contain. Not architecture — that is
`docs/metasmith/architecture.md` — and not a changelog, which is what git is for.

## When a record may be deleted

**"Finished" is not a property of the document. It is a property of whether anything still
points at it.** A plan marked COMPLETE whose invariant a test cites by name is load-bearing;
a plan marked in-progress that nothing references is dead. Grep before deleting, and treat a
hit from source or tests as a veto:

    grep -rn "plans/<name>" --include='*.py' --include='*.rs' --include='*.md' .

Three of the files here are kept by that rule rather than by their own status.
`consolidation-followups.md` is an open punch list. `lineage-quadrant-audit.md` and
`harmonize-data-instance-arity.md` are cited from live tests and from
`models/workflow/cache_decisions.py` as the record of why an invariant test exists and what
its scope caveat is.

When a record genuinely has no referents, **harvest, then delete**: whatever invariant it
established moves into the module's `architecture.md` as a sentence, and git holds the rest.

## A line-numbered citation is the fragile case

`plans/harmonize-data-instance-arity.md:89-123` is the most brittle pointer shape in the
tree: it survives every rename and reflow of the file it names, and then silently means
something else. Prefer citing a section heading. If you edit a file this directory holds,
grep for line-numbered citations to it first.
