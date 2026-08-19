# What goes in this directory

Working records that outlived the work: open punch lists and the reasoning behind a decision
that code cites but cannot contain. Not architecture — that is `docs/metasmith/architecture.md`
— and not a changelog, which is what git is for. An executed plan is a changelog.

## When a record may be deleted

**"Finished" is not a property of the document. It is a property of whether anything still
points at it.** Grep before deleting, and treat a hit from source or tests as a veto:

    grep -rn "plans/<name>" --include='*.py' --include='*.rs' --include='*.md' .

When a record has no referents, **harvest, then delete**: whatever invariant it established
moves into the module's `architecture.md` as a sentence, and git holds the rest.

Never cite a file here by line number. That pointer survives every rename and reflow of the
file it names, and then silently means something else. Cite a section heading.
