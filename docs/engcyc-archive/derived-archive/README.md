# derived-archive

## Purpose & Contents

This topic answers Tony's request of 2026-11-06:

> For those, we will want to do a separate archive, like how Cyanoverse is working right now. And that has like a structured file tree that we would want to adopt as a paradigm. We don't have to do exactly because the tools don't match, but you know, the idea needs to flow.

"Those" are the outputs that the raw-data entries leave behind on the backup volume: annotation tables, BLAST tables, alignments, assemblies and analysis work folders. This topic decides the tree they go into and which of them to keep.

## Status on 2026-11-07

- `research/engcyc/LAYOUT.md` already fixes one tree for derived files. It merges the Cyanoverse and Spanish Lakes publish trees into `<dataset>/<subject>/<product>/<stem>.<ext>`, with one tool output kind per product. New pipeline runs write to it.
- The old outputs come from older tools, such as MetaPathways and the JGI annotation pipeline. LAYOUT.md names no product for them.
- Their size is not measured. The backup-volume listing sized the raw-data folders only.
- The JGI annotation copies are not yet compared with what IMG serves today.

## Open questions

- Does the derived archive reuse the LAYOUT.md tree, with each old tool output as its own product?
