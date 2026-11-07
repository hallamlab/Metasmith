# duplicates

## Purpose & Contents

This topic answers Tony's request of 2026-11-06:

> and we should scan for duplicates and propose old locations to remove (do this now)

It holds the result of the duplicate scan and the decision on what to do with it. The keep-or-quarantine table and the per-group paths live in DVC at `data/engcyc/sources/chinook/`, because this repository is public. The journal post *Chinook cleanup candidates* (2026-11-07) gives each candidate with its options, tradeoff and the owner whose OK it needs.

## Status on 2026-11-07

The scan listed every chinook file and every fir file over 50 MiB. It matched copies on name and size, not checksum. Nothing has moved.

| Group | Size | Owner | Engcyc archive data |
|---|---|---|---|
| Cross-tree copies in personal backups and scratch folders, 13 ranked groups | about 16 TB | Lab members, one group each | No |
| Copies that the new Sakinaw entry makes redundant | about 0.3 TB | Juan, and the binning folder's owner | Yes |
| The older partial sharknado copy on chinook | about 0.7 TB | Tony | Yes, until each project entry exists |
| Agent-made staging folders on altair | 0.47 TB | Tony | Yes |
| The quarantine folder: 6 superseded per-delivery manifests | under 1 MB | Tony | Yes |

Three more matches are false: two hash-named work folders and one benchmark folder.

The fosmid archive folder overlap and the repeated 454 files inside the Sakinaw entry are not deletions. The Fosmids entry build settles the first. The second stays, because the repeated files are the record of what went to SRA.

## Open questions

- Is the proposal the end of this topic, with each cleanup left until an owner or Tony wants the space?
