# Can Lustre take many parallel reads, or should we copy?

> Can lustre handle multiple parallel reads or should we copy?

## Answer

Lustre handles many parallel large sequential reads well. It handles metadata operations and small random reads poorly. Choose by access pattern:

- **Streamed once, large:** read in place, or copy. Lustre serves the bytes from several storage targets at once. `eggnog.db` is striped across 8 targets.
  Examples: `eggnog_proteins.dmnd` (9.3 GB, read by diamond), FASTA inputs.
- **Random small reads, or many lookups:** copy to node-local disk. Every `open`, `stat` and failed lookup goes to the single metadata server, and the client does not cache failed lookups well. 3,100 cores probing for a missing `eggnog.db-journal` file flooded that server. Random 4 KB SQLite page reads also hit the storage targets with small requests.
  Examples: `eggnog.db` (41 GB SQLite), any SQLite or BerkeleyDB file, and trees of many small files such as HMM folders or Pfam splits.

Fir CPU nodes have 7.2 TB of local disk (`$SLURM_TMPDIR`), 768 GB of RAM and 192 cores. A 41 GB copy at 1–2 GB/s takes under a minute, which is small against a multi-hour task. The cost scales with jobs, not cores, so fewer and larger jobs per node mean fewer copies.
