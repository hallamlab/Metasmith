# What is --dbmem?

> what is --dbmem? RAM is limited...

## Answer

`emapper.py --dbmem` loads all of `eggnog.db` (41 GB SQLite) into RAM when the task starts. After that, annotation lookups run against memory. This removes the SQLite file probes and the random reads on shared storage.

The cost is RAM. Commit `71cd5453` sets each 16-core eggnog task to 80 GB, which is 5 GB per core. Fir nodes have 4 GB per core (768 GB / 192), so memory and not cores becomes the limit on how many tasks fit on a node. Each task also reads the full 41 GB at start, as one sequential read.

A local copy without `--dbmem` gets most of the same benefit at less RAM. The file probes go to local disk, which ignores them. The kernel page cache keeps the hot pages in memory, can reclaim them under pressure, and shares them across tasks on the same node.
