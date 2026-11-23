# Reply to Wade (Alliance support)

> ok, draft, I will send

Tony sends it. Reply to Ticket#0331813.

---

Subject: Re: [Ticket#0331813] Account limited by number of parallel jobs = 1

Hi Wade,

Thanks for the explanation, and sorry for the trouble on Friday. We tracked down the source.

The calls came from eggNOG-mapper, specifically from SQLite. Our pipeline split two large co-assemblies into about 400 chunks and ran about 200 of them at once (16 cores each), all reading one shared eggnog.db on /scratch. When eggNOG-mapper runs without loading the DB into memory, SQLite checks for eggnog.db-journal and eggnog.db-wal at the start of every read. Both checks fail because neither file exists. We reproduced this with strace on a single 4-core job, annotating 200 proteins: 78,000 failed stat() calls in 104 seconds, about 390 per protein. Nothing else in the job contributes.

The fix is in progress. Our workflow tool will copy reference databases to node-local disk ($SLURM_TMPDIR) at the start of each job and read them there, so these lookups never reach Lustre. We are also auditing our other tools so that each job reads its inputs once at the start and writes its outputs once at the end.

Once the fix is in, we would appreciate having the limit lifted, in steps if the site staff prefer. I will let you know when it lands.

Thanks,
Tony
