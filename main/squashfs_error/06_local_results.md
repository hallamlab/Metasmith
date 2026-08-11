# capella (this dev box) — WSL2 kernel 6.18.33.2, apptainer 1.4.2, no setuid starter

The host class Bug E.2 was originally reported on. Apptainer ≥1.4 makes
`MakeSandboxDecisionProbe` answer `use-sif` here too, so forced-SIF is the default rather
than something that had to be forced.

`APPTAINER_CACHEDIR=/home/tony/.apptainer/cache` is set on this box, so the SIF had to be
pre-placed *there* rather than under the agent home — `_store_root()` reads the variable
first, and deploy otherwise tries to pull an unpublished tag and dies at the relay-missing
assertion.

| rung | payload | result |
|---|---|---|
| deploy | `metasmith api deploy_from_container` inside the SIF | ok, relay extracted |
| 1 bare | `metasmith --help` | ok 2s, help printed |
| 2 `msm` wrapper | `metasmith --help` | ok 2s, help printed |
| 3 relay-dispatched | `metasmith --help` | ok 4s, help printed |
| 4 nested (container → relay → container) | `metasmith --help` | ok 4s |
| 5 fanout ×8 of rung 4 | `metasmith --help` | ok 4s |
| 6 bare | `nextflow -version` | ok 2s, banner |
| 7 relay-dispatched | `nextflow -version` | ok 2s, banner |
| 8 nested | `nextflow -version` | ok 2s, banner |
| 9 fanout ×8 of rung 8 | `nextflow -version` | ok 4s |

Validity confirmed: `squashfuse_ll -f -o allow_other,ro,uid=1000,gid=1000,offset=40960
/proc/self/fd/3 /var/lib/apptainer/mnt/session/rootfs`, `/sys/fs/fuse/connections/105`
present, container `/` an overlay over that rootfs, no temporary-sandbox conversion.

## The one structural difference between the two hosts

Measured off the reader's own `/proc` entry while a container was open:

| host | apptainer | reader PGID |
|---|---|---|
| capella (WSL2) | 1.4.2 | **shares the launching script's process group** (reader 195799, pgid 195760 = the probe script) |
| chamois | 1.4.5 | **its own** (reader 2386616, pgid 2386616) |

So the lifecycle entanglement that makes a group-level signal tear the rootfs out from
under a live container is still present on 1.4.2 and gone on 1.4.5. It is the mechanism
behind the original report's `Transport endpoint is not connected` tail — and it is *not*
sufficient to produce the wedge on its own, since nothing here wedged.
