"""UniRef50 -- cluster representative sequences, gzipped as served.

One file into `uniref/<release>/`: `uniref50.fasta.gz`, ~8.8 GB at 2026_02. The long
pole of the whole acquisition.

STAYS GZIPPED. `diamond makedb` reads .gz directly and the label pool streams it
line by line, so decompressing would add ~40 GB to this tier and buy nothing. The
DIAMOND database itself is compile/uniref50_dmnd.py's product -- the shipped
library's logistics/downloadUniRef50DB.py fuses fetch and makedb into one step, and
splitting them is what lets this be pinned once and reused when the database is
rebuilt for a new DIAMOND version.

`-c` so an interrupted transfer resumes rather than restarting. At this size that is
the difference between a retry and a lost afternoon.

THE RELEASE IS READ FROM RELEASE.metalink, WHICH ALSO CARRIES THE MD5. Two things
that would otherwise be unavailable come out of one small file fetched first:

  * `<version>` names the directory. The neighbouring `uniref50.release_note` looks
    like the right source and is NOT -- upstream ships it with the field blank
    ("Release: , --"), so anything reading it gets an empty version and no error.
  * The per-file `<hash type="md5">` verifies the transfer, the same claim MetaNetX's
    .md5 sidecars support and the only one that catches a truncated or corrupted
    multi-gigabyte download. The metalink is kept in the product so the check is
    re-runnable offline later without trusting this transform's word for it.

SERVED FROM THE EBI MIRROR, NOT ftp.uniprot.org. The US host was unreachable when
this was written (connect timeouts on every path, including small text files, which
is what left an earlier `reldate.txt` on disk at zero bytes -- a silent empty file
rather than a failure). EBI is an official UniProt mirror carrying the identical
release, and the metalink lists all three hosts for the same bytes, so the md5 check
above makes the choice of mirror a performance question rather than a trust one.

WHAT THIS IS NOT. UniRef50 clusters at 50% identity and names each cluster after
its REPRESENTATIVE, so this file holds one sequence per cluster -- not one per
UniProt entry. Anything that needs a specific accession's sequence needs
swissprot/ or the full UniProtKB, because an accession that is a cluster MEMBER
rather than its representative simply is not in here. That distinction is what the
label pool's coverage turns on; see acquire/swissprot.py.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::uniref"))

BASE_URL = ("https://ftp.ebi.ac.uk/pub/databases/uniprot"
            "/current_release/uniref/uniref50")
DATA_FILE = "uniref50.fasta.gz"
METALINK  = "RELEASE.metalink"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        wget -q {BASE_URL}/{METALINK} -O _metalink.xml
        REL=$(grep -oE '<version>[^<]+' _metalink.xml | head -1 | sed 's/.*>//')
        WANT=$(grep -A4 'file name="{DATA_FILE}"' _metalink.xml \\
               | grep -oE '<hash type="md5">[^<]+' | head -1 | sed 's/.*>//')
        if [ -z "$REL" ] || [ -z "$WANT" ]; then
            echo "[uniref] {METALINK} gave release [$REL] md5 [$WANT] -- one is empty," \\
                 'so the layout upstream serves has changed and this would write an' \\
                 'unlabelled or unverifiable snapshot' >&2
            exit 1
        fi
        D={iout.container}/$REL
        mkdir -p $D
        mv _metalink.xml $D/{METALINK}
        echo "[uniref] release $REL, upstream md5 $WANT"

        wget -q -c {BASE_URL}/{DATA_FILE} -O $D/{DATA_FILE}
        HAVE=$(md5sum $D/{DATA_FILE} | cut -d' ' -f1)
        if [ "$WANT" != "$HAVE" ]; then
            echo "[uniref] MD5 MISMATCH: upstream says $WANT, got $HAVE" >&2
            exit 1
        fi
        gzip -t $D/{DATA_FILE}
        echo "[uniref] $(du -h $D/{DATA_FILE} | cut -f1) verified against upstream md5"
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    vers = sorted(p for p in iout.local.glob("*") if p.is_dir())
    wanted = (DATA_FILE, METALINK)
    got = [f for v in vers for f in wanted
           if (v / f).exists() and (v / f).stat().st_size > 0]
    Log.Info(f"uniref: {len(got)}/{len(wanted)} files in "
             f"{', '.join(v.name for v in vers) or '(no version dir)'}")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(wanted),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(hours=12)),
)
