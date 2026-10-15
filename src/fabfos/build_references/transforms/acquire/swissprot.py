# UniProtKB/Swiss-Prot -- the reviewed sequences, plus the release markers.
#
# Three files into `swissprot/<release>/`: `uniprot_sprot.fasta.gz` (~90 MB),
# `reldate.txt` and `RELEASE.metalink`.
#
# WHY THIS EXISTS AS A SEPARATE SOURCE FROM uniref/. The label pool the kNN transfer
# lane votes against is DEFINED by Swiss-Prot accessions: compile/label_transfer_landmarks
# cuts the bridge to `id_source == "uniprot"` and `evidence_quality == "reviewed"`,
# and `reviewed` is assigned by which Rhea file a row came from -- rhea2uniprot.tsv is
# Swiss-Prot, rhea2uniprot_trembl.tsv.gz is TrEMBL. So the accessions are the reviewed
# set, exactly.
#
# The sequences, however, were being taken from UniRef50, and those two sets only
# coincide where a Swiss-Prot accession happens to be its UniRef50 cluster's
# representative. UniRef50 clusters at 50% identity, so an entry sitting under another
# entry's representative has NO sequence in that file and drops out of the pool
# silently -- the transform counts the loss rather than substituting for it, but
# nothing asserts a floor. The original design chose UniRef50 to avoid adding an
# acquisition ("No separate labelled-proteome acquisition"); at ~90 MB against a
# multi-gigabyte file already being streamed, that trade no longer pays.
#
# A smaller pool is a WEAKER pool, not a wrong one -- this closes a coverage gap, not
# a correctness bug. The two traps that would produce wrong numbers are elsewhere and
# are unaffected: the pool must be embedded with the SAME model as the query, and its
# index and embedding stack must come from ONE pass.
#
# TWO RELEASE MARKERS, AND THEY DO DIFFERENT JOBS. `RELEASE.metalink` is the machine
# one: `<version>` names the directory and the per-file `<hash type="md5">` verifies
# the transfer, which is the same claim MetaNetX's .md5 sidecars support. `reldate.txt`
# is the human one -- it is what UniProt itself points at, it dates the release
# ("Release 2026_02 of 10-Jun-2026") where the metalink only numbers it, and it names
# Swiss-Prot and TrEMBL separately. Both are kept; neither is derived here.
#
# The directory matters because `current_release/` rolls forward under you exactly
# like Rhea's and KOfam's paths do: without it, two acquisitions six weeks apart put
# different reviewed sets at one path, and a pool built from one and an index built
# from the other disagree with nothing on disk to show it.
#
# SERVED FROM THE EBI MIRROR, NOT ftp.uniprot.org, for the reason set out in
# acquire/uniref.py -- the US host was unreachable and its failure mode was a
# zero-byte file rather than an error, which is precisely how an unversioned tier
# loses track of what it holds. The metalink md5 makes the mirror choice a
# performance question rather than a trust one.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::swissprot"))

BASE_URL = ("https://ftp.ebi.ac.uk/pub/databases/uniprot"
            "/current_release/knowledgebase/complete")
DATA_FILE = "uniprot_sprot.fasta.gz"
METALINK  = "RELEASE.metalink"
RELDATE   = "reldate.txt"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        wget -q {BASE_URL}/{METALINK} -O _metalink.xml
        REL=$(grep -oE '<version>[^<]+' _metalink.xml | head -1 | sed 's/.*>//')
        WANT=$(grep -A4 'file name="{DATA_FILE}"' _metalink.xml \\
               | grep -oE '<hash type="md5">[^<]+' | head -1 | sed 's/.*>//')
        if [ -z "$REL" ] || [ -z "$WANT" ]; then
            echo "[swissprot] {METALINK} gave release [$REL] md5 [$WANT] -- one is empty," \\
                 'so the layout upstream serves has changed and this would write an' \\
                 'unlabelled or unverifiable snapshot' >&2
            exit 1
        fi
        D={iout.container}/$REL
        mkdir -p $D
        mv _metalink.xml $D/{METALINK}
        wget -q {BASE_URL}/{RELDATE} -O $D/{RELDATE}
        if [ ! -s $D/{RELDATE} ]; then
            echo '[swissprot] {RELDATE} came back EMPTY -- wget reports success for a' \\
                 'zero-byte body, so this is checked rather than trusted' >&2
            exit 1
        fi
        echo "[swissprot] release $REL, upstream md5 $WANT"
        head -3 $D/{RELDATE}

        wget -q -c {BASE_URL}/{DATA_FILE} -O $D/{DATA_FILE}
        HAVE=$(md5sum $D/{DATA_FILE} | cut -d' ' -f1)
        if [ "$WANT" != "$HAVE" ]; then
            echo "[swissprot] MD5 MISMATCH: upstream says $WANT, got $HAVE" >&2
            exit 1
        fi
        gzip -t $D/{DATA_FILE}
        echo "[swissprot] $(zcat $D/{DATA_FILE} | grep -c '^>') reviewed sequences, md5 verified"
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    vers = sorted(p for p in iout.local.glob("*") if p.is_dir())
    wanted = (DATA_FILE, RELDATE, METALINK)
    got = [f for v in vers for f in wanted
           if (v / f).exists() and (v / f).stat().st_size > 0]
    Log.Info(f"swissprot: {len(got)}/{len(wanted)} files in "
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
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(hours=2)),
)
