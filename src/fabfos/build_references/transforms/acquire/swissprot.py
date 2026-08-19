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
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

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
