from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::kofam"))

BASE_URL = "https://www.genome.jp/ftp/db/kofam"
FILES = ("profiles.tar.gz", "ko_list.gz", "README")

STAMP_FILE = "profiles.tar.gz"

STAGE = "_incoming"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        S={iout.container}/{STAGE}
        mkdir -p $S
        for f in {" ".join(FILES)}; do
            wget -q -c {BASE_URL}/$f -O $S/$f
        done
        gzip -t $S/ko_list.gz
        gzip -t $S/{STAMP_FILE}

        # Bytes 4..7 of a gzip stream are the MTIME field, little-endian unix time.
        N=$(od -An -tu4 -j4 -N4 --endian=little $S/{STAMP_FILE} | tr -d ' ')
        if [ -n "$N" ] && [ "$N" -gt 0 ]; then
            VER=$(date -u -d @$N +%F)
            echo "[kofam] build $VER, from the gzip header of {STAMP_FILE}"
        else
            # gzip -n zeroes the field. Never observed on this FTP, but if it happens
            # the server's mtime is the only thing left -- weaker, and worth saying so
            # out loud rather than silently naming a directory after 1970.
            LM=$(wget --spider --server-response {BASE_URL}/{STAMP_FILE} 2>&1 \\
                 | grep -i 'last-modified' | tail -1 | tr -d '\\r' \\
                 | sed -e 's/^.*[Ll]ast-[Mm]odified:[[:space:]]*//')
            VER=$(date -u -d "$LM" +%F)
            echo "[kofam] WARNING: no gzip timestamp; falling back to the server's" \\
                 "Last-Modified ($VER), which a re-sync can move without a rebuild" >&2
        fi
        if [ -z "$VER" ]; then
            echo '[kofam] no build date from either source -- an unlabelled snapshot is' \\
                 'what this directory layout exists to prevent' >&2
            exit 1
        fi

        D={iout.container}/$VER
        mkdir -p $D
        for f in {" ".join(FILES)}; do
            mv $S/$f $D/$f
        done
        rmdir $S
        echo "[kofam] $VER: $(du -sh $D | cut -f1), both archives verified as gzip"
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    vers = sorted(p for p in iout.local.glob("*") if p.is_dir() and p.name != STAGE)
    got = [f for v in vers for f in FILES
           if (v / f).exists() and (v / f).stat().st_size > 0]
    Log.Info(f"kofam: {len(got)}/{len(FILES)} files in "
             f"{', '.join(v.name for v in vers) or '(no version dir)'}, compressed as served")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(hours=4)),
)
