"""KOfam -- the HMM profile database, as shipped, into `kofam/<build-date>/`.

Three files, none of them opened: `profiles.tar.gz`, `ko_list.gz`, `README`.

THE ARCHIVE IS NOT UNTARRED AND THE LIST IS NOT GUNZIPPED. Both used to happen
here, and the gunzip in particular is why this file changed: the acquire step
fetched `ko_list.gz` and wrote out the DECOMPRESSED `ko_list`, so what sat in the
acquisition tier was not a thing upstream ever served. The unpacking belongs to
compile/kofam_ref.py, which already untars the profiles with `--strip-components=1`
because kofamscan wants a profile DIRECTORY.

Keeping both compressed is also what lets the unpack be re-run -- for a new
kofamscan, a new directory layout -- without re-fetching 1.5 GB.

WHAT ko_list ACTUALLY IS, because the name misleads: not a KEGG KO registry, but
the per-profile scoring table. Ten of its twelve columns are properties of the HMMs
(`threshold`, `score_type`, `profile_type`, `F-measure`, and the build statistics
`nseq`/`nseq_used`/`alen`/`mlen`/`eff_nseq`/`re/pos`); only `knum` and `definition`
are KEGG-ish. It is generated when the profiles are built and released with them,
which is why it lives HERE and not under kegg/ -- pairing a ko_list with profiles
from a different release applies the wrong threshold to every hit, silently.

THE VERSION IS THE GZIP HEADER'S MTIME, NOT THE HTTP `Last-Modified`. KOfam
publishes no release number, no checksums, and a README unchanged since 2019, so
the only identifying facts are timestamps -- and there are three, which are not
interchangeable:

    tar member mtimes   2026-06-28 18:32   when the .hmm files were built
    gzip header mtime   2026-06-30 01:09   when the tarball was compressed
    HTTP Last-Modified  2026-06-30 01:15   when the web server's copy was written

`Last-Modified` is the weakest: it is the server's filesystem mtime, so a re-sync,
a mirror or a restore from backup moves it without a rebuild, and it is visible
only at fetch time -- a file already on disk cannot be re-identified from itself.
The gzip MTIME field is written into the first ten bytes of the stream by gzip(1)
at compression time, so it travels WITH the artifact: the directory name stays
re-derivable offline, by anyone, from the file it names.

    od -An -tu4 -j4 -N4 --endian=little profiles.tar.gz    # -> unix time

The tar member mtimes are the truest build date but cost a decompression pass to
read, and they disagree with each other (`profiles/` is stamped four hours after
the .hmm files it holds), so there is no single one to name a directory after.

`ko_list.gz` is stamped a day earlier than the profiles (2026-06-29 against
2026-06-30) because the two are compressed in sequence, so the profiles' stamp
names the pair.

WHY NOT `archives/<date>/`, WHICH LOOKS LIKE THE OBVIOUS PIN. Those directories are
named by publication, not by build: `archives/2026-05-01/` holds a tarball whose
gzip header says 2026-05-29. So their names identify the same thing this does, less
accurately. They also lag -- the newest archive is 2026-05-01 while the live tree is
a 2026-06-30 build -- so pinning there ships a KO set two releases stale to gain
nothing. They remain the way to re-fetch an OLD build, which a timestamp cannot do.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::kofam"))

BASE_URL = "https://www.genome.jp/ftp/db/kofam"
FILES = ("profiles.tar.gz", "ko_list.gz", "README")

# The file whose gzip stamp names the release. See the note above on why it is the
# profiles and not the list.
STAMP_FILE = "profiles.tar.gz"

# Staged INSIDE the product directory so the promotion below is a rename on the same
# filesystem rather than a 1.5 GB copy.
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
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    # The version directory is discovered rather than declared: only the shell above
    # knows what the artifact said, so this reads back what it created.
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
