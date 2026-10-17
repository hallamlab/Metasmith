import shlex

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::metabuli.env"))
ref   = model.AddProduct(lib.GetType("ref::metabuli_ref"))

# Pinned, because `metabuli databases GTDB` is not. It resolves whatever the
# upstream index calls current, so two runs of the same transform can produce
# different databases and the taxonomy they assign is not comparable. gtdb232 is
# also the release env/metabuli.env's 1.2.0 floor exists for -- 1.1.0 cannot read
# its format at all -- and the release GTDB-Tk is pinned to in downloadGtdbDB.py,
# so bin-level and contig-level taxonomy agree.
DB_URL = "https://steineggerlab.s3.amazonaws.com/metabuli/gtdb232.tar.gz"

# 634 GB packed and 744 GB unpacked, about 9 h at a fir login node's rate. Staging the
# archive would need 1.4 TB at the peak, so it is streamed into tar and never lands. A
# dropped connection resumes with a Range request from the byte count that reached tar;
# curl's own --retry is off because it resumes from an offset it only believes it wrote.
# Upstream publishes no checksum, so gzip's CRC at the end of the stream is the check.
STREAM = r"""
total=$(curl -sSfIL --connect-timeout 60 "$url" | tr -d '\r' \
    | awk 'tolower($1)=="content-length:" {n=$2} END {print n}')
[ -n "$total" ] || { echo "no Content-Length for $url" >&2; exit 1; }
echo "streaming $total bytes from $url" >&2
got=0
idle=0
while [ "$got" -lt "$total" ]; do
    rc=0
    curl -sS -L --fail --retry 0 --connect-timeout 60 --speed-limit 1024 --speed-time 300 \
        -r "$got-" -w '%{stderr}@@ %{http_code} %{size_download}\n' "$url" 2> curl.err || rc=$?
    read -r _ code n < <(grep '^@@ ' curl.err | tail -n1) || { code=000; n=0; }
    n=${n%.*}
    grep -v '^@@ ' curl.err >&2 || true
    if [ "$rc" = 23 ]; then echo "tar stopped reading at byte $got" >&2; exit 1; fi
    if [ "$n" -gt 0 ] && [ "$got" -gt 0 ] && [ "$code" != 206 ]; then
        echo "a resume at byte $got was answered $code, not 206" >&2; exit 1
    fi
    got=$((got + n))
    echo "$got of $total bytes (curl exit $rc, HTTP $code)" >&2
    if [ "$n" -gt 0 ]; then idle=0; continue; fi
    idle=$((idle + 1))
    [ "$idle" -le 10 ] || { echo "ten attempts in a row moved nothing" >&2; exit 1; }
    sleep $((idle * 30))
done
"""


def protocol(context: ExecutionContext):
    idb = context.Output(ref)
    url = context.params.get("metabuli_db_url", DB_URL)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"url={shlex.quote(url)}",
        "mkdir -p extract",
        "{",
        STREAM,
        "} | tar -xzf - -C extract",
        # The tarball's nesting has changed between releases (gtdb232 wraps everything
        # in gtdb+human+virus/), and metabuli is given this directory verbatim -- so
        # the directory holding db.parameters becomes the product, whatever its depth.
        "params=$(find extract -maxdepth 3 -name db.parameters -print -quit)",
        '[ -n "$params" ] || { echo "the archive holds no db.parameters" >&2; exit 1; }',
        f'mv "$(dirname "$params")" {idb.local}',
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # Assert the extracted database, not the command's exit code. The r1 campaign
    # burned a day on exactly this: its verify stage ran `metabuli databases` and
    # read the return value, which said nothing about whether any data landed.
    # The alternatives are the ones `metabuli classify` 1.2.0 accepts at load.
    def present(*names):
        return all((idb.local/n).is_file() and (idb.local/n).stat().st_size > 0 for n in names)
    whole = present("db.parameters") \
        and (present("diffIdx", "info", "split") or present("deltaIdx.mtbl", "deltaIdxSplits.mtbl")) \
        and (present("taxonomyDB") or present("taxonomy/merged.dmp"))
    return ExecutionResult(
        manifest=[{ref: idb.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=24),
    ),
)
