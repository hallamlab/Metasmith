# ECMDB 2.0 -- the E. coli metabolome, acquired as a file plus a scrape.
#
# TWO PRODUCTS, AND NEITHER SUBSTITUTES FOR THE OTHER. `ecmdb.json.zip` is 3,760 metabolite
# cards carrying kegg/chebi/hmdb/m2m identifiers and structures, and it holds NO
# concentrations at all. The measurements live only in a paginated HTML browse with no API
# behind it. So the crosswalk is downloaded and the data is scraped, by
# `buildlib::ecmdb_scrape.py`.
#
# THE ZIP IS NOT UNPACKED, on this tier's standing rule -- the same reason chebi keeps its
# `.tsv.gz` gzipped and kofam keeps its tarball tarred. Unpacking is the processed tier's
# work.
#
# ECMDB PUBLISHES NO RELEASE ENDPOINT, so `RELEASE` is the version the site states for its
# current build. That is the weakest of the pin shapes in fabfos_data.yml -- weaker than
# MetaNetX's frozen tree, comparable to KEGG's quarterly number over a rolling endpoint --
# which is why the scrape's own row and metabolite counts are printed and floored rather
# than assumed. A silently-shortened browse is the failure this cannot afford: it would
# drop metabolites from the concentration table and every reaction touching them would
# quietly fall back to the 1 mM default.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
scrape = model.AddRequirement(lib.GetType("buildlib::ecmdb_scrape.py"))
out    = model.AddProduct(lib.GetType("fabfos_data::ecmdb"))

RELEASE = "2.0"
BULK_URL = "https://ecmdb.ca/download/ecmdb.json.zip"
BULK_FILE = "ecmdb.json.zip"
CONC_FILE = "concentrations.json"

# Floors, not targets. Both endpoints answer an unhappy request with a 200 and a short
# body, so each is checked against what the pinned 2026-08-24 snapshot came in at: the zip
# is 1,334,921 bytes served (10.7 MB of JSON inside) and the browse yields 1,186 rows.
MIN_BULK_BYTES = 1_000_000
MIN_CONC_ROWS = 900


def protocol(context: ExecutionContext):
    iscr = context.Input(scrape)
    iout = context.Output(out)

    cmd = f"""
        set -e
        D={iout.container}/{RELEASE}
        mkdir -p $D

        wget -q {BULK_URL} -O $D/{BULK_FILE}
        if ! unzip -t $D/{BULK_FILE} > /dev/null; then
            echo "[ecmdb] {BULK_FILE} is not a valid zip -- truncated transfer" >&2
            exit 1
        fi
        BYTES=$(stat -c%s $D/{BULK_FILE})
        echo "[ecmdb] {BULK_FILE} $BYTES bytes, zip ok"
        if [ "$BYTES" -lt {MIN_BULK_BYTES} ]; then
            echo '[ecmdb] under the {MIN_BULK_BYTES}-byte floor -- ECMDB answers an' \\
                 'unhappy request with HTTP 200 and a short body' >&2
            exit 1
        fi

        # The browse is the ONLY machine-readable source of the concentrations. It has no
        # API and no bulk endpoint, so walking it IS the acquisition.
        python3 {iscr.container} --out $D/{CONC_FILE} --min-rows {MIN_CONC_ROWS}

        md5sum $D/{BULK_FILE} | cut -d' ' -f1 > $D/{BULK_FILE}.md5
        md5sum $D/{CONC_FILE} | cut -d' ' -f1 > $D/{CONC_FILE}.md5
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    d = iout.local / RELEASE
    wanted = (BULK_FILE, CONC_FILE)
    got = [f for f in wanted if (d / f).exists() and (d / f).stat().st_size > 0]
    Log.Info(f"ecmdb {RELEASE}: {len(got)}/{len(wanted)} products")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(wanted),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    # The scrape is ~48 sequential page fetches with a courtesy sleep, so wall clock is
    # network-bound at a couple of minutes and nothing here is parallel.
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=30)),
)
