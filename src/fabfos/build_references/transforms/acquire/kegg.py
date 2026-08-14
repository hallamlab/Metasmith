"""KEGG REST -- the KO -> reaction map, saved as the endpoint returned it.

One request, one file: `kegg/<release>/ko2reaction.tsv`, byte-for-byte the body of

    https://rest.kegg.jp/link/reaction/ko     ~245 KB, ~2 s

WHAT THIS FILE IS NOW, AND WHAT IT USED TO BE. The previous version of this
transform downloaded the same response and then did three more things to it: strip
the `ko:` / `rn:` prefixes, add a header, and INNER-JOIN it against KOfam's ko_list
to drop KOs with no profile. The artifact it wrote was therefore not attributable
to KEGG at all -- it was a KEGG/KOfam join, sitting in the acquisition tier,
carrying a row count (12,238) that no upstream URL would ever return.

Those three steps still have to happen; they belong to the processed tier, which is
also where the KOfam dependency belongs. Removing them from here is what lets this
transform have no requirement other than its env: acquisition of one source should
not need another source present.

The saved form is the served form -- `ko:K00001<TAB>rn:R00623`, 12,276 lines, no
header.

VERSIONING A CONTINUOUSLY-UPDATED ENDPOINT, which is genuinely awkward and worth
being straight about. KEGG numbers quarterly releases (119.0, 1 July 2026) but REST
serves the live database, which is what KEGG's own notation calls `119.0+` -- the
release plus every incremental change since. So the directory name is the release
the pull falls under, and it is a WEAKER claim than metanetx/4.5: two pulls three
weeks apart both land in `119.0/` and need not be identical.

What closes that gap is `kegg-info.txt`, the verbatim body of `rest.kegg.jp/info/kegg`,
saved alongside. It carries a per-database "Last update" date and entry count -- for
this file, `ko` and `reaction` -- so two same-release pulls are still distinguishable,
by exactly the dates that would make them differ. It is a served endpoint body, not
something computed here, so it belongs in this tier for the same reason Rhea's
rhea-release.properties and MetaNetX's .md5 sidecars do.

The release NUMBER is not in the REST API at all -- `info/kegg` stopped carrying its
`Release N.N+/MM-DD` line -- so it is read from the release-note page instead, which
lists releases newest first.

WHY THE BULK ENDPOINT AND NOT THE DEPLOYED CRAWL. The deployed builder fetched one
`get/<ko>` flat-file per KO and parsed its REACTION block, scoped to the KOs its
hosts called, out of a 185 MB licensed sqlite cache. That cache is not
redistributable and the lanes.yml defining the scope is gone, so the method is
unreproducible here. Measured against 3,854 cached flat-files, the bulk endpoint
agrees on 99.6% of KOs (12 where bulk has more, 4 where the cache does --
consistent with KEGG updates, not with a parsing difference). It is also far wider:
11,723 pairs / 5,950 KOs against the deployed 2,634 / 1,312.

The consequence to carry downstream: the kofam lane reaches ~6.5x the reactions it
did in the deployed build, so its numbers are NOT comparable to the archived ones.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::kegg"))

ENDPOINTS = {
    "ko2reaction.tsv": "https://rest.kegg.jp/link/reaction/ko",
}

# The release marker, kept in the product. See the versioning note above.
INFO_URL    = "https://rest.kegg.jp/info/kegg"
INFO_FILE   = "kegg-info.txt"
RELNOTE_URL = "https://www.genome.jp/kegg/docs/relnote.html"

# A floor on the response, not a target, and not a filter. KEGG answers a malformed
# or unsupported query with HTTP 200 and an EMPTY BODY, so "succeeded" and "returned
# nothing" are the same status code. A bridge built from an empty response raises
# nothing anywhere downstream -- it is simply a kofam lane that annotates nothing.
# Set well below the 12,276 observed so ordinary growth or shrinkage does not trip it.
MIN_LINES = 5000


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    fetches = "\n".join(
        f"        wget -q '{url}' -O $D/{name}\n"
        f"        n=$(wc -l < $D/{name})\n"
        f"        echo \"[kegg] {name}: $n lines\"\n"
        f"        if [ \"$n\" -lt {MIN_LINES} ]; then\n"
        f"            echo '[kegg] under the {MIN_LINES}-line floor -- KEGG returns an empty'\\\n"
        f"                 'body as HTTP 200, so this is checked rather than trusted' >&2\n"
        f"            exit 1\n"
        f"        fi"
        for name, url in ENDPOINTS.items())

    _cmd = f"""
        set -e
        REL=$(wget -qO- {RELNOTE_URL} | grep -oiE 'Release [0-9]+\\.[0-9]+' | head -1 | cut -d' ' -f2)
        if [ -z "$REL" ]; then
            echo '[kegg] no release number on the release-note page -- refusing to write' \\
                 'an unlabelled snapshot of a database that changes daily' >&2
            exit 1
        fi
        D={iout.container}/$REL
        mkdir -p $D
        wget -q {INFO_URL} -O $D/{INFO_FILE}
        echo "[kegg] release $REL+ (REST serves the live database; see {INFO_FILE})"
        grep -E '^\\s+(ko|reaction)\\s' $D/{INFO_FILE} || true
{fetches}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    vers = sorted(p for p in iout.local.glob("*") if p.is_dir())
    wanted = list(ENDPOINTS) + [INFO_FILE]
    got = [f for v in vers for f in wanted
           if (v / f).exists() and (v / f).stat().st_size > 0]
    Log.Info(f"kegg: {len(got)}/{len(wanted)} files in "
             f"{', '.join(v.name for v in vers) or '(no version dir)'}, verbatim")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(wanted),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    # One request. The 8 h this used to declare was for a 28k-KO crawl that no longer
    # happens; the headroom is for KEGG being slow.
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=15)),
)
