from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::kegg"))

ENDPOINTS = {
    "ko2reaction.tsv": "https://rest.kegg.jp/link/reaction/ko",
}

INFO_URL    = "https://rest.kegg.jp/info/kegg"
INFO_FILE   = "kegg-info.txt"
RELNOTE_URL = "https://www.genome.jp/kegg/docs/relnote.html"

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
    context.ExecWithEnv(env=image, cmd=_cmd)

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
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=15)),
)
