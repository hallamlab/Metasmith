from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::equilibrator.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::equilibrator"))

CACHE_ENV = "XDG_CACHE_HOME"
ARTIFACTS = ("compounds.sqlite", "cc_params.npz")
CHECKSUMS = "zenodo.md5"

PIN_PKG = "equilibrator-cache"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        mkdir -p _eq_prime
        export {CACHE_ENV}=$(pwd -P)/_eq_prime
        python3 - <<'PY'
from importlib.metadata import version
from equilibrator_cache.zenodo import DEFAULT_COMPOUND_CACHE_SETTINGS as CACHE
from component_contribution import DEFAULT_CC_PARAMS_SETTINGS as PARAMS

# Written before the download so a failed prime still leaves the intended version
# and the expected checksums on disk to diagnose against.
with open("_eq_ver", "w") as fh:
    fh.write(version("{PIN_PKG}"))
with open("_eq_md5", "w") as fh:
    for s in (CACHE, PARAMS):
        fh.write("# " + s.doi + "\\n")
        fh.write(s.md5 + "  " + s.filename + "\\n")

from equilibrator_api import ComponentContribution
cc = ComponentContribution()
w = cc.get_compound("kegg:C00001")
print("[equilibrator] cache primed;", "water resolved" if w is not None else "water MISSING")
raise SystemExit(0 if w is not None else 1)
PY
        VER=$(cat _eq_ver)
        if [ -z "$VER" ]; then
            echo '[equilibrator] {PIN_PKG} reported no version -- the package pins the' \\
                 'Zenodo DOIs, so without it there is nothing identifying this cache' >&2
            exit 1
        fi
        D={iout.container}/$VER
        mkdir -p $D
        mv _eq_md5 $D/{CHECKSUMS}
        echo "[equilibrator] {PIN_PKG} $VER"

        for f in {" ".join(ARTIFACTS)}; do
            src=$(find _eq_prime -name "$f" -type f | head -1)
            if [ -z "$src" ]; then
                echo "[equilibrator] the package did not produce $f" >&2
                exit 1
            fi
            mv "$src" $D/$f
            echo "[equilibrator] $f $(du -h $D/$f | cut -f1)"
        done
        ( cd $D && md5sum -c {CHECKSUMS} )
        rm -rf _eq_prime
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    vers = sorted(p for p in iout.local.glob("*") if p.is_dir())
    wanted = ARTIFACTS + (CHECKSUMS,)
    got = [f for v in vers for f in wanted
           if (v / f).exists() and (v / f).stat().st_size > 0]
    Log.Info(f"equilibrator: {len(got)}/{len(wanted)} artifacts in "
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
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(hours=2)),
)
