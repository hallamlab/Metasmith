"""eQuilibrator compound cache -- `compounds.sqlite` + `cc_params.npz`.

THE ONE FETCHABLE SOURCE HERE THAT IS NOT A PLAIN GET, and it is worth being
explicit about why rather than pretending otherwise. `equilibrator_cache` resolves
its data through the package at first use -- there is no stable published URL to
wget, and no public "just fetch" entry point. Instantiating `ComponentContribution`
is what triggers the download, and asking it for one compound is the cheapest proof
the cache is complete rather than half-written.

The cache is primed into a scratch directory and the two artifacts are then lifted
to `equilibrator/<version>/`, because the package writes them under its own nested
cache layout and that layout is its business, not this tier's.

THE VERSION IS equilibrator-cache's PACKAGE VERSION, WHICH IS THE HONEST LABEL HERE.
Every other source in this tier is versioned by something the SERVER says. This one
has no server to ask: the two Zenodo records are named by DOIs hard-coded in the
installed package, so which bytes arrive is decided entirely by which version of
equilibrator-cache is in the env. Naming the directory after the package makes the
one variable that actually controls the download visible in the path, rather than
recording a release number the data does not have.

AND IT IS VERIFIABLE, WHICH IT WAS NOT BEFORE. The package embeds the Zenodo
records' md5 alongside each DOI, so both artifacts are checked against upstream's
declared checksum -- the same claim MetaNetX's .md5 sidecars support, and the only
one that distinguishes "primed" from "primed and complete". Both are written out as
`zenodo.md5` in md5sum(1) check format, with the DOIs as comments, so the check is
re-runnable offline later by anyone:

    cd data/originals/equilibrator/<version> && md5sum -c zenodo.md5

Two builds a year apart therefore differ in the path AND fail loudly if they claim
the same version for different bytes.

`XDG_CACHE_HOME` IS THE ONLY LEVER, and it is worth stating because the obvious-looking
one does nothing. `equilibrator_cache.zenodo.get_cached_filepath` resolves its directory
with `pooch.os_cache("equilibrator")` -- `$XDG_CACHE_HOME/equilibrator/<file>` -- and
reads no other variable; `EQUILIBRATOR_CACHE_DIR` is not consulted by
equilibrator_cache, component_contribution or pooch. Because the artifacts are lifted
OUT of that nested layout here, the direction ensemble cannot simply point the same
root back at this product: it rebuilds the `equilibrator/` level by symlink. That is a
deliberate trade -- the nested layout is the package's business, not this tier's, and
one symlink in a consumer beats an undocumented directory shape in the product.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::equilibrator.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::equilibrator"))

# The one variable pooch reads. See the module docstring: EQUILIBRATOR_CACHE_DIR looks
# like the right name and is read by nothing.
CACHE_ENV = "XDG_CACHE_HOME"
ARTIFACTS = ("compounds.sqlite", "cc_params.npz")
CHECKSUMS = "zenodo.md5"

# The package that decides which Zenodo records are fetched, and therefore what the
# version directory is named after.
PIN_PKG = "equilibrator-cache"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # NOTE for anyone editing the heredoc below: it sits inside an f-string, so a
    # literal { or } would be eaten as a field. There are none, deliberately -- the
    # checksum file is built by concatenation rather than by an f-string, and there
    # are no dict or set literals. Keep it that way.
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
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

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
