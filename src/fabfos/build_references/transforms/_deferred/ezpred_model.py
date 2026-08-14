"""R9 -- the EZpred DL-only bundle the EC-head lane runs.

**Requires:** `originals/ezpred/` (the whole source folder) + `buildlib::ezpred_src`.

TWO HALVES FROM TWO PLACES, and the split is the tier rule rather than convenience:

    the CODE     `buildlib::ezpred_src` -- the vendored EZpred tree with our DL-only
                 patch. Patched code is not what any URL returns, so it cannot sit
                 in the originals tier; it is a build-side resource, pinned to an
                 upstream revision, with its patch notes beside it.
    the WEIGHTS  `originals/ezpred/<records>/{models.zip, Data2.zip}` -- Zenodo
                 artifacts, byte-for-byte as served.

`ref::ezpred_model` is the two assembled: the tree with `models/{enzyme,nonenzyme}/`
and the label IA tables in place, which is the directory `functionalAnnotation/
ezpred.py` bind-mounts at /work/EZpred so `settings.py`'s root_dir resolves.

UNZIP EXITS 0 ON A MEMBER THAT IS NOT THERE. `unzip -j archive 'some/path'` prints
"caution: filename not matched" and returns 0. Every extract here is checked by
looking for the files afterwards, never by the exit code -- a partial unpack yields
a bundle that imports cleanly and then predicts from three of five ensemble members,
which is a quieter wrong answer than a crash.

THE ENSEMBLE COUNT IS ASSERTED. Five members per head is what the DL-only path
averages over; four is a different model with the same name.

THE ESM-C WEIGHTS ARE NOT IN HERE. Upstream's `predict.py` re-embeds each FASTA with
its own `fasta2plm.py`; ours consumes the embeddings `functionalAnnotation/esm_c.py`
already produced, so the 600M pass runs once for both the ESM-C GPR lane and this
one. That is also why R8 exists as its own reference rather than as a file inside
this bundle.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
source = model.AddRequirement(lib.GetType("fabfos_data::ezpred"))
src    = model.AddRequirement(lib.GetType("buildlib::ezpred_src"))
bundle = model.AddProduct(lib.GetType("ref::ezpred_model"))

# Five per head is what the DL-only path averages over.
ENSEMBLE_MEMBERS = 5
HEADS = ("enzyme", "nonenzyme")
# The label IA tables predict.py reads out of Data2.zip.
IA_TABLES = ("Data/network_training_data1/IA.txt", "Data/network_training_data2/IA.txt")

DRIVER = r'''
import shutil, subprocess, sys
from pathlib import Path

ROOT = Path("{source}")
SRC = Path("{src}")
OUT = Path("{out}")

subs = sorted(p for p in ROOT.iterdir() if p.is_dir())
if len(subs) != 1:
    raise SystemExit(
        f"[ezpred] expected exactly one release under {{ROOT}}, found {{len(subs)}} "
        f"({{[p.name for p in subs]}}). The heads' output columns are indexed by the "
        f"IA tables' label order, so a models.zip paired with another release's "
        f"Data2.zip is a silent relabelling of every prediction")
REL = subs[0]
print(f"[ezpred] release {{REL.name}}", flush=True)

OUT.mkdir(parents=True, exist_ok=True)
# The patched tree first; the archives unpack into it.
for p in sorted(SRC.iterdir()):
    dest = OUT / p.name
    if p.is_dir():
        shutil.copytree(p, dest, dirs_exist_ok=True)
    else:
        shutil.copyfile(p, dest)

def unpack(archive, members=None):
    """unzip, then CHECK -- its exit code says nothing about whether a selective
    extract matched anything."""
    cmd = ["unzip", "-qo", str(REL / archive), "-d", str(OUT)]
    if members:
        cmd = ["unzip", "-qoj", str(REL / archive)] + list(members) + ["-d", str(OUT)]
    subprocess.run(cmd, check=True)

unpack("models.zip")
for head in {heads}:
    d = OUT / "models" / head
    if not d.is_dir():
        raise SystemExit(
            f"[ezpred] models.zip left no models/{{head}}/ -- unzip returns 0 on a "
            f"member it did not match, so this is checked by looking rather than by "
            f"the exit code")
    n = len([p for p in d.iterdir() if p.is_file()])
    if n < {members}:
        raise SystemExit(
            f"[ezpred] models/{{head}}/ holds {{n}} files, expected at least "
            f"{{{members}}}. A partial unpack yields a bundle that imports cleanly "
            f"and then predicts from a smaller ensemble than the one it names")
    print(f"[ezpred] models/{{head}}: {{n}} files", flush=True)

# Data2.zip is ~GB and only two tables are read; extract those into place.
for rel in {ia_tables}:
    dest = OUT / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["unzip", "-qoj", str(REL / "Data2.zip"), rel,
                    "-d", str(dest.parent)], check=True)
    if not dest.exists():
        raise SystemExit(
            f"[ezpred] Data2.zip has no {{rel}} -- predict.py indexes its output "
            f"columns off that table, so an absent one is a relabelling, not a gap")
    print(f"[ezpred] {{rel}}  {{dest.stat().st_size:,}} bytes", flush=True)

print(f"[ezpred] bundle at {{OUT}}", flush=True)
'''


def protocol(context: ExecutionContext):
    isrc_data = context.Input(source)
    isrc_code = context.Input(src)
    iout = context.Output(bundle)
    driver = DRIVER.format(
        source=isrc_data.container, src=isrc_code.container, out=iout.container,
        heads=repr(HEADS), members=ENSEMBLE_MEMBERS, ia_tables=repr(IA_TABLES),
    )
    context.LocalShell("cat > _ezpred_model.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _ezpred_model.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _ezpred_model.py")

    ok = ((iout.local / "predict.py").exists()
          and all((iout.local / "models" / h).is_dir() for h in HEADS))
    return ExecutionResult(
        manifest=[{bundle: iout.local}],
        success=ok,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)
