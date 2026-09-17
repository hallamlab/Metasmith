# Pratama's MAG annotation (reproduction_map A14), the Pfam half. See dram_kofam_pratama.py for why the
# annotator is selected through a filtered config and why the staged DRAM has only these two annotators.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::dram.env"))
db    = model.AddRequirement(lib.GetType("annotation::dram_db"))
asm   = model.AddRequirement(lib.GetType("sequences::spades_assembly"))
bins  = model.AddRequirement(lib.GetType("sequences::metawrap_bin_fasta"), parents={asm})

out_annot = model.AddProduct(lib.GetType("e3::mag_dram_pfam_annotations"))

KEEP = "pfam"

MKCONFIG = """import json, sys
keep = set(sys.argv[1].split(","))
c = json.load(open("/db/DRAM.config"))
sd = c.get("search_databases") or {}
missing = [k for k in keep if not sd.get(k)]
assert not missing, f"DRAM.config has no path for {missing}; staged databases are {[k for k, v in sd.items() if v]}"
c["search_databases"] = {k: (v if k in keep else None) for k, v in sd.items()}
json.dump(c, open(sys.argv[2], "w"))
print("annotating against:", sorted(k for k, v in c["search_databases"].items() if v))
"""


def protocol(context: ExecutionContext):
    idb = context.Input(db)
    iannot = context.Output(out_annot)
    threads = context.params.get("cpus", 8)

    mags = Path("mags")
    mags.mkdir(exist_ok=True)
    n = 0
    for i, handle in enumerate(context.InputGroup(bins)):
        (mags / f"bin_{i}.fa").write_bytes(handle.local.read_bytes())
        n += 1
    assert n > 0, "no refined MAGs to annotate"
    Log.Info(f"annotating {n} MAGs against Pfam")

    Path("mkconfig.py").write_text(MKCONFIG)
    context.ExecWithEnv(env=image, binds=[(idb.external, "/db")], cmd=f"""
        export HOME=/tmp
        python3 mkconfig.py {KEEP} dram_pfam.config
        DRAM.py annotate -i 'mags/*.fa' -o dram_annot --threads {threads} --min_contig_size 1000 \
            --config_loc dram_pfam.config
        test -s dram_annot/annotations.tsv
    """)
    context.LocalShell(f"cp dram_annot/annotations.tsv {iannot.local}")
    return ExecutionResult(manifest=[{out_annot: iannot.local}], success=iannot.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    # Pfam is an MMseqs2 profile search. The profile db is 161 MB but its MSA is 133 GB on disk, so the
    # request is wide on memory rather than long on wall.
    resources=Resources(cpus=16, memory=Size.GB(96), duration=Duration(hours=20)),
)
