# Pratama's AMG calls, stage 4 of 5: DRAM-v annotate against Pfam only.
# See dram_kofam_pratama.py for why the annotator is chosen through a filtered config.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_dram = model.AddRequirement(lib.GetType("env::dram.env"))
dram_db  = model.AddRequirement(lib.GetType("annotation::dram_db"))
combined = model.AddRequirement(lib.GetType("e3::dramv_checkv_combined"))
contigs  = model.AddRequirement(lib.GetType("e3::dramv_prep_contigs"), parents={combined})
affi     = model.AddRequirement(lib.GetType("e3::dramv_prep_affi"), parents={combined})

out_annot = model.AddProduct(lib.GetType("e3::dramv_pfam_annotations"))

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
    idram = context.Input(dram_db)
    icontigs = context.Input(contigs)
    iaffi = context.Input(affi)
    iannot = context.Output(out_annot)
    threads = context.params.get("cpus", 8)

    Path("mkconfig.py").write_text(MKCONFIG)
    context.ExecWithEnv(env=img_dram, binds=[(idram.external, "/db")], cmd=f"""
        export HOME=/tmp
        python3 mkconfig.py {KEEP} dramv_pfam.config
        # description_db is null in the staged config; DRAM's own pfam path guards on the handler
        # object, not its session, and crashes instead of degrading. Built per task, not into the
        # shared refs dir, whose mtime is a cache key that would re-key 65 done dram_kofam shards
        # (398.9 measured wall-hours).
        DRAM-setup.py update_description_db --config_loc dramv_pfam.config --output_loc $PWD/description_db.sqlite --select_db pfam
        DRAM-v.py annotate -i {icontigs.container} -v {iaffi.container} \
            -o dramv_annot --threads {threads} --min_contig_size 1000 --config_loc dramv_pfam.config
        test -s dramv_annot/annotations.tsv
    """)
    context.LocalShell(f"cp dramv_annot/annotations.tsv {iannot.local}")
    return ExecutionResult(manifest=[{out_annot: iannot.local}], success=iannot.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=combined,
    resources=Resources(cpus=16, memory=Size.GB(96), duration=Duration(hours=20)),
)
