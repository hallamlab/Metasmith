# Pratama's MAG annotation (reproduction_map A14), the KOfam half. One annotator per transform, so a
# failure costs one annotator rather than the whole sample's annotation, and each wall is sized for one
# search. `DRAM.py annotate --min_contig_size 1000`, then distill in its own transform.
#
# CAUTION the staged DRAM 1.5.0 has exactly TWO live annotators. Its DRAM.config lists thirteen
# `search_databases` and eleven are null: kegg, uniref, dbcan, viral, peptidase, vogdb and the four camper
# entries. Only kofam and pfam are set, so this split is kofam / pfam / distill and nothing else. That
# supersedes BLOCKERS B3, which recorded only the missing dbCAN.
#
# The annotator is selected by writing a config that keeps only its own databases, because DRAM chooses
# what to search from the config rather than from a flag.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::dram.env"))
db    = model.AddRequirement(lib.GetType("annotation::dram_db"))
asm   = model.AddRequirement(lib.GetType("sequences::spades_assembly"))
bins  = model.AddRequirement(lib.GetType("sequences::metawrap_bin_fasta"), parents={asm})

out_annot = model.AddProduct(lib.GetType("e3::mag_dram_kofam_annotations"))

KEEP = "kofam_hmm,kofam_ko_list"

# Written to a file rather than inlined, so the protocol's f-string never has to escape its braces.
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
    Log.Info(f"annotating {n} MAGs against KOfam")

    Path("mkconfig.py").write_text(MKCONFIG)
    context.ExecWithEnv(env=image, binds=[(idb.external, "/db")], cmd=f"""
        export HOME=/tmp
        python3 mkconfig.py {KEEP} dram_kofam.config
        DRAM.py annotate -i 'mags/*.fa' -o dram_annot --threads {threads} --min_contig_size 1000 \
            --config_loc dram_kofam.config
        test -s dram_annot/annotations.tsv
    """)
    context.LocalShell(f"cp dram_annot/annotations.tsv {iannot.local}")
    return ExecutionResult(manifest=[{out_annot: iannot.local}], success=iannot.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    # KOfam is an HMM search against 7.2 GB of profiles, the heavier of the two annotators.
    # CAUTION duration must satisfy base x 2^(tries-1) <= 168 h; fir refuses any job over 7.0 days at
    # submit time, and the 24 h monolith this replaces asked an illegal 192 h on rung 4.
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=20)),
)
