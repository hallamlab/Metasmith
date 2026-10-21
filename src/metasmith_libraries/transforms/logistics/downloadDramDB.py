from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::dram.env"))
out   = model.AddProduct(lib.GetType("annotation::dram_sources"))

KOFAM  = "https://www.genome.jp/ftp/db/kofam"
PFAM   = "https://ftp.ebi.ac.uk/pub/databases/Pfam/current_release"
SHEETS = "https://raw.githubusercontent.com/WrightonLabCSU/DRAM/master/data"

# What DRAM-setup.py 1.5.0 fetches for the databases buildDramDB selects, keyed by its
# --select_db names, over HTTPS. Its own dbCAN address, bcb.unl.edu/dbCAN2/download/,
# now redirects to an HTML page, which DRAM saved as the HMM file without complaint;
# pro.unl.edu serves the same 100.9 MB V11 file.
SOURCES = {
    "kofam_hmm":             ("kofam_profiles.tar.gz", f"{KOFAM}/profiles.tar.gz"),
    "kofam_ko_list":         ("ko_list.gz", f"{KOFAM}/ko_list.gz"),
    "pfam":                  ("Pfam-A.full.gz", f"{PFAM}/Pfam-A.full.gz"),
    "pfam_hmm":              ("Pfam-A.hmm.dat.gz", f"{PFAM}/Pfam-A.hmm.dat.gz"),
    "dbcan":                 ("dbCAN-HMMdb-V11.txt",
                              "https://pro.unl.edu/dbCAN2/download/Databases/V11/dbCAN-HMMdb-V11.txt"),
    "genome_summary_form":   ("genome_summary_form.tsv", f"{SHEETS}/genome_summary_form.tsv"),
    "module_step_form":      ("module_step_form.tsv", f"{SHEETS}/module_step_form.tsv"),
    "etc_module_database":   ("etc_module_database.tsv", f"{SHEETS}/etc_module_database.tsv"),
    "function_heatmap_form": ("function_heatmap_form.tsv", f"{SHEETS}/function_heatmap_form.tsv"),
    "amg_database":          ("amg_database.tsv", f"{SHEETS}/amg_database.tsv"),
}


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    # Run params, so a moved upstream file is fixed at launch rather than by a release.
    sources = {key: (name, context.params.get(f"dram_{key}_url", url))
               for key, (name, url) in SOURCES.items()}

    gz = [name for name, _ in sources.values() if name.endswith(".gz")]
    tsv = [name for name, _ in sources.values() if name.endswith(".tsv")]
    dbcan = sources["dbcan"][0]
    # A server that answers with a web page passes wget, so check what each file is.
    checks = [f"gzip -t {name}" for name in gz] + [
        f'[ "$(head -c 6 {dbcan})" = HMMER3 ] || {{ echo "{dbcan} is not an HMMER3 file" >&2; exit 1; }}',
        *(f"awk -F'\\t' 'NR==1 {{exit !(NF>1)}}' {name} || {{ echo \"{name} is not a TSV\" >&2; exit 1; }}"
          for name in tsv),
    ]
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(url, name, label=f"DRAM {key}") for key, (name, url) in sources.items()),
        *checks,
        f"mkdir -p {iout.local}",
        f"mv {' '.join(name for name, _ in sources.values())} {iout.local}/",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # buildDramDB reads this to name each file to DRAM-setup.py, and it records where
    # an unpinned current_release came from.
    with open(iout.local/"sources.tsv", "w") as f:
        for key, (name, url) in sources.items():
            f.write(f"{key}\t{name}\t{url}\n")

    whole = all((iout.local/name).stat().st_size > 0 for name, _ in sources.values())
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=6),
    ),
)
