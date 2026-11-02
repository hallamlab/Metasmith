from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::dram.env"))
out   = model.AddProduct(lib.GetType("annotation::dram_sources"))

KOFAM  = "https://www.genome.jp/ftp/db/kofam"
PFAM   = "https://ftp.ebi.ac.uk/pub/databases/Pfam/current_release"
DBCAN  = "https://pro.unl.edu/dbCAN2/download/Databases/V11"
VOG    = "https://fileshare.lisc.univie.ac.at/vog/vog237"
SHEETS = "https://raw.githubusercontent.com/WrightonLabCSU/DRAM/master/data"

# What DRAM-setup.py 1.5.0 fetches for the databases buildDramDB selects, keyed by its
# --select_db names, over HTTPS. Its own dbCAN address, bcb.unl.edu/dbCAN2/download/,
# now redirects to an HTML page, which DRAM saved as the HMM file without complaint;
# pro.unl.edu serves the same 100.9 MB V11 file. The two CAZy tables are the ones its
# description db reads for dbcan, at DRAM's own default V11 upload date. DRAM-v's
# distill reads VOG's functional categories, so without VOGDB it fails on every
# contig set; DRAM's default is `latest`, pinned here to the release that was current.
SOURCES = {
    "kofam_hmm":             ("kofam_profiles.tar.gz", f"{KOFAM}/profiles.tar.gz"),
    "kofam_ko_list":         ("ko_list.gz", f"{KOFAM}/ko_list.gz"),
    "pfam":                  ("Pfam-A.full.gz", f"{PFAM}/Pfam-A.full.gz"),
    "pfam_hmm":              ("Pfam-A.hmm.dat.gz", f"{PFAM}/Pfam-A.hmm.dat.gz"),
    "dbcan":                 ("dbCAN-HMMdb-V11.txt", f"{DBCAN}/dbCAN-HMMdb-V11.txt"),
    "dbcan_fam_activities":  ("CAZyDB.08062022.fam-activities.txt",
                              f"{DBCAN}/CAZyDB.08062022.fam-activities.txt"),
    "dbcan_subfam_ec":       ("CAZyDB.08062022.fam.subfam.ec.txt",
                              f"{DBCAN}/CAZyDB.08062022.fam.subfam.ec.txt"),
    "vogdb":                 ("vog.hmm.tar.gz", f"{VOG}/vog.hmm.tar.gz"),
    "vog_annotations":       ("vog.annotations.tsv.gz", f"{VOG}/vog.annotations.tsv.gz"),
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

    staged = context.StagingDir()
    # Fetch only: what each file is gets checked by buildDramDB on a compute node.
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(url, staged.local/name, label=f"DRAM {key}") for key, (name, url) in sources.items()),
        f"mkdir -p {iout.local}",
        *(f"mv {staged.local/name} {iout.local}/" for name, _ in sources.values()),
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
        # Measured on fir: 6 h 00 m, mostly Pfam-A.full (24 GB at 1.6 MB/s from EBI).
        duration=Duration(hours=24),
    ),
)
