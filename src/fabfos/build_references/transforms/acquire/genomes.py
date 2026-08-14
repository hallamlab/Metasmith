"""The fosmid host set -- assemblies, proteomes and published curated models.

One folder, `genomes/<host>/`, holding `genome/` (the NCBI assembly) and, where the
strain has one, `GEM/` (the published BiGG model).

REPLACES THREE TRANSFORMS: host_accessions, host_genome and host_gem. Those existed
because the old tier typed one product per FILE and needed a scatter to fan an
accession out to three hosts. With the folder as the product the scatter has nothing
to carry -- the host set is internal to this file, which is where it should be
anyway. A driver that passes accessions in can pass a different three, and then two
runs of "the reference build" are not the same build.

THE HOST SET IS A FACT IN THIS FILE. Moving a host means moving this file.

EPI300 HAS NO GEM ENTRY, AND THAT IS NOT AN OMISSION. It has no published model of
its own and borrows DH10B's, which a measured, EMPTY edit list licenses. But the
borrow is not a download: writing DH10B's JSON a second time under EPI300 would
assert an acquisition that never happened and put identical bytes at two paths
claiming two provenances. The borrow belongs to the processed tier, where it is a
statement about models rather than about files. Any GEM-side comparison of the two
strains is null by construction either way.

WHY NOT THE SHIPPED logistics/getNcbiAssembly. That directory also carries
downloadKofamDB and downloadUniRef50DB, which produce ref::kofamscan_* and
ref::uniref50_diamond_db -- exactly the types compile/kofam_ref.py and
compile/uniref50_dmnd.py produce. Loading it would give the planner two producers
for each and let a TIEBREAK decide which one built a reference.

THE ONE PLACE THIS TIER RENAMES. Every other acquire transform writes upstream's
bytes at upstream's name. NCBI serves one `ncbi_dataset.zip` whose interior layout
is a `datasets` implementation detail (`ncbi_dataset/data/<acc>/...`, with names
like `protein.faa` that collide across hosts), so the files are lifted out and named
by the sequence accession of the primary record -- `NZ_` stripped, since RefSeq's
`NZ_CP189566.1` and INSDC's `CP189566.1` are the same sequence and the bare form is
what the rest of the tree uses. Bytes are untouched; only the path is chosen here.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

ncbi_env = model.AddRequirement(lib.GetType("env::ncbi-datasets.env"))
py_env   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out      = model.AddProduct(lib.GetType("fabfos_data::genomes"))

# host -> NCBI assembly accession.
#
# EPI300 is GCF_052692645.1 (ASM5269264v1, complete genome, Lucigen material). NOT
# GCF_051228345.1, which was carried here previously and does not exist at NCBI at
# all -- the neighbouring GCA_051228345.1 that DOES exist is a Salmonella assembly,
# so a fetch under it would either 404 or, far worse, build the E. coli references
# off the wrong organism.
HOSTS = {
    "e_coli_k12":    "GCF_000005845.2",
    "e_coli_dh10b":  "GCF_000019425.1",
    "e_coli_epi300": "GCF_052692645.1",
}

# host -> the BiGG model that strain's GPR is asserted by. Absent means the strain
# has no published model; see the EPI300 note above.
GEM_FOR_HOST = {
    "e_coli_k12":   "iML1515",           # E. coli K-12 MG1655
    "e_coli_dh10b": "iECDH10B_1368",     # E. coli DH10B
}

BIGG_URL = "http://bigg.ucsd.edu/static/models/{model}.json"

# gff3 is deliberately not requested: nothing in the build consumes it, and the four
# -include kinds are not free -- gbff alone is ~12 MB per host.
INCLUDE = "protein,genome,gbff"


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    fetch = "\n".join(f"""
        H={host}; ACC={acc}
        rm -rf _dl_$H && mkdir -p _dl_$H {iout.container}/$H/genome
        ( cd _dl_$H && datasets download genome accession $ACC --include {INCLUDE} \\
              && unzip -o -q ncbi_dataset.zip )
        # `cds_from_genomic.fna` also ends in genomic.fna and is NOT the assembly.
        # Taking it would make the background reference a CDS set, and every host
        # filter would silently stop matching intergenic sequence.
        FNA=$(find _dl_$H/ncbi_dataset -name '*genomic.fna' ! -name 'cds_from*' | head -1)
        FAA=$(find _dl_$H/ncbi_dataset -name 'protein.faa' | head -1)
        GBK=$(find _dl_$H/ncbi_dataset -name 'genomic.gbff' | head -1)
        if [ -z "$FNA" ] || [ -z "$FAA" ] || [ -z "$GBK" ]; then
            echo "[genomes] $H ($ACC): missing one of fna/faa/gbk in the zip" >&2
            exit 1
        fi
        SEQ=$(head -1 "$FNA" | cut -c2- | cut -d' ' -f1 | sed 's/^NZ_//')
        cp "$FNA" {iout.container}/$H/genome/$SEQ.fna
        cp "$FAA" {iout.container}/$H/genome/$SEQ.faa
        cp "$GBK" {iout.container}/$H/genome/$SEQ.gbk
        rm -rf _dl_$H
        echo "[genomes] $H $ACC -> $SEQ"
    """ for host, acc in sorted(HOSTS.items()))

    context.ExecWithEnv() \
        .ifContainerDo(env=ncbi_env, cmd=f"set -e\n{fetch}") \
        .ifVirtualEnvDo(env=ncbi_env, cmd=f"set -e\n{fetch}")

    # A separate call, and a separate env: the BiGG models are a plain wget and the
    # ncbi-datasets env exists to carry `datasets`. Mixing them would make this
    # transform's env declaration mean "whatever both happen to contain".
    gems = "\n".join(f"""
        mkdir -p {iout.container}/{host}/GEM
        wget -q {BIGG_URL.format(model=m)} -O {iout.container}/{host}/GEM/{m}.json
        echo "[genomes] {host} GEM -> {m}"
    """ for host, m in sorted(GEM_FOR_HOST.items()))

    context.ExecWithEnv() \
        .ifContainerDo(env=py_env, cmd=f"set -e\n{gems}") \
        .ifVirtualEnvDo(env=py_env, cmd=f"set -e\n{gems}")

    ok_g = [h for h in HOSTS
            if len(list((iout.local / h / "genome").glob("*"))) == 3]
    ok_m = [h for h, m in GEM_FOR_HOST.items()
            if (iout.local / h / "GEM" / f"{m}.json").exists()]
    Log.Info(f"genomes: {len(ok_g)}/{len(HOSTS)} host genomes, "
             f"{len(ok_m)}/{len(GEM_FOR_HOST)} curated models")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(ok_g) == len(HOSTS) and len(ok_m) == len(GEM_FOR_HOST),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=ncbi_env,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(hours=2)),
)
