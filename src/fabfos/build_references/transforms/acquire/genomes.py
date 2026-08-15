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

THE FTP, NOT `datasets`, AND THE PROTEOME IS WHY. Every consumer of this chunk joins
on the `[gene=]` and `[locus_tag=]` tags in the protein FASTA -- study_sequences.py
indexes them, derive_annotation_faa.py keeps a lane table in one id space with them,
main/benchmarks/eydallin/build_clone_orfs.py resolves a clone through them. Only the
FTP's `*_translated_cds.faa` carries those tags; `datasets download --include protein`
serves `protein.faa`, whose headers are `>NP_414542.1 <description> [organism]` and
carry no gene, no locus tag and nothing else to join on. Handing that file to any of
those consumers does not fail -- it indexes to nothing and reports every gene missing.
The pinned bytes are the FTP's, which is the other half of the argument: this
transform is the record of where the chunk came from, and it was not `datasets`.

The assembly directory name is READ FROM THE LISTING rather than composed. The path is
`<acc>_<assembly name>` and the name is upstream's free text -- `ASM584v2`, but also
plain `W3110` -- so composing it means guessing, and guessing 404s.

THE ONE PLACE THIS TIER RENAMES. Every other acquire transform writes upstream's
bytes at upstream's name. The FTP names every file after the assembly directory, so
the three of them collide across hosts once they are side by side; they are named by
the sequence accession of the primary record instead -- `NZ_` stripped, since RefSeq's
`NZ_CP189566.1` and INSDC's `CP189566.1` are the same sequence and the bare form is
what the rest of the tree uses. Bytes are untouched; only the path is chosen here.

UPSTREAM HAS MOVED UNDER THE PIN, and a full re-run is therefore not a no-op. Measured
2026-08-14: today's FTP files for all three original hosts differ from the bytes
`data/fabfos/originals/genomes.dvc` pins -- k12's translated CDS set is 4,300 records against
the pinned 4,318. The assembly accessions are unchanged; NCBI re-released the
annotation under them. So re-fetching an existing host would move the reference the
built tables were measured against without any of them changing, which is why
`examples/genomes_acquire.py` publishes ADDITIVELY and says so. This transform still
fetches every host: what it produces is today's snapshot, and which of it gets pinned
is the driver's decision, not this file's.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# One env, not two. `datasets` is no longer called -- see above -- and a requirement on
# a tool this transform does not run would make the declaration mean "whatever the
# image happens to contain".
py_env   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out      = model.AddProduct(lib.GetType("fabfos_data::genomes"))

# host -> NCBI assembly accession.
#
# EPI300 is GCF_052692645.1 (ASM5269264v1, complete genome, Lucigen material). NOT
# GCF_051228345.1, which was carried here previously and does not exist at NCBI at
# all -- the neighbouring GCA_051228345.1 that DOES exist is a Salmonella assembly,
# so a fetch under it would either 404 or, far worse, build the E. coli references
# off the wrong organism.
#
# DH1 AND W3110 ARE HERE FOR THE ASKA COHORTS, and neither is a fosmid host. The ASKA
# library was built in E. coli AG1, whose ORFs were cut from W3110 -- so W3110 is where
# a clone's SEQUENCE comes from, and it gets no GEM entry for the same reason EPI300
# has none: it is not a strain this tree reads a curated model against. AG1 itself is
# absent from NCBI entirely (checked across the complete E. coli set), so its closest
# sequenced relative stands in for it: Qimron et al. PNAS 2006 (data/fabfos/originals/aska/)
# states AG1 is a derivative of DH1. The stand-in is a borrow declared in
# benchmark/host_gpr_gem.py, not bytes written twice here -- same rule as EPI300.
#
# DH1 is GCF_000270105.1 (ME8569) and NOT GCF_000023365.1, which is also called DH1:
# only the former's `ECDH1ME8569_####` locus tags are the id space iECDH1ME8569_1439
# is keyed on, and the other one would give the model 1,439 genes to match and no
# join to match them by.
HOSTS = {
    "e_coli_k12":    "GCF_000005845.2",
    "e_coli_dh10b":  "GCF_000019425.1",
    "e_coli_epi300": "GCF_052692645.1",
    "e_coli_dh1":    "GCF_000270105.1",
    "e_coli_w3110":  "GCF_048541595.1",
}

# host -> the BiGG model that strain's GPR is asserted by. Absent means the strain
# has no published model; see the EPI300 note above.
GEM_FOR_HOST = {
    "e_coli_k12":   "iML1515",              # E. coli K-12 MG1655
    "e_coli_dh10b": "iECDH10B_1368",        # E. coli DH10B
    "e_coli_dh1":   "iECDH1ME8569_1439",    # E. coli DH1 (ME8569) -- AG1 borrows it
}

BIGG_URL = "http://bigg.ucsd.edu/static/models/{model}.json"

# gff3 is deliberately not fetched: nothing in the build consumes it. `translated_cds`
# rather than `protein` -- see the header note; it is the tagged one.
FTP = "https://ftp.ncbi.nlm.nih.gov/genomes/all"
SUFFIXES = {"fna": "genomic.fna", "faa": "translated_cds.faa", "gbk": "genomic.gbff"}


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    fetch = "\n".join(f"""
        H={host}; ACC={acc}
        # GCF_048541595.1 -> GCF/048/541/595, which is how the FTP shards the tree.
        P=$(echo $ACC | sed 's|^\\(GC[FA]\\)_\\([0-9][0-9][0-9]\\)\\([0-9][0-9][0-9]\\)\\([0-9][0-9][0-9]\\).*|\\1/\\2/\\3/\\4|')
        DIR=$(wget -qO- $FTP_ROOT/$P/ | grep -o "${{ACC}}_[^\\"/<]*" | sort -u | head -1)
        if [ -z "$DIR" ]; then
            echo "[genomes] $H ($ACC): no assembly directory under $FTP_ROOT/$P/" >&2
            exit 1
        fi
        rm -rf _dl_$H && mkdir -p _dl_$H {iout.container}/$H/genome
        for KIND in {" ".join(f"{k}:{v}" for k, v in sorted(SUFFIXES.items()))}; do
            EXT=${{KIND%%:*}}; SFX=${{KIND#*:}}
            wget -q "$FTP_ROOT/$P/$DIR/${{DIR}}_$SFX.gz" -O _dl_$H/$EXT.gz || {{
                echo "[genomes] $H ($ACC): $SFX absent at $DIR" >&2; exit 1; }}
            gunzip -f _dl_$H/$EXT.gz
        done
        # Named after the PRIMARY record of the assembly, which is the chromosome and
        # the first record in the genomic fasta. A plasmid-first assembly would name the
        # folder after the plasmid, which no host set here has.
        SEQ=$(head -1 _dl_$H/fna | cut -c2- | cut -d' ' -f1 | sed 's/^NZ_//')
        cp _dl_$H/fna {iout.container}/$H/genome/$SEQ.fna
        cp _dl_$H/faa {iout.container}/$H/genome/$SEQ.faa
        cp _dl_$H/gbk {iout.container}/$H/genome/$SEQ.gbk
        rm -rf _dl_$H
        echo "[genomes] $H $ACC $DIR -> $SEQ"
    """ for host, acc in sorted(HOSTS.items()))
    fetch = f"FTP_ROOT={FTP}\n{fetch}"

    context.ExecWithEnv() \
        .ifContainerDo(env=py_env, cmd=f"set -e\n{fetch}") \
        .ifVirtualEnvDo(env=py_env, cmd=f"set -e\n{fetch}")

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
    group_by=py_env,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(hours=2)),
)
