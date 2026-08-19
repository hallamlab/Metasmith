from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

py_env   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out      = model.AddProduct(lib.GetType("fabfos_data::genomes"))

HOSTS = {
    "e_coli_k12":     "GCF_000005845.2",
    "e_coli_dh10b":   "GCF_000019425.1",
    "e_coli_epi300":  "GCF_052692645.1",
    "e_coli_dh1":     "GCF_000270105.1",
    "e_coli_w3110":   "GCF_048541595.1",
    "e_coli_bw25113": "GCF_050858555.1",
}

GEM_FOR_HOST = {
    "e_coli_k12":   "iML1515",
    "e_coli_dh10b": "iECDH10B_1368",
    "e_coli_dh1":   "iECDH1ME8569_1439",
}

BIGG_URL = "http://bigg.ucsd.edu/static/models/{model}.json"

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
