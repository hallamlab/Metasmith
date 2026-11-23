from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
diamond = model.AddRequirement(lib.GetType("env::diamond.env"))
python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
sprot   = model.AddRequirement(lib.GetType("ref::uniprot_sprot_2025_04"))
uniref  = model.AddRequirement(lib.GetType("ref::uniref_2025_04"))
enzyme  = model.AddRequirement(lib.GetType("ref::expasy_enzyme"))
ec_lib  = model.AddRequirement(lib.GetType("lib::uniprot_ec_maps.py"))
sp_db   = model.AddProduct(lib.GetType("ref::swissprot_diamond_db"))
ur90_db = model.AddProduct(lib.GetType("ref::uniref90_diamond_db"))
ec_maps = model.AddProduct(lib.GetType("ref::uniprot_ec_maps"))


# The archives' member paths are matched by name, so a layout change inside a tarball does not
# matter; a release without the member fails at tar.
def protocol(context: ExecutionContext):
    isp, iur, ienz = context.Input(sprot), context.Input(uniref), context.Input(enzyme)
    lib_ = context.Input(ec_lib)
    isp_db, iur_db, imaps = context.Output(sp_db), context.Output(ur90_db), context.Output(ec_maps)
    threads = context.params.get("cpus") or 8
    context.LocalShell(f"mkdir -p {imaps.local}")
    context.ExecWithEnv(env=python, cmd=f"""
        set -euo pipefail
        tar -xzf {isp.container} --wildcards --no-anchored -O 'uniprot_sprot.fasta.gz' | gzip -dc > swissprot.fasta
        tar -xzf {isp.container} --wildcards --no-anchored -O 'reldate.txt' > {imaps.container}/SwissProt_reldate.txt
        python3 {lib_.container} swissprot --enzyme {ienz.container}/enzyme.dat --out {imaps.container}/EC_map.swissprot.tsv
        for db in uniref50 uniref90; do
            tar -xzf {iur.container} --wildcards --no-anchored "$db.xml.gz"
            xml=$(find . -name "$db.xml.gz" | head -1)
            python3 {lib_.container} uniref --enzyme {ienz.container}/enzyme.dat --xml "$xml" --name $db \\
                --fasta $db.fasta --out {imaps.container}/EC_map.$db.tsv
            echo "UniProt release 2025_04" > {imaps.container}/${{db}}_reldate.txt
            rm "$xml"
        done
        cp {ienz.container}/Expasy_reldate.txt {imaps.container}/
        rm uniref50.fasta
    """)
    context.ExecWithEnv(env=diamond, cmd=f"""
        set -euo pipefail
        diamond makedb --in swissprot.fasta -d swissprot --threads {threads}
        diamond makedb --in uniref90.fasta -d uniref90 --threads {threads}
        mv swissprot.dmnd {isp_db.container}
        mv uniref90.dmnd {iur_db.container}
        rm swissprot.fasta uniref90.fasta
    """)
    outs = {sp_db: isp_db.local, ur90_db: iur_db.local, ec_maps: imaps.local}
    return ExecutionResult(
        manifest=[outs],
        success=all(p.exists() for p in outs.values())
        and all((imaps.local/f"EC_map.{d}.tsv").is_file() for d in ("swissprot", "uniref50", "uniref90")),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=uniref,
    resources=Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=48)),
)
