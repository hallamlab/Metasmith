# Antonio's step 20, run ONCE on the frozen set. vConTACT3 has no path that
# reuses protein clusters against a subset of genomes -- `-p/-g` skips gene
# calling but not clustering, and the parquet intermediates are skipped only on
# an exact re-run -- so a filtered rerun costs the whole thing. Run it on
# everything and treat the cluster assignment as a per-contig annotation.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::vcontact3.env"))
ref    = model.AddRequirement(lib.GetType("ref::vcontact3_db"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))

out_assignments = model.AddProduct(lib.GetType("viromics::vcontact3_assignments"))
out_network     = model.AddProduct(lib.GetType("viromics::vcontact3_network"))
out_ani         = model.AddProduct(lib.GetType("viromics::vcontact3_ani"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    iref = context.Input(ref)
    threads = context.params.get("cpus", 8)

    out_dir = Path("vcontact3_out")

    # -n, not the `-p proteins -g gene2genome -l lengths` trio Antonio used. The
    # -n route calls genes itself with pyrodigal-gv, which drops the dependency
    # on DRAM-v's protein FASTA and on building a gene2genome map -- one
    # requirement instead of three. -p/-g is also mutually exclusive with -n and
    # disables the ANI export, which is why `vcontact3_ani` is a product here but
    # is not something a driver can target through the other route.
    #
    # No --db-version: the reference tree holds one release and vConTACT3 picks
    # the newest json it finds, so pinning it here would be a second place to
    # keep in step with downloadVcontact3DB.
    _cmd = f"""
        vcontact3 run -n {ifrozen.container} -o {out_dir} \
            --db-path {iref.container} -t {threads}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    def _find(name: str) -> Path:
        hits = sorted(out_dir.rglob(name))
        assert hits, (
            f"vConTACT3 wrote no {name}; {out_dir} holds "
            f"{sorted(p.name for p in out_dir.rglob('*') if p.is_file())[:20]}"
        )
        return hits[0]

    oassign = context.Output(out_assignments)
    onet = context.Output(out_network)
    oani = context.Output(out_ani)

    oassign.local.write_bytes(_find("final_assignments.csv").read_bytes())
    # `genome_by_genome_overview.csv` and `viral_cluster_overview.csv` in
    # Antonio's step table are vConTACT **2** names and do not exist in 3.x.
    oani.local.write_bytes(_find("ani_summary.tsv").read_bytes())

    onet.local.mkdir(parents=True, exist_ok=True)
    for part in ("nodes.csv", "edges.csv"):
        (onet.local/part).write_bytes(_find(part).read_bytes())

    outs = {out_assignments: oassign, out_network: onet, out_ani: oani}
    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={
        out_assignments: "final_assignments.csv",
        out_network: "network",
        out_ani: "ani_summary.tsv",
    },
    resources=Resources(cpus=32, memory=Size.GB(128), duration=Duration(hours=24)),
)
