"""The host set fanned out into one ORF set per host.

Small, and it exists for a lineage reason rather than a data one. The acquisition
delivers the host set as ONE folder; the 4-lane mapper takes ONE ORF set and B2 collects
one table per host. Something has to turn the folder into three, and doing it here --
inside the graph, as a scatter -- is what keeps all three mapper runs descendants of the
same `fabfos_data::genomes` node.

That lineage is what `host_gpr_denovo`'s `parents={genomes}` pin resolves against. Stage
three proteomes as unrelated givens instead and the pin has nothing to bind to, so the
planner is free to satisfy the mapper requirement from whatever ORF set is cheapest to
reach -- in a full pipeline, a prodigal run on a fosmid assembly -- and the host
attribution then gets attached to a table built from something else entirely.

NOTHING IS PARSED OR RENAMED. The proteome is NCBI's, at the name the acquisition gave
it, copied byte for byte. The mapper's `source` column is the staged file's stem, which
is how B2 joins a table back to its host, so a rename here would break that join
silently.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
genomes = model.AddRequirement(lib.GetType("fabfos_data::genomes"))
out     = model.AddProduct(lib.GetType("sequences::orfs"))


def protocol(context: ExecutionContext):
    src = Path(str(context.Input(genomes).local))
    hosts = sorted(p for p in src.glob("*") if (p / "genome").is_dir())
    if not hosts:
        raise SystemExit(f"no host/genome/ directories under {src}")

    manifest = []
    for host in hosts:
        faa = sorted((host / "genome").glob("*.faa"))
        if len(faa) != 1:
            raise SystemExit(
                f"{host.name}: expected exactly one proteome under genome/, found "
                f"{[p.name for p in faa]}. The mapper's `source` column is the staged "
                f"file's stem, so two would make the host attribution ambiguous.")
        iout = context.Output(out)
        # Named for the sequence accession, which is what the acquisition named it and
        # what B2 joins on.
        dest = iout.local.parent / faa[0].name
        dest.write_bytes(faa[0].read_bytes())
        n = sum(1 for line in dest.open() if line.startswith(">"))
        Log.Info(f"{host.name}: {faa[0].name}  {n:,} proteins")
        manifest.append({out: dest})

    return ExecutionResult(
        manifest=manifest,
        success=len(manifest) == len(hosts),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=genomes,
    # NO labels=["local"]. That label maps to the slurm preset's 8-core / 8 GB local
    # executor, which REFUSES a larger ask rather than queueing it, with
    # errorStrategy='ignore' and no retry -- so an over-sized step is dropped silently
    # and the workflow finishes green with its output absent. The label is right for a
    # step that needs the login node's outbound network. This one reads a staged folder.
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=15)),
)
