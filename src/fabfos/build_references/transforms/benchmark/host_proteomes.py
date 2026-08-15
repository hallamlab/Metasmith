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

NOTHING IS PARSED. The proteome is NCBI's, copied byte for byte -- but it IS renamed,
because it has to be: the engine names every product and the generated nextflow process
collects that name and nothing else. The accession therefore does not survive into the
staged file's stem, so B2 cannot attribute a mapper table by its file name and does not:
it reads the ORF IDS in the table and asks which proteome they came from. That join is
the stronger one anyway -- it holds whatever anything is called.
"""
from metasmith.python_api import *
# EXPLICIT, because `python_api`'s star export does not carry it. This ran for the first
# time on 0.20.4 and died in the container with `name 'Path' is not defined` after the
# job had queued -- the transform is only executed remotely, so nothing local catches it.
from pathlib import Path

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
    for i, host in enumerate(hosts):
        faa = sorted((host / "genome").glob("*.faa"))
        if len(faa) != 1:
            raise SystemExit(
                f"{host.name}: expected exactly one proteome under genome/, found "
                f"{[p.name for p in faa]}. One proteome per host is what makes the "
                f"attribution in host_gpr_denovo unambiguous.")
        # THE OUTPUT PATH IS THE ENGINE'S, NOT OURS. `_get_output_paths` names each
        # product `{batch}-{i}-{branch}.{hash}-{key}{ext}` and the generated nextflow
        # process collects exactly that glob, so a file written beside it under the
        # accession name is invisible: the step runs, writes five proteomes, and the
        # task fails on `ls: cannot access '*-1.*-<key>.faa'`. The index is what makes
        # a scatter a scatter -- one call to Output per host, not one per step.
        iout = context.Output(out, i)
        dest = Path(str(iout.local))
        dest.write_bytes(faa[0].read_bytes())
        n = sum(1 for line in dest.open() if line.startswith(">"))
        Log.Info(f"{host.name}: {faa[0].name} -> {dest.name}  {n:,} proteins")
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
