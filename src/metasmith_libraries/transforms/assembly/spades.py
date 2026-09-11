from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
img_bbt = model.AddRequirement(lib.GetType("env::bbtools.env"))
img_sp  = model.AddRequirement(lib.GetType("env::spades.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("sequences::spades_assembly"))
# The graph is the assembly; contigs.fasta is a lossy read-out of it. Kept as its
# own product because the workspace lives on node-local storage and is wiped at
# job end -- anything not declared here is gone.
graph   = model.AddProduct(lib.GetType("sequences::spades_assembly_graph"))
# ...and the graph alone is not enough to find a CONTIG in it. The P lines of
# `assembly_graph_with_scaffolds.gfa` are named for scaffolds: on a real pool
# only 9 of 101 path names matched a contigs.fasta record, and the NODE index
# diverged from contig 11 onward, because scaffolding joins contigs across gaps
# and renumbers. `contigs.paths` is the assembler's own contig -> walk mapping,
# so it ships with the graph rather than being reconstructed by matching on
# (length, cov) -- which is a guess that happens to work, not a statement.
paths   = model.AddProduct(lib.GetType("sequences::spades_contig_paths"))

def protocol(context: ExecutionContext):
    ireads=context.Input(reads)
    imeta=context.Input(meta)
    iout=context.Output(out)
    igraph=context.Output(graph)
    ipaths=context.Output(paths)
    with open(imeta.local) as j:
        read_meta = json.load(j)
    parity = read_meta["parity"]
    assert parity in {"single", "paired"}, f"unknown parity: [{parity}]"
    if parity == "paired":
        mode = "--meta"
        bbcms_arg = f"in={ireads.container} interleaved=t"
        reads_arg = "--12 corrected.fastq.gz"
    else:
        mode = ""
        bbcms_arg = f"in={ireads.container}"
        reads_arg = "-s corrected.fastq.gz"

    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-t {threads}"
    threads_bbt = "" if threads is None else f"threads={threads}"

    # NOT megahit's 85% headroom convention, and the difference is a job that
    # dies at 6.5 h. SPAdes' `-m` is a hard setrlimit on its own allocation, not
    # a hint: a hand-rolled pool33 run with 128 GiB granted by SLURM and `-m 108`
    # died exit 250 with "mimalloc: unable to allocate OS memory" at MaxRSS
    # 111.9 GB -- it hit the flag, not the cgroup, with 20 GiB of its grant
    # unused. Worse, `max_memory` is persisted into params.txt and
    # K<k>/configs/config.info, so `--continue` rebuilds the same ceiling and
    # dies in the same place; a retry has to be a fresh run.
    #
    # 95% instead, close to the 384 GiB / `-m 360` ratio that does work on the
    # heavy pool. The remaining 5% is for the container and allocator overhead
    # that lives outside SPAdes' own accounting but inside the cgroup.
    #
    # A pool that genuinely needs more escalates through the runtime rather than
    # through this number: nextflow retries a failed task once at 2x memory, so
    # 128 -> 256 GiB, and the 33 pools that finish inside 128 GiB are not made
    # to queue for a whole-fat-node allocation they never touch.
    mem_gb = context.params.get('memory')
    mem_arg = f"-m {max(1, int(mem_gb * 0.95))}" if mem_gb else ""
    xmx = f"-Xmx{int(mem_gb * 0.85)}g" if mem_gb else ""

    # `-t N` is NOT sufficient. SPAdes' hot phases are OpenMP, and every metasmith
    # runtime pins OMP_NUM_THREADS=1 (nextflow_config/slurm.nf, the apptainer
    # --env list in env/environment.py, and the agent bootstrap). SPAdes then
    # reports "Maximum # of threads to use (adjusted due to OMP capabilities): 1"
    # and honours 1, not N -- measured on the AT7jCizU run, where every arm used
    # 1.02 of its 32 cores for 6-17 h. Re-export inside the command, which is the
    # last writer and therefore wins over the runtime's --env. bbcms is BBTools
    # and threads its own hot loops, so it gets the same treatment.
    omp_arg = "" if threads is None else f"export OMP_NUM_THREADS={threads}\n"

    # This transform follows the DOE JGI Metagenome Workflow (Clum et al. 2021,
    # mSystems 6:e00804-20) rather than a hand-rolled metaSPAdes invocation, so
    # the pipeline it serves is citable to a published protocol. Three steps,
    # each quoted in the comment above it. REFERENCES.md carries the deviations.
    #
    # "Filtered reads are error corrected using bbcms version 38.44 from BBTools
    # with a minimum count of 2 and a high-count fraction of 0.6."
    #
    # The pinned bbtools.env is 39.49, already in use by bbduk.py in this
    # library, not 38.44. `mincount` and `highcountfraction` are unchanged across
    # that range, but the release is a stated deviation rather than a match.
    _bbcms_cmd = f"""\
            {omp_arg}\
            bbcms.sh {xmx} {threads_bbt} \
                mincount=2 highcountfraction=0.6 \
                {bbcms_arg} \
                out=corrected.fastq.gz
        """
    context.ExecWithEnv(env=img_bbt, cmd=_bbcms_cmd)

    # "These split-error-corrected files are assembled with metaSPAdes version
    # 3.13.0 using the 'metagenome' flag, running the assembly module only (i.e.,
    # without error correction) with kmer sizes of 33, 55, 77, 99, and 127."
    #
    # `--only-assembler` is what makes bbcms above the error corrector rather
    # than a duplicate of SPAdes' own; running both would correct twice. The
    # fixed `-k` replaces SPAdes' automatic choice, which varies with read
    # length and is therefore not reproducible across a mixed corpus. spades.env
    # is pinned to 3.15.5, not 3.13.0 -- a stated deviation on the same footing
    # as bbcms; both flags are stable across that range.
    _cmd = f"""\
            {omp_arg}\
            spades.py {mode} --only-assembler -k 33,55,77,99,127 {threads_arg} {mem_arg} \
                {reads_arg} \
                -o spades_ws
        """
    context.ExecWithEnv(env=img_sp, cmd=_cmd)

    # "Contigs that are smaller than 200 bp are discarded."
    #
    # A separate pass with BBTools' reformat.sh rather than hand-rolled FASTA
    # parsing; img_bbt is already a requirement so this adds no dependency. Only
    # the contigs product is filtered. The graph and contigs.paths describe the
    # FULL assembly and are kept unfiltered, because the graph's path names are
    # scaffold names that would no longer line up with a post-filter contig set.
    _filter_cmd = f"""\
            reformat.sh in=spades_ws/contigs.fasta out=filtered_contigs.fasta minlength=200
            [[ $(head filtered_contigs.fasta | wc -c) -ne 0 ]] && mv filtered_contigs.fasta {iout.container} || echo "assembly was empty after length filter"
            # The graph only exists once the run reaches the end, so its absence
            # alongside present contigs means a truncated run, not an empty one.
            [[ -s spades_ws/assembly_graph_with_scaffolds.gfa ]] && mv spades_ws/assembly_graph_with_scaffolds.gfa {igraph.container} || echo "no assembly graph was written"
            [[ -s spades_ws/contigs.paths ]] && mv spades_ws/contigs.paths {ipaths.container} || echo "no contig paths were written"
        """
    context.ExecWithEnv(env=img_bbt, cmd=_filter_cmd)


    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
                graph: igraph.local,
                paths: ipaths.local,
            },
        ],
        success=iout.local.exists() and igraph.local.exists() and ipaths.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        # Whole-node. Measured on fir's hand-rolled 96-core arms: 74.6 and 76.4
        # effective cores over a full run, ~92 while the assembler is hot, and a
        # complete pool in 2h19m -- against 6-17 h for the OMP-throttled 32-core
        # arms. Memory follows from the core count, not from taste: those same
        # runs peaked at MaxRSS 69-72 GB, so the previous 32 GB would OOM.
        #
        # 20 h rather than 18: the bbcms correction pass now runs ahead of the
        # assembly, and those measurements did not include it.
        cpus=96,
        memory=Size.GB(128),
        duration=Duration(hours=20),
    )
)
