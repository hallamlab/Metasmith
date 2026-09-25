from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
img_bbt = model.AddRequirement(lib.GetType("env::bbtools.env"))
img_sp  = model.AddRequirement(lib.GetType("env::spades.env"))
# The length filter. NOT bbtools: reformat.sh shreds its own output on this
# input -- see the comment above the filter command.
img_sqk = model.AddRequirement(lib.GetType("env::seqkit.env"))
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
    # seqkit, NOT BBTools' reformat.sh, and the reason is a measured defect rather
    # than a preference. reformat.sh SHREDS its output on assembly-shaped FASTA
    # while reporting success. Reproduced on a real 19,937-contig megahit assembly
    # on fir, deterministically, byte-identical across threads=1/4, -Xmx4g, and
    # with the jdk.incubator.vector module disabled:
    #
    #   in   19,937 records, opens '>'
    #   out  "Output: 19937 reads (100.00%) 19165202 bases (100.00%)" -- and a file
    #        that opens 'C' mid-sequence, carries 100,690 lines starting with '>',
    #        repeats one 70-char block over and over at its head, and truncates
    #        headers mid-string: ">k141_289528 flag=1 mu", ">k141_289540 fla".
    #   seqkit, same input, same filter: 19,937 records, opens '>'. Correct.
    #
    # This is the whole of the assembly corruption chased on 2026-09-11. It is not
    # the engine, not the bind mounts, not the page cache, not publish-by-copy, and
    # not the dev overlay -- all of which were suspected in turn and cleared. The
    # product check below caught it twice and was right both times.
    #
    # The protocol specifies the FILTER, not the tool: "Contigs that are smaller
    # than 200 bp are discarded" names no program, so seqkit satisfies it exactly
    # and the substitution is recorded in REFERENCES.md rather than hidden.
    #
    # Only the contigs product is filtered. The graph and contigs.paths describe
    # the FULL assembly and are kept unfiltered, because the graph's path names are
    # scaffold names that would no longer line up with a post-filter contig set.
    _filter_cmd = f"""\
            seqkit seq --min-len 200 spades_ws/contigs.fasta > filtered_contigs.fasta
            [[ $(head -c 1 filtered_contigs.fasta) == ">" ]] && mv filtered_contigs.fasta {iout.container} || echo "assembly was empty or malformed after the length filter"
        """
    context.ExecWithEnv(env=img_sqk, cmd=_filter_cmd)

    # The graph and the paths are plain moves and stay with bbtools' image only
    # because they need no tool at all; any shell would do.
    _collect_cmd = f"""\
            # The graph only exists once the run reaches the end, so its absence
            # alongside present contigs means a truncated run, not an empty one.
            [[ -s spades_ws/assembly_graph_with_scaffolds.gfa ]] && mv spades_ws/assembly_graph_with_scaffolds.gfa {igraph.container} || echo "no assembly graph was written"
            [[ -s spades_ws/contigs.paths ]] && mv spades_ws/contigs.paths {ipaths.container} || echo "no contig paths were written"
        """
    context.ExecWithEnv(env=img_bbt, cmd=_collect_cmd)


    # A product check, not an existence check -- and now an instrument, because
    # the thing it catches has not been explained yet.
    #
    # Twice on 2026-09-11, on both corpora, this transform produced an assembly of
    # the RIGHT SIZE whose first byte was not '>'. On CAMI the published file
    # carried 505,012 records against reformat.sh's reported 86,744, opened
    # mid-sequence, and had a hole at byte 1,236,201 -- exactly the length of
    # NODE_1 as named in the .paths product. On Pratama, reformat.sh reported
    # 354,787 records and 368,772,773 bases, metasmith stat'd the product at
    # 369.38 MB, which is right, and the first byte still was not '>'.
    #
    # So the size is correct and visible while the content at offset 0 is not.
    # That is consistent with a read-after-write visibility problem across the two
    # apptainer bind mounts of one directory -- the tool container writes through
    # its /ws, this process reads through its own -- and NOT with a truncated
    # write. But "consistent with" is not a diagnosis, and the first time round I
    # asserted unflushed writeback on less evidence than this.
    #
    # Hence the shape below. It does not just fail; it records what it saw, and it
    # re-reads once after a pause so the NEXT occurrence distinguishes the two
    # remaining explanations by itself:
    #
    #   re-read succeeds -> visibility. The bytes arrive late. Then the fix is a
    #       barrier here, not a change to the assembler, and the run is correct.
    #   re-read fails the same way -> the file really is wrong on disk, and the
    #       hex of its first bytes says what was written instead.
    #
    # A pass on the retry is reported loudly rather than silently, because a check
    # that quietly succeeds on a second try is how this would become invisible
    # again. Everything not on the happy path is logged with its evidence.
    import os, time

    def _inspect(path, label):
        """(problem or None, one line of evidence)."""
        size = os.path.getsize(path) if os.path.exists(path) else -1
        with open(path, "rb") as fh:
            head = fh.read(4096)
        if not head:
            return f"{label} is empty", f"{label}: size={size} head=<empty>"
        shown = head[:48]
        gt = head.find(b">")
        evidence = (f"{label}: size={size} first48={shown.hex()} "
                    f"ascii={shown.decode('ascii', 'replace')!r} "
                    f"first_gt_within_4k={gt}")
        if head[:1] != b">":
            return f"{label} does not open with '>'", evidence
        return None, evidence

    def _whole(path, label):
        problem, evidence = _inspect(path, label)
        if problem is not None:
            Log.Error(f"{problem} -- {evidence}")
            Log.Error(f"{label}: re-reading after a pause to separate visibility from corruption")
            time.sleep(15)
            os.sync()
            problem2, evidence2 = _inspect(path, label)
            if problem2 is None:
                Log.Error(f"{label}: RE-READ PASSED. The bytes arrived late, so this was a "
                          f"visibility problem across the bind mounts and not a bad write. "
                          f"{evidence2}")
            else:
                # NOT "wrong on disk". A re-read goes through the same mount and
                # can be served from the same stale page cache, so this rules out
                # a short-lived lag and nothing more. The in-container probe above
                # is what separates a bad file from a bad view.
                Log.Error(f"{label}: re-read failed identically after 15s and a sync, so this "
                          f"is not a brief lag. Compare MSM_PROBE_IN_CONTAINER above: if that "
                          f"shows '>', the bytes are fine and this process's view of them is "
                          f"not. {evidence2}")
                return problem2
        # Only reached once the file opens correctly. A NUL anywhere in a FASTA is
        # the other shape the CAMI file had.
        with open(path, "rb") as fh:
            n = 0
            while True:
                b = fh.read(1 << 20)
                if not b:
                    break
                if b"\x00" in b:
                    return f"{label} contains NUL at ~{n + b.index(b'\x00')}"
                n += len(b)
        return None

    problems = [m for m in (_whole(iout.local, "assembly"),) if m]
    for m in problems:
        Log.Error(m)

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
                graph: igraph.local,
                paths: ipaths.local,
            },
        ],
        success=(not problems
                 and iout.local.exists() and igraph.local.exists() and ipaths.local.exists()),
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
