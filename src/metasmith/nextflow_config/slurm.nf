// https://www.nextflow.io/docs/latest/reference/config.html

// parameter defaults
params {
    slurmAccount = '<slurm_account>'
    // Some sites charge GPU work to a separate allocation code (Sockeye does).
    // clusterOptions is a *scalar* directive, so a GPU step composes its whole
    // string and cannot append an account -- it has to replace it. Leave null
    // to charge GPU steps to slurmAccount like everything else, or set it via
    // RunWorkflow(params={"slurmGpuAccount": "..."}).
    slurmGpuAccount = null

    executor {
        queueSize = 100
        submitRateLimit = '1/5sec'
        pollInterval = '10sec'
    }

    // Local steps run on the driver's host. A launch sizes the local executor from that host's
    // cgroup, and `cpus` caps it, because a login node is shared.
    localExecutor {
        memory = '8 GB'
        cpus = 8
    }

    process {
        scratch = '${SLURM_TMPDIR:-${TMPDIR:-/tmp}}'
        tries = 3
        // Every attempt is clamped to these, a per-step override included: Slurm refuses a job
        // over the site's limits at submit. fir caps every job at 7 days, and its largest
        // CPU nodes hold 768 GB.
        maxDuration = '7days'
        maxMemory = '750 GB'
        array = 100
        cpus = 4
        memory = '16 GB'                    // https://www.nextflow.io/docs/latest/reference/stdlib-types.html#memoryunit
        time = '6hours'                     // https://www.nextflow.io/docs/latest/reference/stdlib-types.html#duration
        // Scheduler flags. `clusterOptions` replaces the built-in base string
        // (rarely wanted); `clusterOptionsExtra` appends to it, which is the
        // generic sbatch-flag injection point:
        //      RunWorkflow(params={"process_clusterOptionsExtra": "--partition=bigmem"})
        clusterOptions = null
        clusterOptionsExtra = ''
    }
}

cleanup = false                         // keep work dirs after task completion so .command.err/.out are recoverable for failed tasks; set true to remove them and save disk/inodes
nextflow.cache.db.type = 'rocksdb'
filePorter.maxThreads = 2
report.overwrite = true
timeline.overwrite = true

// set some cache paths
// todo: this has been made useless since env is not passed into container...
env {
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
    OPENBLAS_NUM_THREADS = 1
    OMP_NUM_THREADS = 1
}

// report file path is dynamic, so needs to be passed in as argument at runtime
//      otherwise:
// report.enabled = true

executor {
    queueSize = params.executor.queueSize
    pollInterval = params.executor.pollInterval

    retry {
        maxAttempts = 99999                 // controlled per process
        jitter = 0.25
        maxDelay = 30.second
        delay = 1.second
    }
    
    // Scoped to slurm: a flat submitRateLimit also throttles the local executor, where every
    // cache-hit twin runs, so a relaunch replayed its hits at one per five seconds (hours).
    $slurm {
        submitRateLimit = params.executor.submitRateLimit
    }

    // executor = 'hq'                    // todo: consider https://github.com/It4innovations/hyperqueue

    // Local-executor capacity, as FLAT keys rather than a nested `local {}` block.
    // Nextflow reads `executor.cpus` and `executor.memory` as local-executor-only
    // settings. `executor { local { ... } }` renders as `executor.local.cpus`, which it
    // reports as an Unrecognized config option and then DROPS -- so this block never
    // applied. With it dropped the local executor falls back to the JVM's available
    // processor count, and the agent runner pins that to 1 with
    // -XX:ActiveProcessorCount=1 to survive the login node's 512-process cap. Every
    // `xlocalx` step then asked for the default 4 cpus against 1 available and Nextflow
    // aborted the entire run: "Process requirement exceeds available CPUs -- req: 4;
    // avail: 1". Reference-database downloads are exactly those steps, because compute
    // nodes here have no outbound network.
    // queueSize is deliberately NOT set: a flat `executor.queueSize` would apply to the
    // SLURM executor as well and clobber the 100 set above. `executor.cpus` bounds local
    // concurrency implicitly instead -- 8 cpus against 4-cpu steps is two at a time.
    // These lines only hold until launch, which appends the sized block (see params above).
    cpus = params.localExecutor.cpus
    memory = params.localExecutor.memory
}

workflow {
    failOnIgnore = false
    output {
        enabled = true
        ignoreErrors = false
        // A hard link, not a copy: a copy stored every product a second time, and a relaunch
        // rewrote all of them. results/ and nxf_work sit in one run dir, so the link cannot
        // straddle devices. CAUTION a results/ file shares its inode with the task product and
        // any promoted shard, so never edit one in place.
        mode = 'link'
    }
}

process {
    cache = 'lenient'
    stageInMode = 'symlink'                 // some inputs are large reference databases and must not be copied

    // A nested param (`params.process.cpus`) read outside a closure is fixed when the config is
    // parsed, before `-params-file` is merged, so a run's params never reached it. Every key a
    // run may set is therefore a closure, read at task time. `array` cannot be one, and launch
    // appends it as a literal instead. The ladders are rendered by `models/libraries/resources.py`,
    // which pins this text in a unit test.
    errorStrategy = { (task.exitStatus in [125, 137, 140, 143] || task.exitStatus == Integer.MAX_VALUE) && task.attempt < params.process.tries ? 'retry' : 'ignore' }
    maxRetries = 99                         // errorStrategy owns the limit
    maxErrors = '-1'                        // quotes bypass groovy parser bug, should set to number of samples?

    cpus = { params.process.cpus }
    memory = { def p = task.attempt > 1 ? task.previousTrace : null; def want = p?.memory == null ? (params.process.memory as MemoryUnit) : ((p.exit as Integer) in [125, 137] ? 2 : 1) * new MemoryUnit(p.memory as long); [want, ((params.process?.maxMemory ?: '1024 TB') as MemoryUnit)].min() }
    time = { def p = task.attempt > 1 ? task.previousTrace : null; def want = p?.time == null ? (params.process.time as Duration) : ((p.exit as Integer) in [140] ? 2 : 1) * new Duration(p.time as long); [want, ((params.process?.maxDuration ?: params.process?.max_duration ?: params.process?.max?.duration ?: '3650days') as Duration)].min() }

    executor = 'slurm'
    scratch = { params.process.scratch }    // use worker node's local hard drive, if set
    // --nodes=1: one compute node per job submission
    // --ntasks=1: this seems to affect some parallelization behaviour of SLURM,
    //      but we will request N cpus ourselves, so 1 is meant to prevent SLURM
    //      from doing something unexpected, like duplicating jobs.
    //      not sure if this is needed
    clusterOptions = { (params.process.clusterOptions ?: "--nodes=1 --ntasks=1 --account=${params.slurmAccount}") + (params.process.clusterOptionsExtra ? " ${params.process.clusterOptionsExtra}" : "") }
    array = params.process.array            // batch jobs for the same tool

    withLabel: 'xlocalx' {
        executor = 'local'
        array = 0                           // local executor does not support job arrays
        scratch = false                     // login node doesn't have SLURM_TMPDIR
        errorStrategy = { task.attempt < 2 ? 'retry' : 'ignore' }  // a download's failure is usually the network's
    }
}
