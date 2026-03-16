// https://www.nextflow.io/docs/latest/reference/config.html

// parameter defaults
params {
    slurmAccount = '<slurm_account>'

    executor {
        queueSize = 100
        submitRateLimit = '1/5sec'
        pollInterval = '10sec'
        stageInMode = 'symlink'             // some intermediates are large reference databases and should not be copied
    }

    localExecutor {
        queueSize = 4
        memory = '8 GB'
        cpus = 8                            // for local steps
    }

    process {
        scratch = '${SLURM_TMPDIR:-${TMPDIR:-/tmp}}'
        tries = 2
        array = 20
        cpus = 4
        memory = '16 GB'                    // https://www.nextflow.io/docs/latest/reference/stdlib-types.html#memoryunit
        time = '6hours'                     // https://www.nextflow.io/docs/latest/reference/stdlib-types.html#duration
    }
}

cleanup = true                          // remove work dirs of completed tasks to save disk/inodes
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
}

// report file path is dynamic, so needs to be passed in as argument at runtime
//      otherwise:
// report.enabled = true

executor {
    queueSize = params.executor.queueSize
    submitRateLimit = params.executor.submitRateLimit
    pollInterval = params.executor.pollInterval
    stageInMode = params.executor.stageInMode

    retry {
        maxAttempts = 99999                 // controlled per process
        jitter = 0.25
        maxDelay = 30.second
        delay = 1.second
    }
    
    // executor = 'hq'                      // todo: consider https://github.com/It4innovations/hyperqueue

    local {
        cpus = params.localExecutor.cpus
        memory = params.localExecutor.memory
        queueSize = params.localExecutor.queueSize
    }
}

workflow {
    failOnIgnore = false
    output {
        enabled = true
        ignoreErrors = false
        mode = 'copy'
    }
}

process {
    cache = 'lenient'

    errorStrategy = {                       // retry up to limit, then ignore, nextflow defaults to crashing
        task.attempt<params.process.tries? 'retry' : 'ignore'
    }

    cpus = params.process.cpus
    memory = {                              // difficult to combine smarts for time and memory; error codes not reliable
        task.attempt==1? params.process.memory : 2*(params.process.memory as MemoryUnit)
        
    }
    time = {                                // limit scaling of request time since can also fail for other reasons
        task.attempt==1? params.process.time : 2*(params.process.time as Duration)
    }
    
    executor = 'slurm'
    scratch = params.process.scratch        // use worker node's local hard drive, if set
    // --nodes=1: one compute node per job submission
    // --ntasks=1: this seems to affect some parallelization behaviour of SLURM,
    //      but we will request N cpus ourselves, so 1 is meant to prevent SLURM
    //      from doing something unexpected, like duplicating jobs.
    //      not sure if this is needed
    clusterOptions = "--nodes=1 --ntasks=1 --account=${params.slurmAccount}"

    maxRetries = params.process.tries+2     // this must be larger than errorStrategy
    maxErrors = '-1'                        // quotes bypass groovy parser bug, should set to number of samples?
    array = params.process.array            // batch jobs for the same tool

    withLabel: 'xlocalx' {
        executor = 'local'
        array = 0                           // local executor does not support job arrays
        scratch = false                     // login node doesn't have SLURM_TMPDIR
        errorStrategy = 'ignore'            // no retry when local
    }
}
