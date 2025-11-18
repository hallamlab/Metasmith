// https://www.nextflow.io/docs/latest/reference/config.html

// parameter defaults
params.slurm_account = '<slurm_account>' // task.config.nextflow.slurm_account
filePorter.maxThreads = 2
report.overwrite = true

env {
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
}

// report file path is dynamic, so needs to be passed in as argument at runtime
// report.enabled = true

workflow {
    failOnIgnore = false
    output {
        enabled = true
        ignoreErrors = false
        mode = 'rellink'
    }
}

executor {
    queueSize = <queueSize>                 // 100
    submitRateLimit = '<submitRateLimit>'   // 10/1sec  | this may be too aggressive
    pollInterval = '<pollInterval>'         // 10sec
    stageInMode = '<stageInMode>'           // symlink  | some intermediates are large reference databases and should not be copied

    retry {
        maxAttempts = 99999 // controlled per process
        jitter = 0.25
        maxDelay = 30.second
        delay = 1.second
    }
    // -----------------------------------------
    // notes
    // executor = 'hq'                      // todo: consider https://github.com/It4innovations/hyperqueue
}

process {
    publishDir {
        mode = 'rellink'
    }

    scratch = true // use worker node's local hard drive
    executor = 'slurm'
    clusterOptions = "--nodes=1 --ntasks=1 --account=${params.slurm_account}"
    errorStrategy = { task.attempt==1 ? 'retry' : 'ignore' }
    maxRetries = 5 // this must be larger than errorStrategy
    maxErrors = '-1' // quotes bypass groovy parser bug, should set to number of samples?
    array = <array>

    // resource defaults
    cpus = <cpus> // 4
    memory = { <memory>.GB } // 16 GB
    time = { task.attempt==1? <time>.hour : 2*<time>.hour } // 3h
    // memory = { task.exitStatus in [137, 139, 140, 143]? (2**(2*(task.attempt-1)))*<memory>.GB : <memory>.GB } // 16 GB
    // time = { task.exitStatus in [137, 140, 143, 144, 145]? (2**(2*(task.attempt-1)))*<time>.hour : <time>.hour } // 3h
}
