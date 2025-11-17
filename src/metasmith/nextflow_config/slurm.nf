// https://www.nextflow.io/docs/latest/reference/config.html

// parameter defaults
params.slurm_account = '<slurm_account>' // task.config.nextflow.slurm_account
filePorter.maxThreads = 10
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
        maxAttempts = 999999 // controlled per process
        jitter = 0.25
        maxDelay = 30.second
        delay = 1.second
    }
    // -----------------------------------------
    // notes
    // executor = 'hq'          // todo: consider https://github.com/It4innovations/hyperqueue
}

process {
    publishDir {
        mode = 'rellink'
    }

    scratch = true                  // use worker node's local hard drive
    executor = 'slurm'
    clusterOptions = "--nodes=1 --ntasks=1 --account=${params.slurm_account}"
    errorStrategy = { task.attempt<=5 ? 'retry' : 'ignore' }
    maxErrors = '-1' // quotes bypass groovy parser bug, should set to number of samples?
    array = <array>

    // resource defaults
    cpus = <cpus>           // 4
    memory = { <memory>.GB }     // 16 GB
    // time = { <time>.hour**(3*(task.attempt-1)) }         // 3h
    time = { <time>.hour }         // 3h
}
