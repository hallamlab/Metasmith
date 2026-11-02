// https://www.nextflow.io/docs/latest/reference/config.html

// parameter defaults
params {
    executor {
        cpus = 8
        memory = '8 GB'
        queueSize = 4
    }

    process {
        tries = 3
    }
}

filePorter.maxThreads = 2
report.overwrite = true
timeline.overwrite = true

// set some cache paths
env {
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
}

executor {
    cpus = params.executor.cpus
    memory = params.executor.memory
    queueSize = params.executor.queueSize
}

workflow {
    failOnIgnore = false
    output {
        enabled = true
        ignoreErrors = false
        mode = 'rellink'
    }
}

process {
    cache = 'lenient'

    executor = 'local'

    // Retries only an exit that names a resource or an interruption; see slurm.nf.
    errorStrategy = { (task.exitStatus in [125, 137, 140, 143] || task.exitStatus == Integer.MAX_VALUE) && task.attempt < params.process.tries ? 'retry' : 'ignore' }
    maxRetries = 99                         // errorStrategy owns the limit
    maxErrors = '-1'                        // quotes bypass groovy parser bug, should set to number of samples?

    withLabel: 'xlocalx' {
        errorStrategy = { task.attempt < 2 ? 'retry' : 'ignore' }  // a download's failure is usually the network's
    }
}
