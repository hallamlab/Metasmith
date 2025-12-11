// https://www.nextflow.io/docs/latest/reference/config.html

// parameter defaults
params {
    executor {
        cpus = 8
    }

    process {
        tries = 1
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
    executor = 'local'

    errorStrategy = {                       // retry up to limit, then ignore, nextflow defaults to crashing
        task.attempt<params.process.tries? 'retry' : 'ignore'
    }
    maxRetries = params.process.tries+2     // this must be larger than errorStrategy
    maxErrors = '-1'                        // quotes bypass groovy parser bug, should set to number of samples?
}
