// parameter defaults
params.output = 'results/latest'
params.slurm_account = '<slurm_account>' // task.config.nextflow.slurm_account

env {
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
}

executor {
    queueSize = <queueSize>                 // 100
    submitRateLimit = '<submitRateLimit>'   // 10/1sec  | this may be too aggressive
    pollInterval = '<pollInterval>'         // 10sec
    stageInMode = '<stageInMode>'           // symlink  | some intermediates are large reference databases and should not be copied

    // -----------------------------------------
    // notes
    // executor = 'hq'          // todo: consider https://github.com/It4innovations/hyperqueue
}

process {
    scratch = true                  // use worker node's local hard drive
    executor = 'slurm'
    clusterOptions = "--nodes=1 --ntasks=1 --account=${params.slurm_account}"
    errorStrategy = 'ignore'

    // resource defaults
    cpus = <cpus>           // 4
    memory = '<memory>'     // 16 GB
    time = '<time>'         // 3h
}
