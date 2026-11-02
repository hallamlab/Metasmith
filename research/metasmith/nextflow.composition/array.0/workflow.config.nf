// parameter defaults
params.output = 'results/latest'
params.slurm_account = '***' // task.config.nextflow.slurm_account

env {
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
}

process {
    scratch = true                  // use worker node's local hard drive
    executor = 'slurm'
    clusterOptions = "--nodes=1 --ntasks=1 --account=${params.slurm_account}"

    // resource defaults
    cpus = 1           // 4
    memory = '8 GB'     // 16 GB
    time = '3h'         // 3h
    
    queueSize = 10                 // 100
    submitRateLimit = '10/1sec'   // 10/1sec  | this may be too aggressive
    pollInterval = '10sec'         // 10sec
    stageInMode = 'symlink'           // symlink  | some intermediates are large reference databases and should not be copied

    // -----------------------------------------
    // notes
    // executor = 'hq'          // todo: consider https://github.com/It4innovations/hyperqueue
}
