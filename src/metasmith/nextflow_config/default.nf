// parameter defaults
env {
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
}

process {
    cpus = <cpus>
    errorStrategy = { task.attempt<=3 ? 'retry' : 'ignore' }
}
