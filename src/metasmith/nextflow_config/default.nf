// parameter defaults
params.output = 'results/latest'

env {
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
}

process {
    cpus = <cpus>
    errorStrategy = 'ignore'
}
