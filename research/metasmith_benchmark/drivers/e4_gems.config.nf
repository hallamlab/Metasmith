// E4 GEM lane: appended after make_slurm_config's text by stage_and_run(extra_config=...).
// CarveMe's 6 h, 32 GB and no-retry live in the transform (strict Resources), not here.
executor {
    queueSize = 800
    $slurm {
        submitRateLimit = '1/1min'
    }
}
process {
    array = 100
}
