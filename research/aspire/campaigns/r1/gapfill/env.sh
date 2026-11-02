R=/scratch/phyberos/gmcf3495/metasmith/runs/QkqCNJOo
RES=$R/results
CACHE=/scratch/phyberos/cache/apptainer
SIF_METABAT2=$CACHE/docker..quay.io_biocontainers_metabat2..2.17--h6f16272_1.sif
SIF_SEMIBIN=$CACHE/docker..quay.io_biocontainers_semibin..2.1.0--pyhdfd78af_0.sif
SIF_COMEBIN=$CACHE/docker..quay.io_hallamlab_external_comebin..gpu-1.0.4.sif
SIF_SKANI=$CACHE/docker..staphb_skani..0.2.2.sif
SIF_CHECKM=$CACHE/docker..quay.io_hallamlab_external_checkm2..1.1.0.sif
SIF_SAMTOOLS=$CACHE/docker..staphb_samtools..1.23.sif
#
# Three things in this BIND string are each load-bearing, and each cost a job:
#
# --no-home        apptainer binds $HOME by default, and the host's
#                  ~/.local/lib/python3.12/site-packages/torch then shadows the
#                  container's, failing on a libmpi_cxx.so.40 that only exists
#                  in the host stack. Hit on semibin2 S29 (52884293).
#
# --cleanenv       Compute Canada's lmod exports a `which` bash FUNCTION into
#                  the environment (BASH_FUNC_which%%), and apptainer forwards
#                  exported bash functions into the container. COMEBin's
#                  run_comebin.sh finds its own install dir via
#                  `dirname $(which run_comebin.sh)`, so the leaked function
#                  runs `/usr/bin/which --tty-only ...` -- which the container's
#                  minimal `which` rejects with "Illegal option --". dirname
#                  then gets several args, `cd` gets "too many arguments", and
#                  python cannot find main.py. That is the entire S5 comebin
#                  failure that was briefly misread as a GPU problem.
#
# no --env HOME    apptainer refuses to override HOME via APPTAINERENV_HOME and
#                  only warns, so setting it silently does nothing.
#
# Callers must also add `--bind "$SLURM_TMPDIR" --pwd "$SLURM_TMPDIR"`:
# $SLURM_TMPDIR is on /localscratch, which the /scratch/phyberos bind does not
# cover, and --no-home removes the implicit cwd bind that used to hide that.
BIND="--cleanenv --bind /scratch/phyberos --no-home --env PYTHONNOUSERSITE=1"
