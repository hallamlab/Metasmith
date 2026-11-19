# PROGRESS
- Done: docs read; metasmith 0.25.1 installed in envs/msm_env; MetasmithLibraries cloned; apptainer 1.3.5 module verified on nibi.
- Deploy: earlier attempts timed out / orphaned (killed). Remote stale builds killed (16).
- Running: deploy (rootfs="sandbox") in background -> work/deploy3.log (started 11:58 MST).
- Next: when deploy finishes, write the run script for metagenomics_from_paired_reads (drop metabuli target, SRR9658108), run on slurm account def-shallam_cpu, collect to ./results.
- Note: local processes die when the turn ends; poll with sleep 600 within a turn.
- Deploy3 running (Monitor armed). Script work/run_srr.py ready; needs accession file on cluster: /inputs/SRR9658108.txt
