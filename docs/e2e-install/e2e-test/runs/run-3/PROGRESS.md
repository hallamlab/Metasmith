# PROGRESS
- [x] install metasmith 0.25.1 -> envs/msm_env (logs/install.log)
- [x] ssh nibi works (l2.nibi.sharcnet, user phyberos)
- [x] GUI running (PID 1791937, http://127.0.0.1:8090)
- [x] deployed agent striped-nibi to nibi (home /scratch/phyberos/msm_e2e/run-3)
- [ ] run metagenomics_from_paired_reads (no metabuli) on SRR9658108 via GUI
- [ ] collect results to ./results
Notes: docs have no GUI section; use browser tools only for GUI steps.

## update (deploy)
- agent striped-nibi deploy clicked in GUI at 14:41; server log logs/gui.log shows staging to /scratch/phyberos/msm_e2e/run-3 (in progress, waiting via monitor).

## update (inputs)
- workflow myrtle-narwhal solve failed: GUI needs local absolute file paths (URLs rejected). Downloading reads to sandbox/data/ (read1 done, read2 resuming, bg task bab8i3pa7 / monitor bu7223eoq).
- next: set the two read rows in GUI to /home/tony/msm_e2e/run-3/sandbox/data/SRR9658108_{1,2}.fastq.gz, keep metabuli removed, solve, then run on striped-nibi.
- [x] workflow myrtle-narwhal solved (34 steps, no metabuli), inputs = local copies in sandbox/data/
- [ ] setup run (doc: deployment.rst "Set up a workflow") then run on striped-nibi
- setup run started in GUI: run id myrtle-narwhal-mAKE4 (check Runs tab)
- reads copied to nibi:/scratch/phyberos/msm_e2e/run-3/data/ ; GUI rows to be updated
- re-solved w/ remote inputs; setup run started (2nd attempt)
- setup run executing on nibi (slurm job 28039305 first); GUI run myrtle-narwhal-OJHVV; waiting for completion, then real run (stage and run) with same agent+task.
