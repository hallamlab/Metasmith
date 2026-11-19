# PROGRESS
- DONE: metasmith 0.25.1 installed in envs/msm_env (PATH=/home/tony/msm_e2e/run-2/sandbox/envs/msm_env/bin:$PATH).
- DONE: ssh nibi works non-interactively.
- RUNNING: msm gui, PID 1725966, http://127.0.0.1:8765, log logs/gui.log (start: msm gui --no-browser --port 8765, nohup setsid).
- DONE: agent graceful-nibi deployed via GUI to ssh://nibi:/scratch/phyberos/msm_e2e/run-2 (apptainer, slurm preset, setup "module load apptainer"). ERROR: slurmAccount default param not persisted; supply per run.
- DONE: workflow devout-partridge (blank template): inputs read_metadata{parity:paired,length_class:short} + ncbi::sra_accession SRR9658108 (parent=read_metadata); 24 outputs (metabuli excluded), parent links set for checkm/gtdbtk/contig_to_bin.
- ISSUE: first solve -> "no plan" (missing metadata parent). Re-solved after fix; solver RUNNING since 13:26:44 (log: "solving plan for [1] sample").
- STRAY: workflow visionary-malkoha (template, metabuli still in outputs, placeholder inputs) — delete when GUI allows.
- NEXT: when solve succeeds: Workflows -> run with slurm agent graceful-nibi, params slurmAccount=def-shallam_cpu; wait; collect results to ./results.
