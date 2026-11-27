# PROGRESS
## Done
- Read docs (setup/install.rst, setup/deployment.rst, setup/ssh.rst, tutorials/remote_agent.rst, workflows/engcyc_metagenomics.rst).
- `ssh sockeye` works (BatchMode, login02).
## Running
- (none yet) installing metasmith: mamba create -n msm_env -c hallamlab -c bioconda metasmith  (log: /home/tony/msm_e2e/run-4-sockeye/sandbox/logs/install.log)
## Next
- verify `msm --version`, find GUI start command (docs do not document it; check `msm --help`)
- start GUI on port 8091 detached
- drive GUI via browser: deploy agent to sockeye, set up, run.
- GUI (mine): `msm gui --port 8091 --no-browser` from sandbox, PID 3100582, log logs/gui.log. (Port 8092 + run-4-fir processes belong to another run; do not touch.)
- DONE: agent strict-sockeye deployed via GUI to sockeye:/scratch/st-shallam-1/txyliu/msm_e2e/run-4-sockeye.
- NEXT: Workflows tab -> engcyc_prep_references_lite setup run, then engcyc_metagenomics_lite on SRR9658108.
- BLOCKER-ish: prep_references_lite plan has 5 unfilled inputs with no path (see STEPS 11). Investigating.
- SETUP RUN launched via GUI: workflow mature-foxhound (engcyc_prep_references_lite), agent strict-sockeye, run id mature-foxhound-OAIb3. Check GUI Runs tab / `ssh sockeye` for slurm jobs.
- Earlier: 5 blank template inputs removed (2-click discard) to clear "unfinished recipe" block; see STEPS 11-12.
- Setup run hhFno failed same way after GUI restart; trying to add aspire library to the workflow (STEPS 15).
- RUNNING: GUI run yY3pp (prep_references_lite via stage-and-run after setup button failed in staging). Monitor in Runs tab.
- Run yY3pp executing on sockeye (Slurm nf-* jobs seen: nf-p21__buildRefseqViralDB). Monitor task b18xjr0fm polls squeue. Warning from GUI: 8 tool images not in store (should run setup first); watch for compute-node failures.
- prep run yY3pp: downloads ok; p21 failed: compute nodes lack quay.io route (images not pre-pulled). Need setup-run image pull.
