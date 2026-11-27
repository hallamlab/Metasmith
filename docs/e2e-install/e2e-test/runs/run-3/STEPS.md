# STEPS

1. Read docs: setup/_index, setup/install.rst, setup/ssh.rst, setup/deployment.rst. Found no doc describing how to start the web GUI or the metagenomics_from_paired_reads workflow (only "In the web GUI..." mention in deployment.rst:147). Also: ssh config home/.ssh/config ProxyCommand = .no-fresh-login, which always fails ("ask the operator to reconnect") -> likely blocker for ssh nibi.
2. Command: mamba create -n msm_env -c hallamlab -c bioconda metasmith -y  (doc: docs/setup/install.rst "Conda" section). Note: `mamba` resolved from PATH (outside sandbox dir, used only as a binary, not read). envs_dirs in home/.condarc = sandbox/envs.
   Launched detached, log: /home/tony/msm_e2e/run-3/sandbox/logs/install.log
3. Command: ssh nibi 'hostname; whoami; echo ok' (doc: task instructions; ssh.rst for config). Outcome: OK -> l2.nibi.sharcnet, user phyberos. The ProxyCommand .no-fresh-login did not block the connection.
4. Outcome of install: success, metasmith v0.25.1 in envs/msm_env (bin/msm). Log logs/install.log.
5. Command: msm gui --help (no doc: guess; `msm --help` lists "gui: run the web GUI over the current directory"). 
6. Command: nohup setsid envs/msm_env/bin/msm gui --no-browser > logs/gui.log 2>&1 &  (doc: none; discovered from msm --help). Outcome: serving at http://127.0.0.1:8090, PID 1791937. Note: docs don't describe the GUI at all -> AMBIGUITY/GUESS on how to start it.
7. GUI: Agents tab -> "+ agent" created default local agent "striped-local" (GUI behaviour, no doc). Switched to "a remote host" tab (doc: setup/deployment.rst "Deploying agents": remote via SSH host).
8. Command: ssh nibi 'module avail apptainer; which sbatch; mkdir -p /scratch/phyberos/msm_e2e/run-3' (doc: deployment.rst "setup_commands" -> "module load apptainer"; "Container Runtime"). Outcome: apptainer modules 1.2.4,1.3.4,1.3.5(default),1.4.5 available; sbatch present; /scratch/phyberos owned by phyberos, dir created OK; accounts def-shallam_cpu, def-shallam_gpu.
   Note: the mkdir was run via ssh by me, not GUI (directory creation for home).
9. GUI (agent striped-nibi, auto-renamed from striped-local): host=nibi, home=/scratch/phyberos/msm_e2e/run-3, runtime=apptainer (default), nextflow preset=slurm, default param slurmAccount=def-shallam_cpu, setup commands "#!/bin/bash / module load apptainer/1.3.5". Doc basis: deployment.rst "Deploying agents"/"SLURM"/setup_commands; the "+ a param" button needed a second click before the row appeared (GUI quirk, WORKAROUND-free).
10. GUI: click "deploy" (doc: task instruction; GUI button).
11. Outcome pending: deploy running (gui.log shows staged msm, lib/agent.yml, deploying 3 staged files; 'Using cached SIF image' notice at 14:41:41, mount-test ongoing).
12. Outcome: DEPLOYED. gui.log: 'deployed to [ssh://nibi:/scratch/phyberos/msm_e2e/run-3]', relay-deployed. Note: relay used sif cache and ran api deploy_from_container on nibi.
13. GUI: Workflows tab -> "+ workflow" -> template "metagenomics_from_paired_reads" -> create. Result: workflow "myrtle-narwhal" (template placeholder inputs /data/sample_01.fastq.gz; 25 outputs wanted incl. taxonomy/metabuli). Need to: replace inputs with ENA URLs and "stop wanting" metabuli output. Doc: no doc describes this GUI; template name from GUI dropdown (AMBIGUITY: how to express URL inputs in GUI).
14. GUI (myrtle-narwhal): replaced placeholder read paths with https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR965/008/SRR9658108/SRR9658108_{1,2}.fastq.gz (doc: docs/usage/logistics.rst "HTTP(S)/FTP" shows HttpSource; GUI input textbox accepted URL - AMBIGUITY: no GUI doc). Clicked "stop wanting this" on taxonomy/metabuli output row (task instruction). Verified via page text: 'metabuli' no longer on page; header still says "25 wanted" (may be stale count).
15. GUI: click "solve" (doc: task instruction; GUI button).
16. Outcome of solve: ERROR/AMBIGUITY. gui.log AssertionError from metasmith/ops/inputs.py sync: "[https:/ftp...] is a relative path, and a file row points at a file of yours -- give it an absolute path, or make it a value row". GUI file rows need local absolute paths (URL became https:/ after normalization). Doc: docs/usage/data.rst "Register inputs" (local files) - no doc says GUI accepts URLs.
   Workaround plan: download both reads into sandbox (data/), set rows to absolute local paths.
17. Command: curl -fL -o data/SRR9658108_{1,2}.fastq.gz <ENA URLs> (doc: none; task instruction). Sizes 316.6MB + 330.4MB. Detached log logs/download.log.
18. Outcome: read1 complete (316607828 B). read2 truncated at 67MB (curl 18). Retrying with curl -C - (resume).
19. Outcome: read2 resumed and complete (330375666 B). Both files gzip -t OK.
20. GUI: set read rows to absolute local paths /home/tony/msm_e2e/run-3/sandbox/data/SRR9658108_{1,2}.fastq.gz (WORKAROUND: URLs rejected by GUI file rows). Clicked solve again.
21. Outcome of re-solve: SUCCESS (GUI plan status "success", 34 steps, id TQZVgn6u). gui.log went quiet after staging at 14:52:59 but GUI showed the plan. Doc basis for next step: docs/setup/deployment.rst "Set up a workflow before its first run" (setup run, then run with same agent+task; GUI "set up" button). Next: set up on striped-nibi with slurm account param, then run.
22. GUI: selected agent striped-nibi and preset slurm in workflow myrtle-narwhal; clicked 'set up' (doc: deployment.rst 'Set up a workflow before its first run'). Page moved to runs/myrtle-narwhal-mAKE4 (setup run).
23. Outcome of setup run: ERROR. gui.log FileNotFoundError [#240] "cannot stage workflow [TQZVgn6u]: 1 external input folder(s) must be bound into the remote container but do not exist on the remote host: /home/tony/.../sandbox/data". Absolute local inputs are NOT transferred by metasmith.
   Fix (WORKAROUND): copy the two reads to nibi scratch: /scratch/phyberos/msm_e2e/run-3/data/ and set GUI rows to those remote paths.
24. scp to nibi: both reads present at /scratch/phyberos/msm_e2e/run-3/data/ with full sizes (17:05 MST). Next: set GUI rows to remote paths, re-solve, set up, run.
25. GUI: rows set to /scratch/phyberos/msm_e2e/run-3/data/SRR9658108_{1,2}.fastq.gz; clicked 'solve again'.
26. Re-solve SUCCESS (34 steps). GUI: clicked 'set up' (setup run) again with remote input paths. Doc: deployment.rst 'Set up a workflow before its first run'.
27. setup run still in container image builds (15:28 MST), no Nextflow/sbatch yet.
28. setup run now executing on nibi: slurm job 28039305 nf-p10__buildPhyloflashDB RUNNING (15:58 MST). Run id myrtle-narwhal-OJHVV (GUI). Nextflow watcher PID 3125356 on nibi.
