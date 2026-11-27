# STEPS

1. Action: read PROGRESS.md, STEPS.md, PROMPT.md (all empty / fresh start). Doc: none (task prompt).
   Outcome: fresh start, no prior progress.

2. Action: read docs/setup/{_index,install,ssh,deployment,tutorials}.rst, docs/index.rst, docs/tutorials/remote_agent.rst, docs/agentic/setup.rst; grep docs for "gui", "web", "serve", "metagenomics", "paired", "SRR".
   Doc: docs/setup/install.rst (Conda section) for install command.
   Outcome: install command found: `mamba create -n msm_env -c hallamlab -c bioconda metasmith`; `msm` starts the CLI.
   AMBIGUITY: none of the docs describe a web GUI or how to start it (no `msm gui`/serve command documented). The only GUI mention is setup/deployment.rst:147 ("In the web GUI the same action is the workflow's set up button"). Will discover the start command from `msm --help` after install.
   AMBIGUITY: no doc mentions the workflow `metagenomics_from_paired_reads` or SRA run download. docs/workflows/ only has assembly_annotation.rst. Will look for it in the installed package's help output.
   AMBIGUITY: no doc describes the "metabuli target" or how to exclude a target.

3. Action: `nohup setsid mamba create -y -n msm_env -c hallamlab -c bioconda metasmith > logs/install.log 2>&1 &` (run from sandbox; .condarc in home keeps envs/pkgs inside sandbox - note HOME is /home/tony, check)
   Doc: docs/setup/install.rst, "Conda" section (`mamba create -n msm_env -c hallamlab -c bioconda metasmith`).
   Note: mamba binary resolves to ~/.cache/msm-e2e/bootstrap-env/bin/mamba (outside sandbox) on PATH; I am only executing it, not reading files there.
4. Action: `timeout 60 ssh -o BatchMode=yes -o ConnectTimeout=20 nibi 'echo ok && hostname'`
   Doc: docs/tutorials/remote_agent.rst, "Verify SSH works non-interactively" step 3.
   Outcome: OK (login node l2.nibi.sharcnet). Note: home/.ssh/config has ProxyCommand .no-fresh-login (always exits 1) but the connection still succeeded.
5. Action: ssh nibi: whoami, ls /scratch/phyberos, which apptainer, sacctmgr assoc.
   Doc: no doc: guess (checking prerequisites from docs/setup/deployment.rst "Container Runtime" and "Remote-side prerequisites").
   Outcome: user=phyberos; /scratch/phyberos exists (drwx------), msm_e2e/run-2 does not exist yet; no apptainer on default PATH; Slurm associations include def-shall+ (accounts).
6. Action: nohup mamba create metasmith (PID 1722947, log logs/install.log). Still running at 27s; waiting via Monitor.
8. Action: `msm --help`, `msm gui --help` (PATH to envs/msm_env/bin).
   Doc: no doc: guess. docs contain no GUI start command; setup/deployment.rst:147 mentions "the web GUI" only in passing.
   AMBIGUITY: docs/ never document `msm gui`; found it via `msm --help` ("run the web GUI over the current directory").
9. Action: nohup setsid msm gui --no-browser --port 8765 > logs/gui.log 2>&1 &
   Doc: no doc: guess (flags from `msm gui --help`). Port 8765 chosen by me (no default documented).
10. Outcome: GUI serving at http://127.0.0.1:8765 (project = sandbox). Log: logs/gui.log. Startup compiled stdlib libraries (~30s).
11. Action (browser): navigate http://127.0.0.1:8765, snapshot; click "Agents" tab.
   Doc: no doc (GUI undocumented); guessed from the running GUI's nav bar.
12. Action (browser): click "Agents" tab (selector button[data-tour="tab-agents"], first try via ref timed out, then worked). Agents list: "no agents yet".
   Doc: no doc (GUI undocumented).
13. Action (browser): click "+ agent".
   Doc: no doc (GUI); analogous to docs/setup/deployment.rst Agent(home=..., runtime=...) fields.
14. Outcome: GUI created agent "graceful-local" (default: this machine, apptainer, local preset). Need to change to remote host.
15. Action: ssh nibi 'module spider apptainer' / 'module --default avail apptainer'.
   Doc: docs/setup/deployment.rst "Container Runtime" / "SLURM" (setup_commands with `module load apptainer`); docs/tutorials/remote_agent.rst ("module load apptainer").
   Outcome: apptainer/1.3.5 is the default Core module; `module load apptainer` will load it. (Versions 1.2.4-1.5.4 exist.)
   Note: docs warn Apptainer 1.5.x mksquashfs crash (docs/setup/install.rst warning). Default 1.3.5 avoids that.
16. Action (browser): click "a remote host" tab on agent graceful-local.
17. Action (browser): host combobox -> nibi; home directory -> /scratch/phyberos/msm_e2e/run-2. Agent auto-renamed graceful-nibi (GUI behaviour).
   Doc: docs/setup/deployment.rst "Agents can also be deployed to remote machines" (Source.FromSsh path); the GUI field label matches.
18. Action (browser): nextflow preset -> slurm; click setup commands textbox.
   Doc: docs/setup/deployment.rst "SLURM" section (GetNxfConfigPresets()["slurm"], slurmAccount param) and "setup_commands" section.
19. Action (browser): typed setup commands: "#!/bin/bash" + newline + "module load apptainer". Verified by screenshot gui-agent-form.png.
   Doc: docs/setup/deployment.rst "setup_commands ... module load apptainer" (cluster module loading).
   AMBIGUITY: docs show `module load apptainer` but nibi's module is in Core as apptainer/1.3.5 (default); used the docs' form.
20. Action (browser): click "+ a param" to add slurmAccount.
21. Action (browser): default param slurmAccount (name pre-filled by GUI) = def-shallam_cpu.
   Doc: docs/setup/deployment.rst SLURM section: params=dict(slurmAccount=...).
22. Action (browser): click "deploy" on agent graceful-nibi (host nibi, home /scratch/phyberos/msm_e2e/run-2, apptainer, slurm preset, slurmAccount def-shallam_cpu, setup "module load apptainer").
   Doc: docs/setup/deployment.rst (Agent deploy); GUI button labelled "deploy" (no doc).
23. Outcome: deploy in progress (GUI shows "deploying..."); gui log shows apptainer pulling the metasmith image blobs on nibi (logs/gui.log). Waiting.
24. Outcome: DEPLOY OK. Log: "deployed to [ssh://nibi:/scratch/phyberos/msm_e2e/run-2]"; agent file agents/graceful-nibi.yml written.
25. ERROR: agents/graceful-nibi.yml has NO default_params: the slurmAccount param I entered in the GUI was not saved (form showed it before deploy; after deploy the row disappeared). Re-adding via GUI and will verify yml. Doc: no doc (GUI); docs/setup/deployment.rst says slurmAccount is passed as params.
26. ERROR (GUI): default param slurmAccount does not persist to agents/graceful-nibi.yml (tried typing value, Tab, Enter). Will supply slurmAccount per run in the Workflows/run form instead.
   Doc: docs/setup/deployment.rst (SLURM: params=dict(slurmAccount=...) given at run time) - so per-run param is the documented path anyway.
27. Action (browser): click Workflows tab.
28. Action (browser): Workflows tab -> "+ workflow" -> dialog "new workflow" with template combobox.
   Doc: no doc (GUI). 
29. Discovery: workflow metagenomics_from_paired_reads is a vendored template: envs/msm_env/.../vendor/templates/metagenomics_from_paired_reads/spec.yml. Its target_types include taxonomy::metabuli (must be excluded per the task). Its sample_type is sequences::read_metadata; the bundled input is a placeholder sample_1.
   AMBIGUITY: docs never name this workflow; found via GUI template list and the installed package.
30. Action (browser): select template metagenomics_from_paired_reads.
31. Action (browser): click "create" -> workflow "visionary-malkoha" with template contents (4 placeholder input rows, 25 outputs incl. taxonomy::metabuli).
   Outcome: OK.
32. Doc check: grep docs for SRA/sra/accession/uri formats. Docs (usage/logistics.rst, tutorials/my_first_agent.rst) cover NCBI assembly accessions (ncbi::assembly_accession via AddValue), local files, and HttpSource URLs. No SRA accession type or SRA URI format is documented.
   AMBIGUITY: docs give no way to input an SRA run accession (SRR...). Will check the GUI's type list for an SRA type before deciding.
33. WORKAROUND: GUI type list and docs gave no SRA input. Looked at installed package (outside docs): envs/msm_env/.../vendor/transforms/logistics/getSraReads.py takes type ncbi::sra_accession and env::sra-tools.env; logistics library is loaded in the workflow (58 transforms). Plan: in the GUI, replace the 4 placeholder input rows with one value row "SRR9658108" of type ncbi::sra_accession. This departs from "docs only" for the workflow step; it is flagged here and in the report.
   AMBIGUITY: docs never mention SRA or ncbi::sra_accession.
34. ERROR (GUI): tried to discard the 4 placeholder input rows via "discard this row" (×). Clicks register, the rows stay (still "4 row(s)"). First click turns × red, further clicks show ✓. Tried: bottom-up, single clicks, repeated clicks. No effect.
35. Action (browser): "+ workflow" -> template "blank" -> create. Reason: template rows cannot be discarded (see 34).
   Doc: no doc (GUI). Blank template text: "an empty workflow - you name the inputs and the targets yourself".
   Note: stray workflow "visionary-malkoha" (template, metabuli still in outputs) remains; delete it later.
36. Outcome: blank workflow "devout-partridge" created; 13 of 13 libraries loaded there (ncbi types available). One input row present (file mode default).
37. Action (browser): click "value" mode on the input row.
38. Action (browser): value row, value "SRR9658108", type ncbi::sra_accession (picked from list).
   Doc: none (GUI). Type found in installed ncbi.yml; matches getSraReads.py (WORKAROUND, see 33).
39. Action (browser): "+ an output" (GUI) then type type-name + Enter in the output's type combobox. Enter selects. Outputs added so far: sequences::megahit_assembly.
   Doc: none (GUI). Outputs list = template's 25 targets minus taxonomy::metabuli (from spec.yml in the vendored template).
40. Action (browser): added outputs via "+ an output" + type + Enter (selector role=combobox[name="type"] >> nth=-1). Added the template's 24 non-metabuli targets in order (megahit, read_qc_stats, orfs, assembly_stats, per_contig/per_bp coverage, uniref50 results/descriptions, kofamscan results/descriptions, phyloflash_summary, cluster_table, metabat2/semibin2/comebin bin_fasta, checkm_stats+gtdbtk x3, binning contig_to_bin_table x3).
   Note: the template's parent links (e.g. checkm parents = bin fasta) are not set; GUI "+ parent" would be needed to mirror them. Check after solve.
41. Outcome: 24 outputs wanted. GUI flags some rows "already wanted, with the same lineage" (the checkm/gtdbtk duplicates without parents). Need to set "+ parent" links to the bin fasta of each binner (template spec: checkm/gtdbtk parents = metabat2/semibin2/comebin bin fasta; c2b parents = megahit).
   Doc: none (GUI).
42. Action (browser): set "+ parent" links so duplicates are no longer flagged: checkm/gtdbtk x3 -> metabat2/semibin2/comebin bin_fasta; contig_to_bin x3 -> megahit_assembly. Other template parent links (orfs, assembly_stats, etc. -> megahit) NOT set; they are not flagged.
   AMBIGUITY: docs don't explain "+ parent" in the GUI or template parent semantics.
43. Next: click solve.
44. ERROR (GUI): solve -> "no plan: solver could not find a complete plan", all 24 targets listed as unreachable (from the SRA input).
   Next: inspect getSraReads.py outputs (package file, outside docs) to see what it produces vs what the template expects (sample_type sequences::read_metadata).
45. Finding (package source, outside docs): getSraReads.py requires BOTH sra_accession and sequences::read_metadata (parity paired/single, length_class short/long), with the accession's parent = the metadata row. Template input rows were: read_metadata {parity: paired, length_class: short} (root) -> SRA-accession row. So the blank workflow needs a read_metadata root row, and the SRR row needs "+ parent" -> that root row.
   AMBIGUITY: docs don't explain how an SRA accession needs its metadata parent in the GUI.
46. Action (browser): add a read_metadata value row (parity=paired, length_class=short) via "+ an input", and link the SRA row's "+ parent" to it.
47. Outcome (browser): inputs now = read_metadata {parity: paired, length_class: short} + ncbi::sra_accession "SRR9658108" with parent read_metadata. Screenshot gui-meta.png confirms. 24 outputs wanted (no metabuli).
48. Action: click solve.
STEPS: 49. solve again clicked; solver running >3 min (GUI shows solving...).
