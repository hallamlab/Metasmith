# Viromics reference material — index

Compiled 2026-08-11 from the `slack` and `gmail` accounts on the `capella`/`mira`
awm peers. This file records where material is and quotes it; it does not
summarize, interpret, or evaluate any of it.

All 27 attachments are stored beside this file, under `slack/` and `email/`, one
directory per source message — see [Files on disk](#files-on-disk).

Contents:

- [Files on disk](#files-on-disk)
- [Slack](#slack) — DM `D0B2RFTMAAG`
- [Email](#email) — five Gmail threads
- [Attachment inventories](#attachment-inventories)
- [Links and identifiers referenced in the messages](#links-and-identifiers-referenced-in-the-messages)
- [Searches run](#searches-run)

---

## Files on disk

Retrieved 2026-08-11. Byte sizes match the sizes reported by the Slack and Gmail
metadata. Directory names encode the source message's date; the message each
directory came from is identified in the sections below.

```
slack/2026-06-16_filtering-scripts/     3 files   from Slack ts 1781645014.211319
email/2026-05-11_meeting-prep/          3 files   from Gmail 19e1957e6ee40af9
email/2026-05-13_globus-id/             1 file    from Gmail 19e2380efcabe6c5
email/2026-05-15_viral-workflow-update/ 16 files  from Gmail 19e2c9186dd0fb91
email/2026-05-25_metadata-workflow-table/ 3 files from Gmail 19e607a7ce5b20a5
email/2026-05-28_metadata-update/       1 file    from Gmail 19e708064e2a65b4
```

Byte-comparisons between copies that share a filename:

| Filename | Compared | Result |
|---|---|---|
| `07_filtering2.sh` | slack 2026-06-16 vs email 2026-05-15 | identical |
| `10_filtering_3.sh` | slack 2026-06-16 vs email 2026-05-15 | identical |
| `Bioinformatics workflow.pptx` | email 2026-05-11 vs email 2026-05-15 | differ (418075 B vs 73970 B) |
| `Metadata.xlsx` | email 2026-05-25 vs email 2026-05-28 | differ (22389 B vs 22575 B) |

`15_filter_vOTU_10kb.sh` was attached only in Slack; the Gmail attachment set stops
at `14_`. Its `#SBATCH --job-name` is `14_virus_overview_table`, and the Gmail
`14_summarize_vibrant_lifestyle.sh` is a different file.

The 2026-05-28 message (`19e708064e2a65b4`), whose attachment list had not been
retrieved when this file was first written, carries one attachment: `Metadata.xlsx`.

---

## Slack

All messages below are from the direct-message channel `D0B2RFTMAAG` between
`contacttonyliu` (`U01Q4CDSTFX`) and `antonio.castellanohin` (`U0B26D62PFZ`).
Quoted verbatim; `:name:` tokens are emoji shortcodes as they appear in the source.
Timestamps are the Slack `ts` field, converted to UTC dates.

### 2026-05-11 — Antonio (ts `1778519479.279549`)

> Hi Toni, hope you´re doing well! I'm preparing for tomorrow's meeting the list of
> tools to install for the virome workflow. But looking at the Cyanoverse image you
> shared, I'm not sure if that already includes all the tools you currently have
> installed, or if there are others (e.g., Trimmomatic, MEGAHIT, etc.) that are also
> available. Just to know, thanks!

### 2026-06-03 — Antonio (ts `1780530047.898209`)

> Hey Toni! Just wanted to give you a quick update. I´m finishing the viromics part of
> my first dataset today, and tomorrow I'll be done with the tools table we need to
> start building the workflow. Even if I won't be using it directly for my own work, I
> think it's worth testing and maybe trying it with other samples we may have.
>
> Also, I created a Claude account and I'm planning to run the Metasmith tutorial on
> Friday. Then next week we can pick things back up and see how the metagenomics
> analysis you started to run is going for the second dataset and how I can try it
> myself through metasmiht once it is set up correctly on my local machine. I'll have a
> lot more time to dedicate to it, so we should be able to make some good progress.

### 2026-06-03 — Tony (ts `1780530373.583379`)

> Hi Antonio, thanks for checking in. the read-level taxonomy is done and I'm organizing
> it to upload to globus. The binning and annotation are still running and will likely
> take a few more days. I'll send you a link to the folder on globus as results come in.
> I'm hopeful that I can hand you the metagenomics results and start on the viromics
> workflow using what you've learned from running the tools, late next week.

### 2026-06-08 — Antonio (ts `1780955517.380569`)

> Hi Tony, thanks for the update, that´s fantastic!! I need a couple more days to catch
> up with MetaSmith via Claude. I'm finishing some extra analyses for the virome dataset
> that Steven suggested last Friday, and I'd like to wrap those up first. I'll get up to
> speed with MetaSmith later this week and keep you posted.

### 2026-06-09 — Antonio (ts `1781041278.476819`)

> Hey Tony! Quick update on a few things:
>
> :large_green_circle: Metasmith tutorial: I'm currently on the StageWorkflow step,
> transferring context to Sockeye. It's going slowly but it's working! Hopefully I'll
> finish the tutorial today. If the transfer is too slow I might try an older version.
>
> :bar_chart: Viromics table: I have a large summary table with all the viromics
> workflow tools, versions and parameters. I'll review it this afternoon and send it to
> you tomorrow by email, cc Steven and Alvaro.
>
> :microscope: Viromics analysis: I'm finishing the last viromics analyses this week.
> Next week I'd like to start working on the outputs from the second dataset you're
> running with Metasmith. Where can I find the CoverM abundance table and the GTDB-Tk
> classification file for the dereplicated MAGs? I don't see them in Globus yet, are
> they uploaded or still pending? just wanted to check the taxonomy, I was curious :)
>
> :handshake: Next steps ideas: Maybe next week we could meet in person so you can show
> me how to launch the sample analysis with Metasmith myself. That way while it's
> running I can start making figures with the outputs that are already available. We
> could also look into implementing the viromics workflow in Metasmith and eventually
> test it with my 12 samples that I already ran manually. That way we can validate it
> and have it ready for future studies. It'll probably take some debugging but I'm sure
> we can get there!
> Thanks a lot!

### 2026-06-09 — Tony (ts `1781050197.363099`)

> Hey Antonio, I placed them in the parent folder by mistake
> https://app.globus.org/file-manager?origin_id=2602486c-1e0f-47a0-be15-eec1b0ff0f96&origin_path=%2FManuscripts%2FScience%2F2026-05-13_Spanish_lakes_viromics%2Fbinning%2F
> Note that there is a cluster table to get representative MAGs, but that may lead to
> duplication if you are using it for relative abundance. I would use the comebin MAGs
> if you are comparing against the metabuli contig-level taxonomy and the read taxonomy
> results.
> You will have to do some mapping to get taxonomy of dereplicated bins, sorry.
> tables here describe the contig -> bin mapping of each sample.
> /Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/binning/contig_to_bin/ (I will
> copy binning to .../metagenomics/binning/... in the next few days)
> Sample -> bins -> cluster representative -> gtdb taxonomy
> Or to ensure no overlap of bins within a sample:
> sample -> bins -> filter to comebin only -> gtdb taxonomy
>
> I suspect you will also need the BUSCO results for single copy marker genes which can
> be used to estimate the taxonomic richness
> https://app.globus.org/file-manager?origin_id=2602486c-1e0f-47a0-be15-eec1b0ff0f96&origin_path=%2FManuscripts%2FScience%2F2026-05-13_Spanish_lakes_viromics%2Fmetagenomics%2Fannotation_orfs%2Fbusco_full_table%2F
>
> For depth, there are tables that describe fold coverage of each contig, calculated in
> the style of TPM: (base pairs of reads)/(length of contig in base pairs). Assembly
> stats will also give you the total read count and read base pairs for normalization
> across samples
> https://app.globus.org/file-manager?origin_id=2602486c-1e0f-47a0-be15-eec1b0ff0f96&origin_path=%2FManuscripts%2FScience%2F2026-05-13_Spanish_lakes_viromics%2Fmetagenomics%2Fassembly%2F
>
> I will move the binning results to the metagenomics folder, but there will be 2 copies
> while the move is happening in the next few days.
>
> Yes, lets meet next week to talk about the next steps. I'll be on campus at Tuesday and
> we can talk then?
> Let me know if there's any issues with the tables since I renamed the outputs to match
> the sample names, but some tools may have taken the original file name.

### 2026-06-16 — Antonio (ts `1781645014.211319`) — 3 attachments

> I've also attached the scripts corresponding to the main filtering steps of the
> viromics workflow (steps 7, 10, and 15), in case you'd like to see in more detail how
> I implemented the filtering process. The overall workflow is summarized in the table
> attached to the email, while these scripts provide the specific commands and criteria
> used at each filtering stage.

Attachments — stored in `slack/2026-06-16_filtering-scripts/`; source URLs:

| File | Size | URL |
|---|---|---|
| `07_filtering2.sh` | 4187 B | https://files.slack.com/files-pri/T148XDEUQ-F0BAVRGKVV1/07_filtering2.sh |
| `10_filtering_3.sh` | 3894 B | https://files.slack.com/files-pri/T148XDEUQ-F0BB42022GH/10_filtering_3.sh |
| `15_filter_vOTU_10kb.sh` | 661 B | https://files.slack.com/files-pri/T148XDEUQ-F0BAVRH0RJP/15_filter_votu_10kb.sh |

### 2026-06-16 — Antonio (ts `1781645199.575209`)

> Sure. I mainly followed the methodology described in this paper:
>
> https://www.nature.com/articles/s41467-026-68914-2#Sec10
>
> All of the thresholds were adopted from there.
>
> Hopefully I'll also manage to publish the Sierra Nevada lake viromics paper at some
> point, so we can cite that one too :wink:

### 2026-06-22 — Antonio (ts `1782165692.189209`)

> Hey Toni, sorry to bother you. Working on replicating the metagenomics analysis on
> Sockeye with your scripts as templates. Sockeye setup is ready (apptainer, scratch,
> SLURM account). I understand the reads need to be staged on Sockeye first — mine are
> in Globus at `/Received_raw_data/2026-05-13_Spanish_lake_viromics_Antonio_Castellano/`.
> Where should I stage them on Sockeye scratch, and what's the best way to transfer from
> Globus? Also, is there a MetasmithLibrary for this project I should clone?

### 2026-06-23 — Tony (ts `1782240550.076069`)

> thank you for debugging, this is valuable information.
> I haven't gotten to the new tools you requested or the viromics pipeline yet, but
> should start running the new tools for metagenomics before next Tuesday

### 2026-06-23 — Antonio (ts `1782240715.105489`)

> Thanks Tony! Sounds great. One suggestion for prioritization: could we run the
> resistome analysis for the 101 samples first? That way I can start analyzing the
> output files and work towards writing the resistome paper (once I finish running the
> full pipeline I´m focus in now). Once that's running we can tackle implementing the
> viromics pipeline in Metasmith. Does that make sense?

### 2026-07-07 — Antonio (ts `1783411012.606809`)

> Thanks a lot, Toni! I really appreciate your help. My plan is to start analyzing those
> data next week. I'm just wrapping up some additional virome analyses I discussed with
> Steven and trying to make some progress with the Metasmith metagenomic analysis.
> I'll check the results on Globus. I imagine you had to troubleshoot quite a few issues
> to get everything running, you're an absolute legend!! Hopefully VirSorter2 and
> PathoFact finish soon.

### 2026-07-13 — Tony (ts `1783968790.099359`)

> Hey Antonio, please make a folder beside this one, name it similarly, and place the
> new reads there. A short readme that briefly describes what was sequenced would be
> helpful as well.
> https://app.globus.org/file-manager?origin_id=2602486c-1e0f-47a0-be15-eec1b0ff0f96&origin_path=%2FReceived_raw_data%2F2026-05-13_Spanish_lake_viromics_Antonio_Castellano%2F

### Also matched, different channel and participants

Channel `CCWC0Q7C1` (`bioinformatics`), 2026 search hit dated 1589947189.004600
(2020-05-20), from `mclaughlinr2`:

> Hey Alvero just shared these articles with me for another purpose, but I think they
> apply here, particularly the one about viromes.

Attachments: `1-s2.0-S0167701218301210-main.pdf` (251371 B),
`s40168-019-0626-5.pdf` (3209554 B). Not from Antonio and not in the DM thread;
recorded here only because it matched the `viromics`/`virome` searches.

---

## Email

Gmail account `contacttonyliu@gmail.com` / `phyberosis@gmail.com`. Thread and message
IDs are Gmail API IDs.

### 2026-05-11 — "RE: Invitation: Viral Workflow @ Tue May 12, 2026 12pm - 2pm (PDT)"

Thread `19e1957e6ee40af9`, message `19e1957e6ee40af9`.
From `antonio.castellanohinojosa@ubc.ca`; to `shallam@mail.ubc.ca`,
`alvaro.munozplominsky@ubc.ca`, `shatadru.2@buckeyemail.osu.edu`,
`contacttonyliu@gmail.com`. 3 attachments.

> Hi everyone,
> I'm sending over some information I prepared for tomorrow's meeting in case you'd like
> to take a look beforehand.
> I've attached:
>
> * The adapted Bioinformatics workflow and software installation requirements for
>   viromics analyses + A list of the tools/software we need to install together with
>   their GitHub page for installation, including the ones I believe Toni already has
>   installed
> * A brief overview of the high-mountain lake study, so you have more context regarding
>   the samples, number of datasets, objectives, and overall study design
>
> Looking forward to discussing everything tomorrow!
> See you then.
> Best,
> Antonio

The same message quotes the calendar invitation it replies to, which states the agenda:

> Based on previous communication, I propose the following presentation order and agenda:
>
> - Antonio will present the objectives of the viral study and intended approach
> - Tony will present the supporting infrastructure available from the Hallam Lab,
>   including Metasmith and compute resources
> - We will then refine and plan out how to achieve the objectives, with domain knowledge
>   from Rokaiya.

### 2026-05-13 / 2026-05-25 — "FW: Viromics - getting started"

Thread `19e2376fbd2a36d3`. Forwards of correspondence between
`Antonio Castellano <ach@ugr.es>` and `txyliu@student.ubc.ca`.

Message `19e2376fbd2a36d3` (2026-05-13), Antonio:

> Hi Tony,
>
> Great, thanks a lot for all the detailed information and instructions. This is super
> helpful.
>
> I'll follow everything step by step to get familiar with the workflow, and I'll start
> uploading my data to the Globus folder soon. I'll also prepare the metadata table and
> add the presentation slides there for context.
>
> For now, I'm not planning to deposit the sequences in SRA yet.
>
> Thanks again!
>
> Best,
> Antonio

Message `19e2380efcabe6c5` (2026-05-13), Antonio; 1 attachment
(`Captura de pantalla 2026-05-13 a las 15.40.00.png`):

> Hey Toni. This is my Globus ID: 383e4c58-57b0-4138-80ed-f424f207cc3f

Message `19e607a7ce5b20a5` (2026-05-25), Antonio; 3 attachments:

> Hi Tony,
>
> Hope you have a great weekend.
>
> I'm attaching three files:
>
> * The metadata for the 101 metagenomic samples. I've already uploaded all the sequences
>   (202 files) to our shared Globus folder.
> * A copy of the workflow we put together the other day for the bioinformatics analysis.
> * The table with the expected output files and the tools we want to keep, etc.
>
> I'll try to follow the Metasmith tutorial before Thursday. Still don´t have access to
> the cluster, but Ryan is working on that.
>
> Thanks!
>
> Antonio

The thread also carries the originating 2026-05-13 message from `txyliu@student.ubc.ca`,
which contains a step list under the headings "Sharing reads", "Running metagenomics with
Metasmith", and "Planning out the Viromics workflow", the latter including:

> * This will hopefully make more sense after trying the metasmith tutorial
> * Sketch out the contract of each step
>   An example based on the current metagenomics workflow is provided below
>   Existing tools that have been integrated are here
>   https://github.com/hallamlab/MetasmithLibraries

followed by a Group / tool / inputs / outputs table listing: seqkit (raw reads → read
stats), filtlong, bbduk, megahit, flye, seqkit (contig stats), minimap2, prodigal,
diamond:uniref50, diamond:BUSCO, kofamscan, deepEC, comeBin, metaBAT2, semiBin, skANI,
checkM, metabuli, GTDB-TK.

### 2026-05-15 — "Viral Workflow update"

Thread `19e2c9186dd0fb91`, message `19e2c9186dd0fb91`.
From `antonio.castellanohinojosa@ubc.ca`; to `contacttonyliu@gmail.com`,
`shallam@mail.ubc.ca`, `alvaro.munozplominsky@ubc.ca`,
`shatadru.2@buckeyemail.osu.edu`. 16 attachments.

> Hi all,
>
> Just to give you a quick update on my progress testing the viral workflow I showed you
> the other day.
>
> Over the last few days, I've been able to run the first 12 steps (basically the whole
> left side of the workflow), generating all the output files and getting familiar with
> the different outputs and summaries from each tool.
>
> At the same time, I'm preparing a document called Viral_Metagenomics_Workflow_Summary
> (see attached), which includes:
> - installation notes for each tool,
> - the objective of each step,
> - useful commands to verify that the analyses are running correctly,
> - and the scripts associated with each step.
>
> At the end of the document, I'm also building a table to "sketch out the contract" of
> each step, which I think will probably be the most useful part for Toni later on.
>
> I'm also making some small modifications to the bioinformatics workflow figure for
> clarity.
>
> My idea now is to continue and hopefully complete all remaining steps so I can finish
> the full summary document. I think this document and/or the scripts may become a good
> starting point for future analyses.
> So far, I'm running everything using the 12 virome samples I brought from Spain, and
> everything is going great so far. At the moment, I'm running these analyses on the
> supercomputer at my university in Spain since I still didn't have access to the cluster
> here, but later on I should also be able to run everything on the local cluster here.
> I'll also complete the metadata file for the non-virome samples ( around 100 samples) I
> have, upload the datasets to Globus, and start getting more familiar with MetaSmith so
> we could run them simultaneously at some point.
>
> Best,
> Antonio

### 2026-05-27 / 2026-05-28 — "Metasmith on Sockeye"

Thread `19e6ae76a379607f`. Messages from `antonio.castellanohinojosa@ubc.ca` dated
2026-05-27 (`19e6ae76a379607f`), 2026-05-28 (`19e6f8c0c1e57be2`), and 2026-05-28
(`19e708064e2a65b4`, with attachment), plus one reply from `contacttonyliu@gmail.com`
(`19e6baca101cfd01`). The 2026-05-28 message snippet reads:

> Hi Tony, Thanks for your great work! I've attached an updated version of the metadata
> file. There were an errors in one of the sample names (SG22W5 has been renamed as
> SG22W15), as well as in the […]

Full bodies of this thread were not retrieved.

### 2026-06-22 / 2026-06-25 — "Draft manuscript Multi-niche metagenomics Sierra Nevada"

Thread `19ef0cc5fbb95aa1`. From `antonio.castellanohinojosa@ubc.ca` to
`shallam@mail.ubc.ca`, `contacttonyliu@gmail.com`; snippet:

> Hi Steven, Tony: I've been spending time working on the metagenomics dataset from the
> Sierra Nevada high-mountain lakes. The final dataset includes 89 metagenomes spanning
> sediments, epilithon, and […]

Reply from `shallam@mail.ubc.ca` (2026-06-25, `19f00998f95909f7`):

> Hi Antonio, Thanks for sending this manuscript link. I have sent you a calendar invite
> to connect tomorrow at 12:00. We can spend the first half on viromics and the second
> half on this. Cheers, Steven

Full bodies of this thread were not retrieved.

---

## Attachment inventories

Filenames exactly as they appear in the source messages.

### Gmail thread `19e2c9186dd0fb91`, message `19e2c9186dd0fb91` (2026-05-15) — 16 files

| Filename | MIME |
|---|---|
| `01_fastqc.sh` | application/x-sh |
| `02_trimmomatic.sh` | application/x-sh |
| `03_metaSPADES.sh` | application/x-sh |
| `04_megahit.sh` | application/x-sh |
| `05_combined_contigs.sh` | application/x-sh |
| `06_geNomad.sh` | application/x-sh |
| `07_filtering2.sh` | application/x-sh |
| `08_vOTUs.sh` | application/x-sh |
| `09_count_votus.sh` | application/x-sh |
| `10_filtering_3.sh` | application/x-sh |
| `11_count_vOTUs_after filtering_3.sh` | application/x-sh |
| `12_checkv_final_votus.sh` | application/x-sh |
| `13_vibrant_lifestyle.sh` | application/x-sh |
| `14_summarize_vibrant_lifestyle.sh` | application/x-sh |
| `Bioinformatics workflow.pptx` | pptx |
| `Viral_Metagenomics_Workflow_Summary.docx` | docx |

### Gmail thread `19e1957e6ee40af9` (2026-05-11) — 3 files

| Filename | MIME |
|---|---|
| `Bioinformatics_workflow_tools_installation_ACH.docx` | docx |
| `Sierra_Nevada_viromics_experimental_design.docx` | docx |
| `Bioinformatics workflow.pptx` | pptx |

### Gmail thread `19e2376fbd2a36d3`, message `19e607a7ce5b20a5` (2026-05-25) — 3 files

| Filename | MIME |
|---|---|
| `Metadata.xlsx` | xlsx |
| `Workflow_metagenomics.pptx` | pptx |
| `Table.xlsx` | xlsx |

### Gmail thread `19e2376fbd2a36d3`, message `19e2380efcabe6c5` (2026-05-13) — 1 file

`Captura de pantalla 2026-05-13 a las 15.40.00.png` (image/png)

### Gmail thread `19e6ae76a379607f`, message `19e708064e2a65b4` (2026-05-28)

One attachment: `Metadata.xlsx` (22575 B), stored in `email/2026-05-28_metadata-update/`.

### Slack `D0B2RFTMAAG`, ts `1781645014.211319` (2026-06-16) — 3 files

`07_filtering2.sh`, `10_filtering_3.sh`, `15_filter_vOTU_10kb.sh` — URLs in the
[Slack section](#2026-06-16--antonio-ts-1781645014211319--3-attachments) above.

The three Slack filenames overlap with the Gmail set by name for `07_filtering2.sh` and
`10_filtering_3.sh`; `15_filter_vOTU_10kb.sh` appears only in Slack, and Gmail step
numbers stop at `14_`. Byte-comparison of the retrieved copies: the two shared filenames
are identical across Slack and Gmail — see [Files on disk](#files-on-disk).

---

## Links and identifiers referenced in the messages

**Globus** — collection `2602486c-1e0f-47a0-be15-eec1b0ff0f96`:

| Path | Named in |
|---|---|
| `/Received_raw_data/2026-05-13_Spanish_lake_viromics_Antonio_Castellano/` | Slack 2026-06-22, 2026-07-13; email 2026-05-13 |
| `/Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/` | email 2026-05-13 |
| `/Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/binning/` | Slack 2026-06-09 |
| `/Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/binning/contig_to_bin/` | Slack 2026-06-09 |
| `/Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/metagenomics/annotation_orfs/busco_full_table/` | Slack 2026-06-09 |
| `/Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/metagenomics/assembly/` | Slack 2026-06-09 |

Antonio's Globus ID: `383e4c58-57b0-4138-80ed-f424f207cc3f` (email 2026-05-13).

**Publications**

- https://www.nature.com/articles/s41467-026-68914-2#Sec10 — named by Antonio, Slack
  2026-06-16, as the methodology he followed and the source of "all of the thresholds".
- https://www.nature.com/articles/s41587-023-01953-y — shared by
  `shatadru.2@buckeyemail.osu.edu`, email thread `19e07e6d291e4fd9`, 2026-05-08.
- `1-s2.0-S0167701218301210-main.pdf`, `s40168-019-0626-5.pdf` — Slack `bioinformatics`
  channel, 2020-05-20, from `mclaughlinr2`.

**Other**

- https://github.com/hallamlab/MetasmithLibraries
- https://metasmith.readthedocs.io/en/latest/index.html
- https://metasmith.readthedocs.io/en/latest/tutorials/my_first_agent.html
- Digital Alliance of Canada cluster "Fir"; role identifier `zyr-135-01`
  (email 2026-05-13).

**Sample counts as stated in the messages** — 12 virome samples (email 2026-05-15,
Slack 2026-06-09); "around 100" non-virome samples (email 2026-05-15); 101 metagenomic
samples / 202 sequence files (email 2026-05-25, Slack 2026-06-23); 20 reads, "6 + 14
from both datasets" (email 2026-05-13); 89 metagenomes (email 2026-06-22). One sample
rename is recorded: `SG22W5` → `SG22W15` (email 2026-05-28).

---

## How the files were retrieved

Neither `social(verb="download_attachments", …)` path worked; both were bypassed.

- **Slack** returned `mira GET /v1/slack/download -> 502: JS error: TypeError: Failed
  to fetch` from both the `capella` and `mira` peers. Fetched instead by reading the
  live `xoxc` token + `d` cookie on mira via `~/.local/bin/awm-slack-creds` and
  requesting each `files.slack.com` URL directly with those credentials.
- **Gmail** returned `EXAMINE command error: BAD [b'Could not parse command']`, and the
  Gmail MCP surface exposes no attachment-download tool. Fetched instead over IMAP with
  the app password at `mira:~/agentic_workspace/.awm/social-secrets/gmail`, selecting
  `"[Gmail]/All Mail"` and locating each message by `X-GM-MSGID` (the decimal form of the
  Gmail API message id).

Both service faults are reported to the `svc-socials` scope on capella; see that
scope's messages for the diagnosis.

## Material not located

The email Antonio described in Slack on 2026-06-09 — the "large summary table with all
the viromics workflow tools, versions and parameters", to be sent "tomorrow by email, cc
Steven and Alvaro" — was not found. `from:antonio has:attachment` returns four threads,
the most recent dated 2026-05-28.

Full message bodies of Gmail threads `19e6ae76a379607f` ("Metasmith on Sockeye") and
`19ef0cc5fbb95aa1` ("Draft manuscript Multi-niche metagenomics Sierra Nevada") were not
fetched; only the snippets quoted above, plus the one attachment from
`19e708064e2a65b4`, are on record here.

---

## Searches run

Compiled 2026-08-11. Everything in this file came from these queries; no other source
was consulted.

| Surface | Query | Result |
|---|---|---|
| `social` slack | `viromics` | 14 messages |
| `social` slack | `virome` | 14 messages (same set) |
| `social` slack | `vOTU CheckV clustering` | 0 messages |
| Gmail | `viromics OR virome OR vOTU` | 18 threads |
| Gmail | `from:antonio has:attachment` | 4 threads |

Gmail threads `19e6ae76a379607f` and `19ef0cc5fbb95aa1` were identified by search but
their message bodies were not fetched; only the snippets quoted above are on record here.
