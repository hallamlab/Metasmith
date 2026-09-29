// The GUI twin of docs/metasmith/source/tutorials/my_first_agent.rst: the same
// three E. coli genomes, the same target, reached by clicking instead of typing.
//
// A step's `done(ctx, app)` is polled every frame while the step is open, so it
// reads what is on screen right now and nothing slower. Its `id` is what an
// edit made with `?tourEdit=1` is keyed by, so it outlives a reordering.

import { app } from '../state.svelte.js'

const $ = (sel) => document.querySelector(sel)

const TEMPLATE = 'pangenome_heatmap_from_assembly'

const SHEET = `genome,accession,group
DH10b,GCF_000019425.1,pangenome
K12,GCF_000005845.2,pangenome
EPI300,GCF_049667475.1,pangenome`

const entry = (type) => () => $(`.entry[data-row-type="${type}"]`)
const railRow = (id) => () => (id ? $(`[data-rail-id="${CSS.escape(id)}"]`) : null)

// With a sheet attached every field of a row is a column picker, so "bound" is
// the row being in value mode with that column chosen.
function boundTo(type, column) {
  const el = entry(type)()
  if (!el) return false
  const mode = el.querySelector('.modeswitch button.on')?.textContent.trim()
  return mode === 'value' && [...el.querySelectorAll('select')].some((s) => s.value === column)
}

// A remembered selection is not an action: the tabs keep what was last open,
// so steps that ask for something new compare against what was there when
// the step opened.
const names = (list, key = (x) => x.name) => new Set(list.map(key))
const runId = (r) => `${r.workflow}/${r.name}`

const selectedAgent = (app) => app.agents.find((a) => a.name === app.selected.agents)

// Where a step happens, given what this pass has made so far: the recovery a
// "take me there" and a jump from the Tutorials tab both use.
const atAgent = (l) => ({ section: 'agents', id: l.agent })
const atWorkflow = (l) => ({ section: 'workflows', id: l.workflow })
const atRun = (l) => ({ section: 'runs', id: l.run })

export default {
  id: 'local-pangenome',
  title: 'Your first pangenome, on this machine',
  summary: 'Three E. coli genomes, from their NCBI accessions to a pangenome heatmap, run by a local agent.',
  duration: 'about 20 minutes, most of it waiting',
  makes: ['agent', 'workflow', 'run'],
  steps: [
    {
      id: 'intro',
      chapter: 'start',
      title: 'Your first pangenome',
      body: [
        'This builds a pangenome of three E. coli strains — DH10B, K-12 and EPI300 — from nothing but their NCBI accession numbers, and draws it as a heatmap. Everything runs on this computer.',
        'On the way you make an agent, start a workflow from a template, hand it the three accessions, let metasmith plan the steps, and run them.',
        'A ring marks where to act and a card like this says what to do. Most steps move on by themselves once you have done them. The window in the corner steps back and forth and lists every step; the tutorial button at the top hides it and brings it back.',
      ],
    },

    {
      id: 'open-agents',
      chapter: 'agent',
      title: 'Open the agents',
      target: '[data-tour="tab-agents"]',
      placement: 'bottom',
      body: [
        'An agent is a worker with a home directory: tools are installed there and runs happen there. A local agent lives on this machine; others live on servers you reach over SSH.',
      ],
      do: 'click `Agents`',
      done: (_c, app) => app.section === 'agents',
    },
    {
      id: 'new-agent',
      chapter: 'agent',
      title: 'Make an agent',
      target: '[data-tour="new-agent"]',
      placement: 'right',
      where: () => ({ section: 'agents' }),
      body: [
        'A new agent is made on the spot under a made-up name, and the page it opens on is all editable. Every change saves itself — there is no save button.',
        'Already have a local agent? Click it in the list instead.',
      ],
      do: 'click `+ agent`',
      enter: (c, app) => (c.agentAtEntry = app.selected.agents ?? null),
      done: (c, app) =>
        app.section === 'agents' &&
        'agentAtEntry' in c &&
        !!app.selected.agents &&
        app.selected.agents !== c.agentAtEntry,
      link: (_c, app) => ({ agent: app.selected.agents }),
    },
    {
      id: 'agent-where',
      chapter: 'agent',
      title: 'Keep it on this machine',
      target: '[data-tour="agent-where"]',
      placement: 'bottom',
      where: atAgent,
      body: [
        '`this machine` makes a local agent. `a remote host` sends the work to a server instead — those are set up on the SSH tab.',
      ],
      do: 'choose `this machine`',
      done: () => !!$('[data-tour="agent-where-local"].on'),
      waiting: 'Open your agent from the list on the left first.',
    },
    {
      id: 'agent-runtime',
      chapter: 'agent',
      title: 'Pick a container runtime',
      target: '[data-tour="agent-runtime"]',
      placement: 'right',
      where: atAgent,
      body: [
        'Every tool runs inside its own container, so nothing is installed by hand. Pick the runtime this machine has: Docker on most workstations, Apptainer on shared servers.',
      ],
      do: 'check the runtime is the one this machine has',
      doneText: 'runtime chosen',
      done: () => !!$('[data-tour="agent-runtime"]')?.value,
      advance: false,
      waiting: 'Open your agent from the list on the left first.',
    },
    {
      id: 'deploy',
      chapter: 'agent',
      title: 'Deploy it',
      target: '[data-tour="agent-deploy"]',
      placement: 'bottom',
      where: atAgent,
      wait: true,
      watch: [
        { target: '[data-tour="agent-progress"]', label: 'deploy progress' },
        { target: '[data-tour="agent-log"]', label: 'what it is doing' },
      ],
      body: [
        'Deploying builds the agent\'s home and fetches metasmith\'s own container. The first time takes a few minutes. The bar under the form shows each phase, and the log under that says what is happening.',
        'You can press next and come back: the deploy keeps going.',
      ],
      do: 'click `deploy` and wait for the bar to fill',
      doneText: 'deployed',
      done: (_c, app) => selectedAgent(app)?.deployed === true,
      waiting: 'Open your agent from the list on the left.',
    },

    {
      id: 'open-workflows',
      chapter: 'workflow',
      title: 'Open the workflows',
      target: '[data-tour="tab-workflows"]',
      placement: 'bottom',
      body: ['A workflow is a recipe — what you have and what you want — plus the plan metasmith works out between them.'],
      do: 'click `Workflows`',
      done: (_c, app) => app.section === 'workflows',
    },
    {
      id: 'new-workflow',
      chapter: 'workflow',
      title: 'Start a new one',
      target: '[data-tour="new-workflow"]',
      placement: 'right',
      where: () => ({ section: 'workflows' }),
      body: ['You can start from nothing, or from a template: a finished recipe with the inputs left blank for you.'],
      do: 'click `+ workflow`',
      done: () => !!$('[data-tour="template-select"]'),
    },
    {
      id: 'template',
      chapter: 'workflow',
      title: 'Choose the pangenome template',
      target: '[data-tour="template-select"]',
      placement: 'bottom',
      where: () => ({ section: 'workflows' }),
      body: ['Each template is named for what it makes and what it starts from.'],
      do: `choose \`${TEMPLATE}\``,
      done: () => $('[data-tour="template-select"]')?.value === TEMPLATE,
      waiting: 'Click `+ workflow` to open the template chooser again.',
    },
    {
      id: 'template-preview',
      chapter: 'workflow',
      title: 'See what it builds',
      target: '[data-tour="template-preview"]',
      placement: 'right',
      where: () => ({ section: 'workflows' }),
      body: [
        'This is the plan the template solves to. Each genome is downloaded from NCBI by its accession, the genomes are pooled into one pangenome with PPanGGOLiN, and the pangenome is drawn as a heatmap.',
      ],
      do: 'scroll over it to zoom, or drag it to pan',
      doneText: 'moved it',
      done: () => !!$('[data-tour="template-preview"][data-moved]'),
      advance: false,
      waiting: 'Click `+ workflow` and choose the template again.',
    },
    {
      id: 'create',
      chapter: 'workflow',
      title: 'Create it',
      target: '[data-tour="template-create"]',
      placement: 'top',
      where: () => ({ section: 'workflows' }),
      body: ['The workflow gets a made-up name. Rename it any time by double-clicking the name.'],
      do: 'click `create`',
      enter: (c, app) => (c.workflowsBefore = names(app.workflows)),
      done: (c, app) =>
        app.section === 'workflows' &&
        !!app.selected.workflows &&
        !!c.workflowsBefore &&
        !c.workflowsBefore.has(app.selected.workflows) &&
        !$('[data-tour="template-select"]'),
      link: (_c, app) => ({ workflow: app.selected.workflows }),
      waiting: 'Click `+ workflow` and choose the template again.',
    },
    {
      id: 'recipe',
      chapter: 'workflow',
      title: 'Read the recipe',
      target: '#msm-recipe',
      where: atWorkflow,
      body: [
        'Inputs on top, outputs below. Every row has a type, and types are how the planner matches tools to data.',
        'This one wants a `pangenome::heatmap`. To get it, it needs a pangenome group, a name for each genome, and each genome\'s NCBI accession. The rail on the left draws the lineage: an accession belongs to a genome, and each genome to the group.',
      ],
      waiting: 'Open the workflow you just created from the list on the left.',
    },
    {
      id: 'sheet',
      chapter: 'workflow',
      title: 'Paste a sample sheet',
      target: '[data-tour="sample-sheet"]',
      placement: 'bottom',
      where: atWorkflow,
      body: [
        'Three genomes is a table with three rows. Attach it as a sample sheet and each recipe row reads its values from a column, one item per sheet row.',
      ],
      snippet: { text: SHEET, label: 'copy the sample sheet' },
      do: 'copy the sheet, click `paste`, paste it in, then click `use this`',
      done: () => !!$('[data-tour="sample-sheet"].on'),
      waiting: 'Open the workflow you just created from the list on the left.',
    },
    {
      id: 'bind-group',
      chapter: 'workflow',
      title: 'The group reads `group`',
      target: entry('pangenome::pangenome'),
      placement: 'right',
      where: atWorkflow,
      body: [
        'With a sheet attached, every row picks a column. The group row holds a value, not a file, so flip its switch to `value`.',
        'Every sheet row says `pangenome` here, so all three genomes land in one group.',
      ],
      do: 'flip the switch to `value`, then choose the column `group`',
      done: () => boundTo('pangenome::pangenome', 'group'),
      waiting: 'Paste the sample sheet first.',
    },
    {
      id: 'bind-name',
      chapter: 'workflow',
      title: 'Names read `genome`',
      target: entry('ncbi::genome_name'),
      placement: 'right',
      where: atWorkflow,
      body: ['Each genome is named by the sheet\'s first column. The name is what labels it on the heatmap.'],
      do: 'flip the switch to `value`, then choose the column `genome`',
      done: () => boundTo('ncbi::genome_name', 'genome'),
      waiting: 'Paste the sample sheet first.',
    },
    {
      id: 'bind-accession',
      chapter: 'workflow',
      title: 'Accessions read `accession`',
      target: entry('ncbi::assembly_accession'),
      placement: 'right',
      where: atWorkflow,
      body: ['The accession is what the download step fetches. This is the last blank in the recipe.'],
      do: 'flip the switch to `value`, then choose the column `accession`',
      done: () => boundTo('ncbi::assembly_accession', 'accession'),
      waiting: 'Paste the sample sheet first.',
    },
    {
      id: 'solve',
      chapter: 'workflow',
      title: 'Solve',
      target: '[data-tour="solve"]',
      placement: 'right',
      where: atWorkflow,
      wait: true,
      watch: [{ target: '[data-tour="solve-progress"]', label: 'solving' }],
      body: [
        'Solving asks the planner to work backwards from the heatmap to what you gave it, choosing a tool for every step between. It takes a few seconds.',
      ],
      do: 'click `solve`',
      doneText: 'solved',
      done: () => !!$('[data-tour="plan-ok"]'),
    },
    {
      id: 'plan',
      chapter: 'workflow',
      title: 'Read the plan',
      target: '[data-tour="plan"]',
      where: atWorkflow,
      body: [
        'Three steps: download each genome, build the pangenome, draw the heatmap. The download runs once per genome, side by side, and all three feed the one pangenome.',
        'The boxes beside each step override its cpus, memory and time for this workflow only.',
      ],
      do: 'hover a step to light what it needs, or click one to read about it on the right',
      doneText: 'explored',
      touch: '[data-tour="plan"] .dag-scroll button',
      advance: false,
      // clicking a step opens the panel on the right, which has to be lit
      dim: false,
    },

    {
      id: 'run-agent',
      chapter: 'run',
      title: 'Choose where it runs',
      target: '[data-tour="run-agent"]',
      placement: 'right',
      where: atWorkflow,
      body: ['Agents that are not ready yet are listed but greyed out, with the reason beside them. An agent has to be deployed before it can run anything.'],
      do: 'choose the agent you deployed',
      // the agent this pass made, when it made one: the picker starts on the
      // last agent used, which is not a choice made here
      done: (_c, app, links) => {
        const v = $('[data-tour="run-agent"]')?.value
        if (!v) return false
        return links.agent ? v === links.agent : app.agents.some((a) => a.name === v && a.deployed)
      },
      link: () => ({ agent: $('[data-tour="run-agent"]')?.value }),
      waiting: 'Solve the workflow first — running unlocks once there is a plan.',
    },
    {
      id: 'launch',
      chapter: 'run',
      title: 'Stage and run',
      target: '[data-tour="launch"]',
      placement: 'right',
      where: atWorkflow,
      body: [
        'Staging copies the plan and its inputs into the agent\'s home; then Nextflow runs the steps. You are taken to the run\'s own page.',
      ],
      do: 'click `stage and run`',
      enter: (c, app) => (c.runsBefore = names(app.runs, runId)),
      done: (c, app) =>
        app.section === 'runs' && !!app.selected.runs && !!c.runsBefore && !c.runsBefore.has(app.selected.runs),
      link: (_c, app) => ({ run: app.selected.runs }),
    },
    {
      id: 'watch-run',
      chapter: 'run',
      title: 'Watch it run',
      target: '[data-tour="run-steps"]',
      where: atRun,
      wait: true,
      watch: [
        { target: '[data-tour="run-progress"]', label: 'progress' },
        { target: () => railRow(app.selected.runs)(), label: 'its state' },
      ],
      body: [
        'Each step ticks over as it finishes. The first run also fetches every tool\'s container, so it is slower than the ones after it.',
        'The run belongs to the agent, not to this page: close the tab and it carries on.',
      ],
      do: 'wait for the run to complete',
      doneText: 'completed',
      done: (_c, app, links) => app.runs.find((r) => runId(r) === (links.run ?? app.selected.runs))?.state === 'completed',
      waiting: 'Open the run from the Runs tab.',
    },
    {
      id: 'collect',
      chapter: 'run',
      title: 'Collect the results',
      target: '[data-tour="collect"]',
      placement: 'top',
      where: atRun,
      wait: true,
      watch: [{ target: '[data-tour="run-results"]', label: 'results land here' }],
      body: ['Results stay in the agent\'s home until you collect them into this project.'],
      do: 'click `collect results`',
      done: () => !!$('[data-tour="results-collected"]'),
      waiting: 'Collect is on the run\'s page, under results. If it reported an error, read it there, then press next.',
    },
    {
      id: 'heatmap',
      chapter: 'run',
      title: 'Open the heatmap',
      target: '[data-tour="run-results"]',
      placement: 'top',
      where: atRun,
      body: [
        'The table checks what came back against what the recipe asked for.',
      ],
      do: 'open the heatmap in the file tree — it is drawn in the panel on the right',
      doneText: 'opened',
      done: () => /heatmap/i.test($('[data-tour="run-results"] .row.sel')?.textContent ?? ''),
      advance: false,
      dim: false,
    },

    {
      id: 'outro',
      chapter: 'done',
      title: 'That is the whole loop',
      body: [
        'Agent, recipe, solve, run, results. Everything else is a variation on it: other templates, your own files as inputs, a remote agent for the big jobs.',
        'The Tutorials tab lists every tutorial and restarts this one from any chapter.',
      ],
    },
  ],
}
