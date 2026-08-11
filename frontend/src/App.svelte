<script>
  import { untrack } from 'svelte'
  import { api } from './lib/api.svelte.js'
  import { runSuffix } from './lib/runname.js'
  import {
    SECTIONS,
    app,
    attempt,
    clearNotice,
    createAgent,
    forkWorkflow,
    loadProject,
    loadRuns,
    refresh,
    openRunGroup,
    select,
    selectSection,
    toggleRunGroup,
    toggleTheme,
    ui,
  } from './lib/state.svelte.js'
  import Ago from './components/Ago.svelte'
  import Rail from './components/Rail.svelte'
  import DeleteControl from './components/DeleteControl.svelte'
  import Icon from './components/Icon.svelte'
  import CopyButton from './components/CopyButton.svelte'
  import NewWorkflow from './components/NewWorkflow.svelte'
  import ShareIn from './components/ShareIn.svelte'
  import StatusDot from './components/StatusDot.svelte'
  import SshHost from './views/SshHost.svelte'
  import SshEditor from './views/SshEditor.svelte'
  import SshNew from './views/SshNew.svelte'
  import AgentView from './views/AgentView.svelte'
  import WorkflowView from './views/WorkflowView.svelte'
  import RunView from './views/RunView.svelte'
  // The wordmark's own M *is* the logo mark, so this replaces the whole word
  // rather than sitting beside it. Both variants are imported because both now
  // ship: the "-dark" file is the one drawn *for* a dark ground. Importing is
  // also what makes them exist -- vite emits only what something references,
  // and `setup.py` packages the emitted bundle and nothing else under `gui/`.
  import wordmarkDark from '$icon/metasmith-lockup-dark.svg'
  import wordmarkLight from '$icon/metasmith-lockup-light.svg'

  const wordmark = $derived(ui.theme === 'light' ? wordmarkLight : wordmarkDark)

  // where a release lives. The urls themselves come from the server, which reads
  // them from constants.py -- the one place they are written down.
  // A left-to-right mark, pinned to the front of the path. The path is laid out
  // right-to-left so the ellipsis lands at the *start* and the last segment --
  // the part that says which project this is -- is always the part you can see.
  // Without the mark the leading `/` is a neutral character at the edge of an
  // RTL paragraph, and bidi resolution moves it to the far end: `home/…/proj/`.
  const LRM = '‎'

  const LINKS = [
    { id: 'docs', title: 'documentation' },
    { id: 'github', title: 'source on github' },
    { id: 'conda', title: 'conda package' },
    { id: 'container', title: 'container image on quay.io' },
  ]

  let creating = $state(false)
  let forking = $state(null)
  // `+ workflow` asks what to start from now: blank, or one of the standard
  // library's templates. The create itself still happens in `createWorkflow`;
  // the modal only decides what to pass it.
  let choosing = $state(false)
  // One dialog for all three rails: the payload says what it is, so the button
  // beside `+ add` does not have to, and pasting an agent onto the hosts rail
  // lands the agent rather than being refused for being on the wrong list.
  let importing = $state(false)
  const IMPORT_TITLE = 'paste a host, an agent or a workflow somebody shared with you'

  async function newAgent() {
    creating = true
    await createAgent()
    creating = false
  }

  // Copying a workflow is a list action, so it is on the row: a button in the
  // pane could only ever copy the one workflow already open, and getting a copy
  // of a *different* one meant opening it first. Guarded like the create above,
  // and by name rather than a flag, so two rows do not disable each other.
  async function copyWorkflow(e, name) {
    e.stopPropagation()
    forking = name
    await forkWorkflow(name)
    forking = null
  }

  $effect(() => {
    attempt(loadProject)
  })

  // the dot would otherwise only be as fresh as the last thing that was
  // clicked: a server killed in its terminal reads green until someone saves
  $effect(() => api.watch())

  // reload the section's list whenever the section or the archive filter changes
  $effect(() => {
    const s = app.section
    const a = app.showArchived
    refresh(s)
    void a
  })

  // A run advances on the agent and is written to disk by the watcher, so the
  // rail is stale the moment it is drawn. This is what makes it move on its
  // own: while anything listed is live, re-read the list on the watcher's own
  // cadence. Conditional on the section *and* on something being live, so an
  // idle tab left open on Workflows issues nothing at all.
  const RUN_POLL_MS = 8000
  $effect(() => {
    if (app.section !== 'runs' || !app.runs.some((r) => r.live)) return
    const t = setInterval(() => attempt(loadRuns), RUN_POLL_MS)
    return () => clearInterval(t)
  })

  let sel = $derived(app.selected[app.section])

  // Which views own their own layout row because they carry a side panel. Named
  // here rather than spelled into the class expression so the next one to grow
  // a panel edits a list instead of an `||`.
  const PANELLED = new Set(['workflows', 'runs'])

  // -- rail contents -------------------------------------------------------

  // Two runs of hosts under their own headings: the ones metasmith wrote and can
  // rewrite, and the ones that were already in your config and are only read.
  // Which is which decides whether the form below is editable, so it is worth
  // seeing before you click rather than after.
  let sshItems = $derived.by(() => {
    const row = (h) => ({ id: `host:${h.alias}`, kind: 'host', host: h })
    const managed = app.hosts.filter((h) => h.managed)
    const native = app.hosts.filter((h) => !h.managed)
    return [
      { id: 'editor', kind: 'editor' },
      ...(managed.length ? [{ id: 'h:managed', kind: 'heading', label: 'managed' }] : []),
      ...managed.map(row),
      ...(native.length ? [{ id: 'h:native', kind: 'heading', label: 'native' }] : []),
      ...native.map(row),
    ]
  })

  let agentItems = $derived(
    app.agents.map((a) => ({ id: a.name, agent: a, dim: !!a.archived_at })),
  )

  let workflowItems = $derived(
    app.workflows.map((w) => ({ id: w.name, wf: w, dim: !!w.archived_at })),
  )

  // Runs under the workflow they belong to. The list handed to the rail is still
  // flat -- a shut group simply contributes its heading and none of its rows --
  // and the collapsed set lives outside this derivation, so the eight-second
  // refresh above redraws the rows without reopening anything.
  //
  // Group order follows first appearance in `app.runs`, which the server already
  // sorts newest-first, so the workflow you last ran leads.
  let runItems = $derived.by(() => {
    const groups = new Map()
    for (const r of app.runs) {
      if (!groups.has(r.workflow)) groups.set(r.workflow, [])
      groups.get(r.workflow).push(r)
    }
    const out = []
    for (const [workflow, runs] of groups) {
      const collapsed = ui.collapsedRuns.has(workflow)
      out.push({
        id: `g:${workflow}`,
        kind: 'heading',
        label: workflow,
        count: runs.length,
        collapsed,
        ontoggle: () => toggleRunGroup(workflow),
      })
      if (collapsed) continue
      for (const r of runs) {
        out.push({ id: `${r.workflow}/${r.name}`, run: r, dim: !!r.archived_at })
      }
    }
    return out
  })

  // Selecting a run inside a shut group would leave it selected and invisible,
  // and the selection can move without a click -- a deleted run, a fresh launch.
  //
  // `untrack` matters here: `openRunGroup` itself reads `ui.collapsedRuns`, and
  // without untracking that read becomes one of *this* effect's dependencies
  // too -- so toggling any group re-runs this effect, and if the selected run
  // still lives in the group that was just shut, it gets silently reopened in
  // the same tick. The intent is to react only to the selection changing.
  $effect(() => {
    const id = app.selected.runs
    if (!id) return
    untrack(() => openRunGroup(String(id).split('/')[0]))
  })

  // -- deletions -----------------------------------------------------------

  async function removeHost(alias) {
    await attempt(async () => {
      await api.del(`/ssh/hosts/${alias}`)
      if (sel === `host:${alias}`) app.selected.ssh = null
      await refresh('ssh')
    })
  }

  async function removeAgent(name) {
    await attempt(async () => {
      const out = await api.del(`/agents/${name}`)
      if (out.action === 'deleted' && sel === name) app.selected.agents = null
      await refresh('agents')
    })
  }

  async function removeWorkflow(name) {
    await attempt(async () => {
      await api.del(`/workflows/${name}`)
      if (sel === name) app.selected.workflows = null
      await refresh('workflows')
    })
  }

  // Archived is not gone: the pane stays on it so the restore is where you are
  // already looking. Only a real delete drops the selection.
  //
  // The row updates on this click, not on the response: a click already armed
  // through DeleteControl's confirm step, so waiting on the network on top of
  // that makes the click feel unacknowledged. Guess the outcome from
  // `archived_at` (first press archives, second press on an archived run
  // removes it for good), apply it to `app.runs` immediately, then let the
  // real request run and always resync afterward -- on success this is a
  // no-op, on failure it corrects the guess back to server truth.
  async function removeRun(r) {
    const key = `${r.workflow}/${r.name}`
    const wasArchived = !!r.archived_at
    app.runs = wasArchived
      ? app.runs.filter((x) => `${x.workflow}/${x.name}` !== key)
      : app.runs.map((x) =>
          `${x.workflow}/${x.name}` === key ? { ...x, archived_at: new Date().toISOString() } : x,
        )
    await attempt(async () => {
      const out = await api.del(`/runs/${r.workflow}/${r.name}`)
      if (out.action === 'deleted' && sel === key) app.selected.runs = null
    })
    await refresh('runs')
  }
</script>

<div class="shell">
  <header>
    <div class="brand">
      <img class="wordmark" src={wordmark} alt="Metasmith" />
      {#if app.project}<span class="ver mono">{app.project.version}</span>{/if}
    </div>

    <nav>
      {#each SECTIONS as s}
        <button class="tab" class:on={app.section === s.id} onclick={() => selectSection(s.id)}>
          {s.label}
        </button>
      {/each}
    </nav>

    <!-- takes all the slack: a project path is long and the interesting end
         of it is the last segment, so it gets the room and the tabs do not.
         Empty before the project loads -- it is still the spacer that pushes
         the dot and the links to the right edge. -->
    <div class="where small muted">
      {#if app.project}
        {#if !app.project.stdlib.present}
          <span class="tag bad">standard library not cloned</span>
        {/if}
        <span class="path mono" title={app.project.root}>{LRM}{app.project.root}</span>
        <CopyButton text={app.project.root} label="copy the project path" />
      {/if}
    </div>

    <!-- the glyph is the theme you would switch *to*, not the one showing, and
         the title says so. Outside the project check for the same reason the
         dot beside it is: neither is about the project. -->
    <button
      class="copy"
      onclick={toggleTheme}
      title={ui.theme === 'dark' ? 'switch to the light theme' : 'switch to the dark theme'}
      aria-label={ui.theme === 'dark' ? 'switch to the light theme' : 'switch to the dark theme'}
    >
      <Icon name={ui.theme === 'dark' ? 'sun' : 'moon'} size={14} />
    </button>

    <!-- outside the project check on purpose: whether the server is answering
         is exactly the thing you want to read when nothing has loaded -->
    <StatusDot />

    {#if app.project}
      <div class="links">
        {#each LINKS as l}
          <a href={app.project.links?.[l.id]} target="_blank" rel="noreferrer noopener" title={l.title}>
            <Icon name={l.id} />
            <span class="sr">{l.title}</span>
          </a>
        {/each}
      </div>
    {/if}
  </header>

  {#if app.notice}
    <div
      class="notice"
      class:refused={app.notice.kind === 'refused'}
      class:info={app.notice.kind === 'info'}
    >
      <span class="grow">{app.notice.message}</span>
      <button class="small" onclick={clearNotice}>dismiss</button>
    </div>
  {/if}

  <div class="body">
    {#if app.section === 'ssh'}
      <Rail
        title="hosts"
        items={sshItems}
        selected={sel}
        onselect={(id) => select('ssh', id)}
        empty="no hosts in your ssh config"
      >
        {#snippet actions()}
          <button class="small" onclick={() => (importing = true)} title={IMPORT_TITLE}>import</button>
          <button class="small" onclick={() => select('ssh', 'new')}>+ host</button>
        {/snippet}
        {#snippet row(item)}
          {#if item.kind === 'editor'}
            <div class="spread">
              <span class="small">the config file</span>
              <span class="tag">editor</span>
            </div>
          {:else}
            <div class="spread">
              <div class="grow truncate">
                <div>{item.host.alias}</div>
                <div class="small muted truncate mono">
                  {item.host.hostname ?? '(no hostname)'}
                </div>
              </div>
              <div class="row">
                {#if item.host.managed}<span class="tag ok">msm</span>{/if}
                <!-- an ssh host is a block in a text file you can also edit by
                     hand, so this one really does remove it -->
                <DeleteControl
                  title="remove host"
                  archived
                  onconfirm={() => removeHost(item.host.alias)}
                />
              </div>
            </div>
          {/if}
        {/snippet}
      </Rail>
    {:else if app.section === 'agents'}
      <Rail
        title="agents"
        items={agentItems}
        selected={sel}
        onselect={(id) => select('agents', id)}
        empty="no agents yet"
        showArchivedToggle
        showArchived={app.showArchived}
        ontoggleArchived={(v) => (app.showArchived = v)}
      >
        {#snippet actions()}
          <button class="small" onclick={() => (importing = true)} title={IMPORT_TITLE}>import</button>
          <button class="small" disabled={creating} onclick={newAgent}>+ agent</button>
        {/snippet}
        {#snippet row(item)}
          <div class="spread">
            <div class="grow truncate">
              <div>{item.agent.name}</div>
              <div class="small muted truncate mono">{item.agent.home ?? item.agent.error}</div>
            </div>
            <div class="row">
              <!-- an agent can be half-filled-in for as long as it takes to set
                   the host up, so the list says which ones are ready rather
                   than pretending they all are -->
              {#if item.agent.valid === false}
                <span class="tag warn" title={item.agent.problems?.join(' · ')}>·&nbsp;·&nbsp;·</span>
              {/if}
              {#if item.agent.archived_at}<span class="tag warn">arch</span>{/if}
              <DeleteControl
                title="delete agent"
                archived={!!item.agent.archived_at}
                onconfirm={() => removeAgent(item.agent.name)}
              />
            </div>
          </div>
        {/snippet}
      </Rail>
    {:else if app.section === 'workflows'}
      <Rail
        title="workflows"
        items={workflowItems}
        selected={sel}
        onselect={(id) => select('workflows', id)}
        empty="no workflows yet"
        showArchivedToggle
        showArchived={app.showArchived}
        ontoggleArchived={(v) => (app.showArchived = v)}
      >
        {#snippet actions()}
          <button class="small" onclick={() => (importing = true)} title={IMPORT_TITLE}>import</button>
          <button class="small" onclick={() => (choosing = true)}>+ workflow</button>
        {/snippet}
        {#snippet row(item)}
          <div class="spread">
            <div class="grow truncate">
              <div>{item.wf.display_name || item.wf.name}</div>
              <div class="small muted">
                {item.wf.planned
                  ? item.wf.success
                    ? `${item.wf.step_count} step(s)`
                    : 'no plan'
                  : 'not solved'}
                {item.wf.run_count ? ` · ${item.wf.run_count} run(s)` : ''}
              </div>
            </div>
            <div class="row">
              {#if item.wf.live_runs}<span class="tag live">live</span>{/if}
              {#if item.wf.archived_at}<span class="tag warn">arch</span>{/if}
              <button
                class="rowact"
                disabled={forking === item.wf.name}
                onclick={(e) => copyWorkflow(e, item.wf.name)}
                title="copy — the same recipe under a new identity, so it re-runs from scratch"
                aria-label="copy workflow"
              >
                <Icon name="copy" size={12} />
              </button>
              <DeleteControl
                title="delete workflow"
                archived={!!item.wf.archived_at}
                onconfirm={() => removeWorkflow(item.wf.name)}
              />
            </div>
          </div>
        {/snippet}
      </Rail>
    {:else}
      <Rail
        title="runs"
        items={runItems}
        selected={sel}
        onselect={(id) => select('runs', id)}
        empty="nothing has been run yet"
        showArchivedToggle
        showArchived={app.showArchived}
        ontoggleArchived={(v) => (app.showArchived = v)}
      >
        {#snippet row(item)}
          <div class="spread">
            <div class="grow truncate">
              <div class="truncate">{runSuffix(item.run.name, item.run.workflow)}</div>
              <div class="small muted truncate">
                {item.run.agent} · <Ago iso={item.run.launched_at ?? item.run.created_at} />
              </div>
            </div>
            <div class="row">
              <span
                class="tag"
                class:live={item.run.live}
                class:ok={item.run.state === 'completed'}
                class:bad={item.run.state === 'failed'}
              >{item.run.state}</span>
              <DeleteControl
                title="delete run"
                archived={!!item.run.archived_at}
                onconfirm={() => removeRun(item.run)}
              />
            </div>
          </div>
        {/snippet}
      </Rail>
    {/if}

    <!-- A pane carrying a panel on its right edge cannot live inside a
         scrolling box: the page's scrollbar ends up outside the panel and it
         slides under the header. So for those views `main` stops being the
         scroll container and becomes a plain row -- the view scrolls its own
         column, the panel scrolls its own list. Every other view is unchanged. -->
    <main class:flush={PANELLED.has(app.section) && sel}>
      <!-- Nothing selected draws nothing. Each of these four held a heading, a
           paragraph of prose about what the section is for, and two of them a
           second `new` button beside the one already in the rail's header. It
           is the screen you see for a moment on the way to a selection, so
           nobody reads it -- and the duplicate button made "new agent" a thing
           in two places that had to be kept in step. The rail is the section's
           own explanation. -->
      {#if app.section === 'ssh'}
        {#if sel === 'new'}
          <SshNew />
        {:else if sel === 'editor'}
          <SshEditor />
        {:else if sel?.startsWith('host:')}
          {#key sel}<SshHost alias={sel.slice(5)} />{/key}
        {/if}
      {:else if app.section === 'agents'}
        {#if sel}
          {#key sel}<AgentView name={sel} />{/key}
        {/if}
      {:else if app.section === 'workflows'}
        {#if sel}
          {#key sel}<WorkflowView name={sel} />{/key}
        {/if}
      {:else if sel}
        {#key sel}
          <RunView workflow={sel.split('/')[0]} run={sel.split('/').slice(1).join('/')} />
        {/key}
      {/if}
    </main>
  </div>
</div>

{#if importing}
  <ShareIn onclose={() => (importing = false)} />
{/if}

{#if choosing}
  <NewWorkflow onclose={() => (choosing = false)} />
{/if}

<style>
  .shell { display: flex; flex-direction: column; height: 100vh; }
  header {
    display: flex;
    align-items: center;
    gap: 18px;
    padding: 0 14px;
    border-bottom: 1px solid var(--line);
    background: var(--panel);
    flex: 0 0 auto;
  }
  .brand { display: flex; align-items: center; white-space: nowrap; }
  /* height, never width: the lockup is ~5.5:1 and the header is what constrains
     it. `display: block` so no baseline gap opens under it. */
  .wordmark { display: block; height: 18px; width: auto; }
  .ver { color: var(--muted); font-weight: 400; margin-left: 8px; }
  nav { display: flex; }

  .where {
    flex: 1;
    min-width: 12px;
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 8px;
    color: var(--muted);
  }
  /* an action that lives on a rail row: the same weight as the × beside it, so
     neither reads as the row's purpose */
  .rowact {
    display: flex;
    padding: 3px;
    background: none;
    border-color: transparent;
    color: var(--muted);
  }
  .rowact:hover:not(:disabled) { color: var(--text); background: var(--panel-2); }
  .path {
    min-width: 0;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    /* clip the head, not the tail */
    direction: rtl;
    text-align: left;
  }

  .links { display: flex; align-items: center; gap: 2px; }
  .links a {
    display: flex;
    padding: 6px;
    border-radius: var(--radius);
    color: var(--muted);
  }
  .links a:hover { color: var(--text); background: var(--panel-2); }
  /* the label is for screen readers and nothing else; `title` covers the mouse */
  .sr {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }
  .tab {
    background: none;
    border: none;
    border-bottom: 2px solid transparent;
    border-radius: 0;
    padding: 12px 14px;
    color: var(--muted);
  }
  .tab:hover { color: var(--text); border-color: var(--line); }
  .tab.on { color: var(--text); border-bottom-color: var(--accent); }
  .body { display: flex; flex: 1; min-height: 0; }
  main { flex: 1; min-width: 0; overflow-y: auto; padding: 18px; }
  main.flush { overflow: hidden; padding: 0; display: flex; min-height: 0; }
  .notice {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 14px;
    background: var(--notice-bad-bg);
    border-bottom: 1px solid var(--notice-bad-line);
    color: var(--notice-bad-text);
    font-size: 13px;
  }
  .notice.refused { background: var(--notice-warn-bg); border-color: var(--notice-warn-line); color: var(--notice-warn-text); }
  /* not everything worth saying is a failure: a rename that moved other objects
     with it is a report, and colouring it like an error reads as one */
  .notice.info { background: var(--notice-info-bg); border-color: var(--notice-info-line); color: var(--notice-info-text); }
</style>
