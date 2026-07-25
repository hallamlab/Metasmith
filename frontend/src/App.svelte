<script>
  import { api } from './lib/api.js'
  import {
    SECTIONS,
    app,
    attempt,
    clearNotice,
    loadProject,
    refresh,
    select,
  } from './lib/state.svelte.js'
  import Rail from './components/Rail.svelte'
  import DeleteControl from './components/DeleteControl.svelte'
  import SshHost from './views/SshHost.svelte'
  import SshEditor from './views/SshEditor.svelte'
  import SshNew from './views/SshNew.svelte'
  import AgentNew from './views/AgentNew.svelte'
  import AgentView from './views/AgentView.svelte'
  import WorkflowNew from './views/WorkflowNew.svelte'
  import WorkflowView from './views/WorkflowView.svelte'
  import RunView from './views/RunView.svelte'

  $effect(() => {
    attempt(loadProject)
  })

  // reload the section's list whenever the section or the archive filter changes
  $effect(() => {
    const s = app.section
    const a = app.showArchived
    refresh(s)
    void a
  })

  let sel = $derived(app.selected[app.section])

  // -- rail contents -------------------------------------------------------

  let sshItems = $derived([
    { id: 'editor', kind: 'editor' },
    ...app.hosts.map((h) => ({ id: `host:${h.alias}`, kind: 'host', host: h })),
  ])

  let agentItems = $derived(
    app.agents.map((a) => ({ id: a.name, agent: a, dim: !!a.archived_at })),
  )

  let workflowItems = $derived(
    app.workflows.map((w) => ({ id: w.name, wf: w, dim: !!w.archived_at })),
  )

  let runItems = $derived(
    app.runs.map((r) => ({ id: `${r.workflow}/${r.name}`, run: r, dim: !!r.archived_at })),
  )

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
      const out = await api.del(`/workflows/${name}`)
      if (out.action === 'deleted' && sel === name) app.selected.workflows = null
      await refresh('workflows')
    })
  }

  async function removeRun(r) {
    await attempt(async () => {
      await api.del(`/runs/${r.workflow}/${r.name}`)
      if (sel === `${r.workflow}/${r.name}`) app.selected.runs = null
      await refresh('runs')
    })
  }
</script>

<div class="shell">
  <header>
    <nav>
      {#each SECTIONS as s}
        <button class="tab" class:on={app.section === s.id} onclick={() => (app.section = s.id)}>
          {s.label}
        </button>
      {/each}
    </nav>
    <div class="row small muted">
      {#if app.project}
        <span class="mono truncate" title={app.project.root}>{app.project.root}</span>
        {#if !app.project.stdlib.present}
          <span class="tag bad">standard library not cloned</span>
        {/if}
        <span>metasmith {app.project.version}</span>
      {/if}
    </div>
  </header>

  {#if app.notice}
    <div class="notice" class:refused={app.notice.kind === 'refused'}>
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
                <DeleteControl title="remove host" onconfirm={() => removeHost(item.host.alias)} />
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
          <button class="small" onclick={() => select('agents', 'new')}>+ agent</button>
        {/snippet}
        {#snippet row(item)}
          <div class="spread">
            <div class="grow truncate">
              <div>{item.agent.name}</div>
              <div class="small muted truncate mono">{item.agent.home ?? item.agent.error}</div>
            </div>
            <div class="row">
              {#if item.agent.archived_at}<span class="tag warn">arch</span>{/if}
              <DeleteControl
                title="delete agent"
                archives
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
          <button class="small" onclick={() => select('workflows', 'new')}>+ workflow</button>
        {/snippet}
        {#snippet row(item)}
          <div class="spread">
            <div class="grow truncate">
              <div>{item.wf.name}</div>
              <div class="small muted">
                {item.wf.planned
                  ? item.wf.success
                    ? `${item.wf.step_count} step(s)`
                    : 'no plan'
                  : 'not generated'}
                {item.wf.run_count ? ` · ${item.wf.run_count} run(s)` : ''}
              </div>
            </div>
            <div class="row">
              {#if item.wf.live_runs}<span class="tag live">live</span>{/if}
              {#if item.wf.archived_at}<span class="tag warn">arch</span>{/if}
              <DeleteControl
                title="delete workflow"
                archives
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
              <div class="truncate">{item.run.name}</div>
              <div class="small muted truncate">
                {item.run.agent} · {item.run.launched_at ?? item.run.created_at}
              </div>
            </div>
            <div class="row">
              <span
                class="tag"
                class:live={item.run.live}
                class:ok={item.run.state === 'completed'}
                class:bad={item.run.state === 'failed'}
              >{item.run.state}</span>
              <DeleteControl title="delete run" onconfirm={() => removeRun(item.run)} />
            </div>
          </div>
        {/snippet}
      </Rail>
    {/if}

    <main>
      {#if app.section === 'ssh'}
        {#if sel === 'new'}
          <SshNew />
        {:else if sel === 'editor'}
          <SshEditor />
        {:else if sel?.startsWith('host:')}
          {#key sel}<SshHost alias={sel.slice(5)} />{/key}
        {:else}
          <div class="blank">
            <h1>ssh hosts</h1>
            <p class="muted">
              Metasmith connects to remote machines with a bare <span class="mono">ssh &lt;alias&gt;</span>,
              so your own ssh config is what makes a host reachable. Pick a host to
              see it, or add one — metasmith writes into its own block and leaves
              the rest of the file alone.
            </p>
          </div>
        {/if}
      {:else if app.section === 'agents'}
        {#if sel === 'new'}
          <AgentNew />
        {:else if sel}
          {#key sel}<AgentView name={sel} />{/key}
        {:else}
          <div class="blank">
            <h1>agents</h1>
            <p class="muted">
              An agent is a place metasmith can run work: a directory on this
              machine or on a host you reach over ssh. Deploying one installs
              everything it needs there.
            </p>
          </div>
        {/if}
      {:else if app.section === 'workflows'}
        {#if sel === 'new'}
          <WorkflowNew />
        {:else if sel}
          {#key sel}<WorkflowView name={sel} />{/key}
        {:else}
          <div class="blank">
            <h1>workflows</h1>
            <p class="muted">
              A workflow is some inputs and the types you want out of them. The
              planner finds the steps in between. Workflows are not tied to an
              agent — the same one can run anywhere.
            </p>
          </div>
        {/if}
      {:else if sel}
        {#key sel}
          <RunView workflow={sel.split('/')[0]} run={sel.split('/').slice(1).join('/')} />
        {/key}
      {:else}
        <div class="blank">
          <h1>runs</h1>
          <p class="muted">
            Every launch of a workflow, newest first. Runs keep going on the agent
            whether or not this page is open.
          </p>
        </div>
      {/if}
    </main>
  </div>
</div>

<style>
  .shell { display: flex; flex-direction: column; height: 100vh; }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    padding: 0 14px;
    border-bottom: 1px solid var(--line);
    background: var(--panel);
    flex: 0 0 auto;
  }
  nav { display: flex; }
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
  .blank { max-width: 560px; display: flex; flex-direction: column; gap: 10px; }
  .notice {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 14px;
    background: #3a2220;
    border-bottom: 1px solid #6b3a36;
    color: #f0c9c5;
    font-size: 13px;
  }
  .notice.refused { background: #3a3320; border-color: #6b5a2f; color: #efe0bc; }
</style>
