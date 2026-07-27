<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, loadAgents, select } from '../lib/state.svelte.js'
  import { agentPayload, formFromAgent, formProblems } from '../lib/agentform.js'
  import AgentFields from './AgentFields.svelte'
  import EditableName from '../components/EditableName.svelte'
  import JobLog from '../components/JobLog.svelte'
  import SaveChip from '../components/SaveChip.svelte'

  let { name } = $props()

  let agent = $state(null)
  let form = $state(null)
  let runtimes = $state(['APPTAINER', 'DOCKER', 'MAMBA'])
  let defaultContainer = $state('')
  let jobId = $state(null)
  let ping = $state(null)
  let pinging = $state(false)

  // The name is a field like any other -- `PUT /agents/<name>` carries the whole
  // object, and a name that differs from the url is a rename. So the agent's
  // file moves and its run records are re-pointed by the save, not by a second
  // gesture somewhere else. It is edited on the heading, the way a workflow's is.
  let dirty = $derived(
    !!agent && !!form && JSON.stringify(agentPayload(form)) !== JSON.stringify(agentPayload(formFromAgent(agent))),
  )

  // Two sources, and they answer different questions. The form's own problems
  // are live as you type; the server's are what it saw at the last save, and
  // only it can say whether a host exists. While the form is clean they are the
  // same list, so the saved one is only shown once there is nothing pending.
  let problems = $derived(
    form ? (dirty ? formProblems(form) : (agent?.problems ?? [])) : [],
  )

  function adopt(a) {
    agent = a
    form = formFromAgent(a)
  }

  $effect(() => {
    const n = name
    agent = null
    form = null
    jobId = null
    ping = null
    attempt(async () => {
      const d = await api.get('/defaults/agent').catch(() => null)
      if (d?.runtimes?.length) runtimes = d.runtimes
      if (d?.container) defaultContainer = d.container
      adopt(await api.get(`/agents/${n}`))
    })
  })

  // What the save did that was not asked for: an auto-named agent pointed at a
  // different machine is renamed by the save itself, and being renamed behind
  // your back is only acceptable if you are told. Kept against the name it is
  // about rather than cleared on navigation, because the rename *is* a
  // navigation -- the note has to survive the reselect that follows it, and
  // nothing else.
  let notice = $state(null) // {name, notes}

  async function save() {
    await attempt(async () => {
      const next = await api.put(`/agents/${name}`, agentPayload(form))
      notice = next.notes?.length ? { name: next.name, notes: next.notes } : null
      await loadAgents()
      // a rename moved the object; the rail and this view are keyed by name, so
      // the selection has to follow it or the next read is a 404
      if (next.name !== name) select('agents', next.name)
      else adopt(next)
    })
  }

  async function doPing() {
    pinging = true
    ping = await attempt(() => api.post(`/agents/${name}/ping`))
    pinging = false
  }

  async function deploy() {
    const job = await attempt(() => api.post(`/agents/${name}/deploy`, {}))
    if (job) jobId = job.id
  }

  async function unarchive() {
    await attempt(async () => {
      await api.post(`/agents/${name}/archive`, { archived: false })
      await loadAgents()
      adopt(await api.get(`/agents/${name}`))
    })
  }
</script>

{#if !agent || !form}
  <p class="muted">loading…</p>
{:else}
  <div class="col" style="gap:14px; max-width:760px">
    <div class="spread">
      <div class="row grow">
        <EditableName
          value={form.name}
          hint="enter to accept — the rename happens on save"
          title="rename this agent"
          oncommit={(next) => (form.name = next)}
        />
        <SaveChip {dirty} />
        {#if agent.archived_at}<span class="tag warn">archived</span>{/if}
      </div>
      <div class="row">
        {#if agent.archived_at}<button onclick={unarchive}>restore</button>{/if}
        <button onclick={doPing} disabled={pinging}>{pinging ? 'pinging…' : 'ping'}</button>
        <button onclick={save} disabled={!dirty}>save</button>
        <button class="primary" onclick={deploy} disabled={problems.length > 0}>deploy</button>
      </div>
    </div>

    <AgentFields
      bind:form
      {runtimes}
      realPath={agent.real_path}
      presets={Object.keys(agent.config_presets ?? {})}
      {defaultContainer}
    />

    <!-- An agent is saveable long before it can be run on: you know it is going
         on a cluster days before the host exists. So what is missing is stated
         rather than enforced here, and enforced where it costs something --
         launching a run refuses the same list. -->
    {#if notice?.name === name}
      {#each notice.notes as note}
        <p class="small muted" style="margin:0">{note}</p>
      {/each}
    {/if}

    {#if problems.length}
      <div class="row small wrap incomplete">
        <span class="tag warn">incomplete</span>
        <span>{problems.join(' · ')}</span>
      </div>
    {:else if agent.deployed === false}
      <!-- separate from `problems` on purpose: this one is fixed by pressing
           the button above, not by filling anything in, so it must not be
           allowed to disable it -->
      <p class="small muted" style="margin:0">
        Nothing is installed at this home yet — deploy it before launching a run
        on it.
      </p>
    {/if}

    {#if ping}
      <div class="col" style="gap:6px">
        <div class="spread">
          <h3>ping</h3>
          <span class="tag" class:ok={ping.ok} class:bad={!ping.ok}>
            {ping.ok ? 'reachable' : 'no answer'}
          </span>
        </div>
        <pre class="log">{[...ping.out, ...ping.err].join('\n') || '(no output)'}</pre>
      </div>
    {/if}

    <JobLog
      {jobId}
      onend={async () => {
        await loadAgents()
        adopt(await api.get(`/agents/${name}`))
      }}
    />

    {#if agent.runs?.length}
      <div class="col" style="gap:6px">
        <h3>runs on this agent</h3>
        <table class="small">
          <tbody>
            {#each agent.runs as r}
              <tr>
                <td>
                  <button class="link" onclick={() => select('runs', `${r.workflow}/${r.name}`)}>
                    {r.name}
                  </button>
                </td>
                <td class="muted">{r.workflow}</td>
                <td>{r.state}</td>
              </tr>
            {/each}
          </tbody>
        </table>
        <p class="small muted">
          Renaming this agent takes its runs with it; deleting it archives it
          instead, since a run whose agent is gone cannot be tailed, cancelled,
          or collected.
        </p>
      </div>
    {/if}
  </div>
{/if}

<style>
  .link {
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    text-align: left;
  }
  .link:hover { text-decoration: underline; border: none; }
  .incomplete { color: var(--warn, #efe0bc); }
</style>
