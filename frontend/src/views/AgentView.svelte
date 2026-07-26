<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, loadAgents, select } from '../lib/state.svelte.js'
  import { agentPayload, formFromAgent, formProblems, homeUri } from '../lib/agentform.js'
  import AgentFields from './AgentFields.svelte'
  import JobLog from '../components/JobLog.svelte'

  let { name } = $props()

  let agent = $state(null)
  let form = $state(null)
  let runtimes = $state(['APPTAINER', 'DOCKER', 'MAMBA'])
  let jobId = $state(null)
  let ping = $state(null)
  let pinging = $state(false)
  let saved = $state(false)

  // The name is a field like any other -- `PUT /agents/<name>` carries the whole
  // object, and a name that differs from the url is a rename. So the agent's
  // file moves and its run records are re-pointed by the save, not by a second
  // gesture somewhere else.
  let dirty = $derived(
    !!agent && !!form && JSON.stringify(agentPayload(form)) !== JSON.stringify(agentPayload(formFromAgent(agent))),
  )
  let home = $derived(form ? homeUri(form) : '')

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
      adopt(await api.get(`/agents/${n}`))
    })
  })

  async function save() {
    await attempt(async () => {
      const next = await api.put(`/agents/${name}`, agentPayload(form))
      await loadAgents()
      saved = true
      setTimeout(() => (saved = false), 1500)
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
      <h1>{agent.name}</h1>
      <div class="row">
        {#if agent.archived_at}
          <span class="tag warn">archived</span>
          <button onclick={unarchive}>restore</button>
        {/if}
        {#if saved}<span class="tag ok">saved</span>{/if}
        <button onclick={doPing} disabled={pinging}>{pinging ? 'pinging…' : 'ping'}</button>
        <button onclick={save} disabled={!dirty}>save</button>
        <button class="primary" onclick={deploy} disabled={problems.length > 0}>deploy</button>
      </div>
    </div>

    <AgentFields bind:form {runtimes} />

    <div class="row small muted wrap">
      <span>home</span>
      <span class="mono">{home || '—'}</span>
      {#if agent.real_path}
        <span>· resolves to</span>
        <span class="mono">{agent.real_path}</span>
      {/if}
      {#if dirty}<span class="tag warn">unsaved</span>{/if}
    </div>

    <!-- An agent is saveable long before it can be run on: you know it is going
         on a cluster days before the host exists. So what is missing is stated
         rather than enforced here, and enforced where it costs something --
         launching a run refuses the same list. -->
    {#if problems.length}
      <div class="row small wrap incomplete">
        <span class="tag warn">incomplete</span>
        <span>{problems.join(' · ')}</span>
      </div>
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

    <div class="col" style="gap:6px">
      <h3>nextflow presets</h3>
      {#if Object.keys(agent.config_presets ?? {}).length}
        <p class="small muted">
          Named configurations found on this agent; pick one when you launch a run.
        </p>
        <div class="row wrap">
          {#each Object.keys(agent.config_presets) as p}<span class="tag">{p}</span>{/each}
        </div>
      {:else}
        <p class="small muted">
          None yet — they appear once the agent has been deployed.
        </p>
      {/if}
    </div>

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
