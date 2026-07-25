<script>
  import { api } from '../lib/api.js'
  import { attempt, loadAgents, select } from '../lib/state.svelte.js'
  import { agentPayload, formFromAgent, homeUri } from '../lib/agentform.js'
  import AgentFields from './AgentFields.svelte'
  import JobLog from '../components/JobLog.svelte'

  let { name } = $props()

  let agent = $state(null)
  let form = $state(null)
  let jobId = $state(null)
  let ping = $state(null)
  let pinging = $state(false)
  let saved = $state(false)

  // The name is the directory the agent yaml lives in, so renaming is a move,
  // not an edit -- it stays read-only here.
  let dirty = $derived(
    !!agent && !!form && JSON.stringify(agentPayload(form)) !== JSON.stringify(agentPayload(formFromAgent(agent))),
  )
  let home = $derived(form ? homeUri(form) : '')

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
    attempt(async () => adopt(await api.get(`/agents/${n}`)))
  })

  async function save() {
    await attempt(async () => {
      await api.put(`/agents/${name}`, agentPayload(form))
      adopt(await api.get(`/agents/${name}`))
      await loadAgents()
      saved = true
      setTimeout(() => (saved = false), 1500)
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
        <button onclick={save} disabled={!dirty || !home}>save</button>
        <button class="primary" onclick={deploy}>deploy</button>
      </div>
    </div>

    <AgentFields bind:form nameEditable={false} />

    <div class="row small muted wrap">
      <span>home</span>
      <span class="mono">{home || '—'}</span>
      {#if agent.real_path}
        <span>· resolves to</span>
        <span class="mono">{agent.real_path}</span>
      {/if}
      {#if dirty}<span class="tag warn">unsaved</span>{/if}
    </div>

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
          An agent with runs is archived rather than deleted — a run whose agent
          is gone cannot be tailed, cancelled, or collected.
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
</style>
