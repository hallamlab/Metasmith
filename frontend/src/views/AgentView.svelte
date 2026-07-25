<script>
  import { api } from '../lib/api.js'
  import { app, attempt, loadAgents, select } from '../lib/state.svelte.js'
  import JobLog from '../components/JobLog.svelte'

  let { name } = $props()

  let agent = $state(null)
  let jobId = $state(null)
  let ping = $state(null)
  let pinging = $state(false)

  $effect(() => {
    const n = name
    agent = null
    jobId = null
    ping = null
    attempt(async () => (agent = await api.get(`/agents/${n}`)))
  })

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
      agent = await api.get(`/agents/${name}`)
    })
  }
</script>

{#if !agent}
  <p class="muted">loading…</p>
{:else}
  <div class="col" style="gap:14px">
    <div class="spread">
      <h1>{agent.name}</h1>
      <div class="row">
        {#if agent.archived_at}
          <span class="tag warn">archived</span>
          <button onclick={unarchive}>restore</button>
        {/if}
        <button onclick={doPing} disabled={pinging}>{pinging ? 'pinging…' : 'ping'}</button>
        <button class="primary" onclick={deploy}>deploy</button>
      </div>
    </div>

    <div class="card">
      <table class="small">
        <tbody>
          <tr><td class="muted">home</td><td class="mono">{agent.home}</td></tr>
          <tr><td class="muted">reached by</td><td>{agent.home_type === 'SSH' ? 'ssh' : 'this machine'}</td></tr>
          <tr><td class="muted">runtime</td><td>{agent.runtime.toLowerCase()}</td></tr>
          <tr><td class="muted">container</td><td class="mono">{agent.container}</td></tr>
          {#if agent.real_path}
            <tr><td class="muted">resolved path</td><td class="mono">{agent.real_path}</td></tr>
          {/if}
          {#if agent.setup_commands.length}
            <tr>
              <td class="muted">setup</td>
              <td class="mono">{agent.setup_commands.join('\n')}</td>
            </tr>
          {/if}
        </tbody>
      </table>
    </div>

    {#if ping}
      <div class="card col">
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
        agent = await api.get(`/agents/${name}`)
      }}
    />

    {#if Object.keys(agent.config_presets ?? {}).length}
      <div class="card col">
        <h3>nextflow presets</h3>
        <p class="small muted">
          Named configurations found on this agent; pick one when you launch a run.
        </p>
        <div class="row wrap">
          {#each Object.keys(agent.config_presets) as p}<span class="tag">{p}</span>{/each}
        </div>
      </div>
    {:else}
      <p class="small muted">
        No nextflow presets yet — they appear once the agent has been deployed.
      </p>
    {/if}

    {#if agent.runs?.length}
      <div class="card col">
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
