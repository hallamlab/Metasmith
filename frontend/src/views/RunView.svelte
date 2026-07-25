<script>
  import { api } from '../lib/api.js'
  import { attempt, loadRuns, select } from '../lib/state.svelte.js'
  import JobLog from '../components/JobLog.svelte'

  let { workflow, run } = $props()

  const STAGES = ['staged', 'running', 'completed', 'collected']

  let rec = $state(null)
  let log = $state({ lines: [], error: null })
  let results = $state(null)
  let jobId = $state(null)
  let busy = $state(false)

  async function load() {
    rec = await api.get(`/runs/${workflow}/${run}`)
    results = await api.get(`/runs/${workflow}/${run}/results`)
  }

  // Attach to whatever background job is working on this run. Staging can take
  // a while and the launch was started from the workflow view, so without this
  // the first thing a user sees after pressing the button is an empty log.
  async function attachJob() {
    try {
      const jobs = await api.get(`/jobs?run=${encodeURIComponent(run)}`)
      const mine = jobs.filter((j) => j.subject?.workflow === workflow)
      if (mine.length) jobId = mine[0].id
    } catch {
      /* the job list is a convenience; its absence is not an error */
    }
  }

  async function tail() {
    try {
      log = await api.get(`/runs/${workflow}/${run}/log?lines=200`)
    } catch (e) {
      log = { lines: [], error: e.message }
    }
  }

  $effect(() => {
    const w = workflow, r = run
    rec = null
    results = null
    jobId = null
    log = { lines: [], error: null }
    attempt(async () => {
      await load()
      await attachJob()
      await tail()
      void w, r
    })
  })

  // Polling is server-side rate-limited; this only asks while the run is live.
  $effect(() => {
    if (!rec?.live) return
    const t = setInterval(async () => {
      await tail()
      await load()
      await loadRuns()
    }, 8000)
    return () => clearInterval(t)
  })

  let stageIndex = $derived(
    rec == null
      ? -1
      : rec.state === 'staging' || rec.state === 'launching'
        ? 0
        : rec.state === 'running'
          ? 1
          : rec.state === 'completed'
            ? results?.collected
              ? 3
              : 2
            : -1,
  )

  async function cancel() {
    busy = true
    await attempt(async () => {
      await api.post(`/runs/${workflow}/${run}/cancel`, {})
      await load()
      await loadRuns()
    })
    busy = false
  }

  async function collect() {
    const job = await attempt(() => api.post(`/runs/${workflow}/${run}/collect`, {}))
    if (job) jobId = job.id
  }
</script>

{#if !rec}
  <p class="muted">loading…</p>
{:else}
  <div class="col" style="gap:16px">
    <div class="spread">
      <div class="row">
        <h1>{rec.name}</h1>
        <span
          class="tag"
          class:live={rec.live}
          class:ok={rec.state === 'completed'}
          class:bad={rec.state === 'failed'}
        >{rec.state}</span>
      </div>
      <div class="row">
        {#if rec.live}
          <button class="danger" onclick={cancel} disabled={busy}>cancel</button>
        {/if}
        {#if rec.state === 'completed'}
          <button class="primary" onclick={collect}>collect results</button>
        {/if}
      </div>
    </div>

    <div class="strip">
      {#each STAGES as label, i}
        <div class="stage" class:done={i < stageIndex} class:now={i === stageIndex}>
          <span class="dot"></span>{label}
        </div>
      {/each}
    </div>

    <div class="card">
      <table class="small">
        <tbody>
          <tr>
            <td class="muted">workflow</td>
            <td>
              <button class="link" onclick={() => select('workflows', rec.workflow)}>
                {rec.workflow}
              </button>
            </td>
          </tr>
          <tr>
            <td class="muted">agent</td>
            <td>
              <button class="link" onclick={() => select('agents', rec.agent)}>{rec.agent}</button>
            </td>
          </tr>
          <tr><td class="muted">task key</td><td class="mono">{rec.task_key}</td></tr>
          <tr><td class="muted">started</td><td>{rec.launched_at ?? rec.created_at}</td></tr>
          {#if rec.finished_at}<tr><td class="muted">finished</td><td>{rec.finished_at}</td></tr>{/if}
          {#if rec.preset}<tr><td class="muted">preset</td><td class="mono">{rec.preset}</td></tr>{/if}
          {#if rec.error}<tr><td class="muted">error</td><td class="bad">{rec.error}</td></tr>{/if}
        </tbody>
      </table>
    </div>

    <div class="card col" style="gap:8px">
      <div class="spread">
        <h3>agent log</h3>
        <button class="small" onclick={tail}>refresh</button>
      </div>
      {#if log.error}
        <p class="small muted">{log.error}</p>
      {/if}
      <pre class="log">{log.lines?.join('\n') || 'nothing yet'}</pre>
      {#if rec.live}
        <p class="small muted">
          Following every 8 seconds. The run is detached on the agent, so closing
          this page — or restarting the server — does not stop or lose it.
        </p>
      {/if}
    </div>

    <JobLog {jobId} onend={load} />

    <div class="card col" style="gap:8px">
      <h3>results</h3>
      {#if !results?.collected}
        <p class="small muted">
          Results live on the agent until you collect them. Collecting copies the
          result library into this run's own outputs folder.
        </p>
        <p class="small mono muted">{results?.path}</p>
        <div>
          <button onclick={collect} disabled={rec.live}>collect results</button>
        </div>
      {:else if results.error}
        <p class="small muted">
          Collected, but this does not read as a result library: {results.error}
        </p>
      {:else}
        <table class="small">
          <thead><tr><th>file</th><th>type</th></tr></thead>
          <tbody>
            {#each results.items as item}
              <tr><td class="mono">{item.path}</td><td class="mono muted">{item.type_name}</td></tr>
            {/each}
          </tbody>
        </table>
        <p class="small muted mono">{results.path}</p>
      {/if}
    </div>
  </div>
{/if}

<style>
  .strip { display: flex; gap: 18px; flex-wrap: wrap; }
  .stage {
    display: flex;
    align-items: center;
    gap: 6px;
    color: var(--muted);
    font-size: 12px;
  }
  .stage .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--line);
  }
  .stage.done { color: var(--ok); }
  .stage.done .dot { background: var(--ok); }
  .stage.now { color: var(--accent); }
  .stage.now .dot { background: var(--accent); }
  .bad { color: var(--bad); }
  .link {
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    text-align: left;
  }
  .link:hover { text-decoration: underline; border: none; }
</style>
