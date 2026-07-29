<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, loadRuns, select } from '../lib/state.svelte.js'
  import { runSuffix } from '../lib/runname.js'
  import Ago from '../components/Ago.svelte'
  import JobLog from '../components/JobLog.svelte'
  import SidePanel from '../components/SidePanel.svelte'
  import FileTree from '../components/FileTree.svelte'
  import FilePreview from '../components/FilePreview.svelte'

  let { workflow, run } = $props()

  const STAGES = ['staged', 'running', 'completed', 'collected']

  let rec = $state(null)
  let log = $state({ lines: [], error: null })
  let results = $state(null)
  let trace = $state(null)
  let steps = $state([])
  let tree = $state(null)
  let picked = $state(null)
  let jobId = $state(null)
  let busy = $state(false)

  async function load() {
    rec = await api.get(`/runs/${workflow}/${run}`)
    results = await api.get(`/runs/${workflow}/${run}/results`)
  }

  // The plan's steps and the trace answer different halves of the question.
  // The trace lists the tasks that were *attempted*; the plan lists every step
  // there is. A step the run never reached appears in one and not the other,
  // and that difference is exactly what "not started" means.
  async function loadSteps() {
    try {
      const wf = await api.get(`/workflows/${workflow}`)
      steps = wf?.result?.step_display ?? []
    } catch {
      steps = []
    }
  }

  async function loadTrace() {
    try {
      trace = await api.get(`/runs/${workflow}/${run}/trace`)
    } catch (e) {
      trace = { tasks: [], failed: 0, error: e.message }
    }
  }

  // The tree deliberately does not join the 8-second poll: there is nothing to
  // walk until collect has run, and once it has the folder does not change.
  async function loadTree() {
    try {
      tree = await api.get(`/runs/${workflow}/${run}/tree`)
    } catch (e) {
      tree = { collected: false, root: null, error: e.message }
    }
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
    trace = null
    steps = []
    tree = null
    picked = null
    jobId = null
    log = { lines: [], error: null }
    attempt(async () => {
      await load()
      await attachJob()
      await Promise.all([tail(), loadTrace(), loadSteps(), loadTree()])
      void w, r
    })
  })

  // Polling is server-side rate-limited; this only asks while the run is live.
  // The trace rides along because a live run is exactly when a step turning red
  // is news; a finished one is fetched once and left alone.
  $effect(() => {
    if (!rec?.live) return
    const t = setInterval(async () => {
      await tail()
      await loadTrace()
      await load()
      await loadRuns()
    }, 8000)
    return () => clearInterval(t)
  })

  // -- the progress bar ------------------------------------------------------
  //
  // Four segments, four states, and the one that matters is `failed`: a run
  // whose steps died under an ignoring error strategy still reports `completed`,
  // so "green everywhere" would be the page repeating the lie rather than
  // reading the trace it already has.
  let traceFailed = $derived((trace?.failed ?? 0) > 0)
  let stageStates = $derived.by(() => {
    if (!rec) return STAGES.map(() => 'idle')
    const s = rec.state
    const staging = s === 'staging' || s === 'launching'
    const running = s === 'running'
    const done = s === 'completed'
    const collected = !!results?.collected
    const dead = s === 'failed' || s === 'cancelled'
    // Whether the run ever got off the ground: a stage that failed never
    // launched, so the red belongs on `staged` rather than on `running`.
    const launched = !!rec.launched_at
    const ended = done ? (traceFailed ? 'failed' : 'done') : null
    return [
      staging ? 'running'
        : dead && !launched ? 'failed'
        : running || done || collected || dead ? 'done'
        : 'idle',
      running ? 'running'
        : dead && launched ? 'failed'
        : ended ?? 'idle',
      dead ? 'failed' : ended ?? 'idle',
      collected ? 'done' : 'idle',
    ]
  })

  // -- per-step status -------------------------------------------------------
  //
  // Nextflow names a task `<process> (<n>)`, so the process is the part before
  // the space. Grouping on it joins the trace's attempts back to the plan's
  // steps, and a plan step with no attempts is one the run never reached.
  function processOf(name) {
    return String(name ?? '').split(' ')[0]
  }

  let stepRows = $derived.by(() => {
    const byProcess = new Map()
    for (const t of trace?.tasks ?? []) {
      const k = processOf(t.name)
      if (!byProcess.has(k)) byProcess.set(k, [])
      byProcess.get(k).push(t)
    }
    const rows = steps.map((s) => ({
      order: s.order,
      process: s.process,
      transform: s.transform,
      produces: s.produces ?? [],
      tasks: byProcess.get(s.process) ?? [],
    }))
    // A trace row whose process is in no plan step still gets shown: it ran, and
    // silently dropping it would make the card less trustworthy than the log.
    const known = new Set(rows.map((r) => r.process))
    for (const [k, tasks] of byProcess) {
      if (!known.has(k)) rows.push({ order: null, process: k, transform: k, produces: [], tasks })
    }
    return rows.map((r) => ({ ...r, state: rollup(r.tasks) }))
  })

  function rollup(tasks) {
    if (!tasks.length) return 'idle'
    if (tasks.some((t) => t.state === 'failed')) return 'failed'
    if (tasks.some((t) => t.state === 'running')) return 'running'
    if (tasks.every((t) => t.state === 'done')) return 'done'
    return 'other'
  }

  // Where collect puts a task's own log inside the results library. Reached
  // through `logs.latest`, which the file route resolves like any other link,
  // so this does not have to know the run's timestamp.
  function stepLogNode(task) {
    const h = String(task.hash ?? '').replace('/', '-')
    if (!h || !tree?.collected) return null
    return {
      name: `${processOf(task.name)}_${h}.log`,
      path: `_metadata/logs.latest/steps/${processOf(task.name)}_${h}.log`,
      size: null, type_name: null, dangling: false, type: 'file',
    }
  }

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

  // Deleting a run archives it, so the pane you are looking at is where the
  // way back belongs -- the same shape the workflow and the agent already have.
  async function unarchive() {
    await attempt(async () => {
      await api.post(`/runs/${workflow}/${run}/archive`, { archived: false })
      await load()
      await loadRuns()
    })
  }

  // A finished job is the one moment the results folder changes under us.
  async function afterJob() {
    await load()
    await Promise.all([loadTree(), loadTrace()])
  }
</script>

{#if !rec}
  <p class="loading muted">loading…</p>
{:else}
  <div class="pane">
  <div class="col main" style="gap:16px">
    <div class="spread">
      <div class="row">
        <!-- the suffix alone: the workflow it belongs to is a row of the table
             a few lines below, and a link to it -->
        <h1>{runSuffix(rec.name, rec.workflow)}</h1>
        <span
          class="tag"
          class:live={rec.live}
          class:ok={rec.state === 'completed'}
          class:bad={rec.state === 'failed'}
        >{rec.state}</span>
        {#if rec.archived_at}<span class="tag warn">archived</span>{/if}
      </div>
      <div class="row">
        {#if rec.archived_at}
          <button onclick={unarchive}>restore</button>
        {/if}
        {#if rec.live}
          <button class="danger" onclick={cancel} disabled={busy}>cancel</button>
        {/if}
        {#if rec.state === 'completed'}
          <button class="primary" onclick={collect}>collect results</button>
        {/if}
      </div>
    </div>

    <div class="progress">
      {#each STAGES as label, i}
        <div class="seg {stageStates[i]}">
          <span class="bar"></span>
          <span class="label">{label}</span>
        </div>
      {/each}
    </div>
    {#if traceFailed}
      <p class="small warnline">
        {trace.failed} task{trace.failed === 1 ? '' : 's'} failed. Nextflow was told to
        ignore step failures, so the run finished and was recorded as completed —
        the steps below are what actually happened.
      </p>
    {/if}

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
          <tr>
            <td class="muted">started</td>
            <td><Ago iso={rec.launched_at ?? rec.created_at} /></td>
          </tr>
          {#if rec.finished_at}
            <tr><td class="muted">finished</td><td><Ago iso={rec.finished_at} /></td></tr>
          {/if}
          {#if rec.preset}<tr><td class="muted">preset</td><td class="mono">{rec.preset}</td></tr>{/if}
          <!-- A run is reproducible only if it says what it was launched with,
               and neither of these is visible anywhere else once the launch
               panel has been left. The agent's own defaults are layered in on
               the agent, so what is listed here is what this run asked for. -->
          {#if rec.params && Object.keys(rec.params).length}
            <tr>
              <td class="muted">params</td>
              <td class="mono small">
                {#each Object.entries(rec.params) as [k, v]}
                  <div>{k} = {v}</div>
                {/each}
              </td>
            </tr>
          {/if}
          {#if rec.resource_overrides && Object.keys(rec.resource_overrides).length}
            <tr>
              <td class="muted">resources</td>
              <td class="mono small">
                {#each Object.entries(rec.resource_overrides) as [step, spec]}
                  <div>
                    step {step} —
                    {Object.entries(spec).map(([k, v]) => `${k} ${v}`).join(', ')}
                  </div>
                {/each}
              </td>
            </tr>
          {/if}
          {#if rec.error}<tr><td class="muted">error</td><td class="bad">{rec.error}</td></tr>{/if}
        </tbody>
      </table>
    </div>

    <!-- Stage first, run second: that is the order they happen in, and the
         staging log is the one that explains a run that never started. -->
    <div class="card col" style="gap:8px">
      <h3>stage log</h3>
      <JobLog {jobId} onend={afterJob} />
    </div>

    <div class="card col" style="gap:8px">
      <div class="spread">
        <h3>run log</h3>
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

    <div class="card col" style="gap:8px">
      <div class="spread">
        <h3>steps</h3>
        <button class="small" onclick={loadTrace}>refresh</button>
      </div>
      {#if trace?.error}
        <p class="small muted">{trace.error}</p>
      {/if}
      {#if !stepRows.length}
        <p class="small muted">
          No per-task record yet. Nextflow writes one row per task as it goes, and
          it arrives with the run's logs.
        </p>
      {:else}
        <div class="scroll">
          <table class="small">
            <thead>
              <tr>
                <th></th><th>step</th><th>tasks</th>
                <th>status</th><th>exit</th><th>duration</th><th>peak rss</th>
              </tr>
            </thead>
            <tbody>
              {#each stepRows as row}
                <tr class="steprow">
                  <td><span class="pip {row.state}"></span></td>
                  <td class="mono">{row.process}</td>
                  <td class="muted">{row.tasks.length || '—'}</td>
                  <td colspan="4" class="muted">
                    {row.state === 'idle' ? 'not started' : ''}
                    {#if row.produces.length}
                      <span class="produces">→ {row.produces.join(', ')}</span>
                    {/if}
                  </td>
                </tr>
                {#each row.tasks as t}
                  <tr class="task">
                    <td></td>
                    <td class="mono muted">
                      {#if stepLogNode(t)}
                        <button class="link" onclick={() => (picked = stepLogNode(t))}>
                          {t.hash}
                        </button>
                      {:else}{t.hash}{/if}
                    </td>
                    <td class="muted">{t.name}</td>
                    <td class={t.state}>{t.status}</td>
                    <td class:bad={t.exit !== 0 && t.exit != null}>{t.exit ?? '—'}</td>
                    <td class="muted">{t.duration ?? '—'}</td>
                    <td class="muted">{t.peak_rss ?? '—'}</td>
                  </tr>
                {/each}
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </div>

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
        <!-- What was asked for, before what came back: a collected folder full
             of intermediates looks like a success until it is read against the
             request. -->
        {#if results.targets?.length}
          <table class="small targets">
            <thead><tr><th></th><th>requested output</th><th>delivered</th></tr></thead>
            <tbody>
              {#each results.targets as t}
                <tr>
                  <td><span class="pip {t.count ? 'done' : 'failed'}"></span></td>
                  <td class="mono">{t.type}</td>
                  <td class={t.count ? 'muted' : 'bad'}>
                    {t.count ? `${t.count} file${t.count === 1 ? '' : 's'}` : 'not produced'}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
          {#if results.targets.some((t) => !t.count)}
            <p class="small warnline">
              An output that was never produced means the step that makes it did
              not run or did not succeed — the steps card above says which.
            </p>
          {/if}
        {/if}
        <p class="small muted">
          The files themselves are in the panel on the right — click one to look
          inside it.
        </p>
        <p class="small muted mono">{results.path}</p>
      {/if}
    </div>
  </div>

  <SidePanel
    id="run"
    title="results"
    subtitle={tree?.collected ? (picked?.name ?? 'nothing selected') : 'not collected yet'}
    topDefault={300}
  >
    {#snippet top()}
      <div class="treebox">
        {#if !tree?.collected}
          <p class="small muted" style="padding:8px">
            Nothing to browse until the results are collected.
          </p>
        {:else}
          <FileTree node={tree.root} selected={picked?.path} onpick={(n) => (picked = n)} />
          {#if tree.truncated}
            <p class="small muted" style="padding:8px">
              Listing stopped early — this folder is bigger than the tree will
              walk. What is shown is a prefix, not the whole of it.
            </p>
          {/if}
        {/if}
      </div>
    {/snippet}
    <FilePreview {workflow} {run} node={picked} />
  </SidePanel>
  </div>
{/if}

<style>
  /* The column scrolls, not the page, so the panel sits outside the column's
     scrollbar rather than behind it. `main` is in flush mode for this view
     (App.svelte) so this row owns the height. */
  .pane { display: flex; flex: 1; min-width: 0; height: 100%; align-items: stretch; }
  .main { flex: 1; min-width: 0; overflow-y: auto; padding: 18px; }
  .loading { padding: 18px; }
  .treebox { height: 100%; overflow: auto; }

  /* A progress bar rather than a row of dots: the four stages are consecutive,
     so the thing that reads them is a filled track, and the four states carry
     the whole meaning -- grey nothing yet, blue underway, green done, red
     failed. Nothing here is keyed on position; a segment says only what it
     knows about itself. */
  .progress { display: flex; gap: 4px; }
  .seg { flex: 1; display: flex; flex-direction: column; gap: 5px; min-width: 0; }
  .seg .bar { height: 6px; border-radius: 3px; background: var(--line); }
  .seg .label {
    font-size: 12px;
    color: var(--muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .seg.running .bar { background: var(--accent); }
  .seg.running .label { color: var(--accent); }
  .seg.done .bar { background: var(--ok); }
  .seg.done .label { color: var(--ok); }
  .seg.failed .bar { background: var(--bad); }
  .seg.failed .label { color: var(--bad); }

  /* the same four states again, as a marker beside a row */
  .pip {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--line);
  }
  .pip.running { background: var(--accent); }
  .pip.done { background: var(--ok); }
  .pip.failed { background: var(--bad); }
  .pip.other { background: var(--warn); }

  .warnline { color: var(--warn); }
  .scroll { max-height: 340px; overflow: auto; }
  .steprow td { border-top: 1px solid var(--line); }
  .task td { font-size: 12px; }
  .task .done { color: var(--ok); }
  .task .failed { color: var(--bad); }
  .task .running { color: var(--accent); }
  .produces { color: var(--muted); }
  .targets td { padding-right: 12px; }
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
