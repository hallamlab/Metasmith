<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, loadRuns, select } from '../lib/state.svelte.js'
  import { runSuffix } from '../lib/runname.js'
  import Ago from '../components/Ago.svelte'
  import CopyButton from '../components/CopyButton.svelte'
  import JobLog from '../components/JobLog.svelte'
  import SidePanel from '../components/SidePanel.svelte'
  import StageProgress from '../components/StageProgress.svelte'
  import FileTree from '../components/FileTree.svelte'
  import FilePreview from '../components/FilePreview.svelte'
  import AncestryList from '../components/AncestryList.svelte'

  let { workflow, run } = $props()

  const STAGES = ['staging', 'executing', 'finalizing']
  const POLL_MIN_MS = 5000
  const POLL_MAX_MS = 60000

  let rec = $state(null)
  let log = $state({ lines: [], error: null })
  let results = $state(null)
  let trace = $state(null)
  let steps = $state([])
  let tree = $state(null)
  let picked = $state(null)
  let jobId = $state(null)
  let busy = $state(false)

  // Bumped every time the displayed run changes. Each loader below captures
  // the generation it was launched under and checks it again just before
  // writing state -- an in-flight fetch for a run the user has since
  // navigated away from must not win a race and overwrite what is on screen
  // for the run now selected.
  let loadGen = 0

  async function load(w, r, gen) {
    const nextRec = await api.get(`/runs/${w}/${r}`)
    const nextResults = await api.get(`/runs/${w}/${r}/results`)
    if (gen !== loadGen) return
    rec = nextRec
    results = nextResults
  }

  // The plan's steps and the trace answer different halves of the question.
  // The trace lists the tasks that were *attempted*; the plan lists every step
  // there is. A step the run never reached appears in one and not the other,
  // and that difference is exactly what "not started" means.
  async function loadSteps(w, gen) {
    let next
    try {
      const wf = await api.get(`/workflows/${w}`)
      next = wf?.result?.step_display ?? []
    } catch {
      next = []
    }
    if (gen !== loadGen) return
    steps = next
  }

  async function loadTrace(w, r, gen) {
    let next
    try {
      next = await api.get(`/runs/${w}/${r}/trace`)
    } catch (e) {
      next = { tasks: [], failed: 0, error: e.message }
    }
    if (gen !== loadGen) return
    trace = next
  }

  // The tree deliberately does not join the 8-second poll: there is nothing to
  // walk until collect has run, and once it has the folder does not change.
  async function loadTree(w, r, gen) {
    let next
    try {
      next = await api.get(`/runs/${w}/${r}/tree`)
    } catch (e) {
      next = { collected: false, root: null, error: e.message }
    }
    if (gen !== loadGen) return
    tree = next
  }

  // Attach to whatever background job is working on this run. Staging can take
  // a while and the launch was started from the workflow view, so without this
  // the first thing a user sees after pressing the button is an empty log.
  async function attachJob(w, r, gen) {
    try {
      const jobs = await api.get(`/jobs?run=${encodeURIComponent(r)}`)
      const mine = jobs.filter((j) => j.subject?.workflow === w)
      if (gen !== loadGen) return
      if (mine.length) jobId = mine[0].id
    } catch {
      /* the job list is a convenience; its absence is not an error */
    }
  }

  async function tail(w, r, gen) {
    let next
    try {
      next = await api.get(`/runs/${w}/${r}/log?lines=200`)
    } catch (e) {
      next = { lines: [], error: e.message }
    }
    if (gen !== loadGen) return
    log = next
  }

  $effect(() => {
    const w = workflow, r = run
    const gen = ++loadGen
    rec = null
    results = null
    trace = null
    steps = []
    tree = null
    picked = null
    jobId = null
    log = { lines: [], error: null }
    attempt(async () => {
      await load(w, r, gen)
      await attachJob(w, r, gen)
      await Promise.all([tail(w, r, gen), loadTrace(w, r, gen), loadSteps(w, gen), loadTree(w, r, gen)])
    })
  })

  // Polling is server-side rate-limited; this only asks while the run is live.
  // The trace rides along because a live run is exactly when a step turning red
  // is news; a finished one is fetched once and left alone.
  //
  // Backs off 5s -> 10s -> 20s -> 40s -> 60s (capped) instead of a flat
  // interval -- a run that has been executing for twenty minutes does not need
  // asking every 5 seconds, but the countdown next to the manual refresh
  // buttons still tells you exactly when the next ask lands. A manual refresh
  // advances the backoff exactly like the tick it pre-empted would have,
  // rather than resetting it -- pressing the button is "ask now" for this one
  // poll, not "go back to asking every 5 seconds."
  let pollDelay = $state(POLL_MIN_MS)
  let nextPollAt = $state(null)
  let nowTick = $state(Date.now())
  // Plain (non-reactive) handle to the pending tick -- both the scheduling
  // effect and a manual "refresh now" need to cancel the *same* timer, so it
  // cannot live only inside the effect's own closure.
  let pollTimer = null

  async function pollTick() {
    const w = workflow, r = run, gen = loadGen
    await tail(w, r, gen)
    await loadTrace(w, r, gen)
    await load(w, r, gen)
    await loadRuns()
  }

  function scheduleNext() {
    clearTimeout(pollTimer)
    nextPollAt = Date.now() + pollDelay
    pollTimer = setTimeout(async () => {
      await pollTick()
      pollDelay = Math.min(pollDelay * 2, POLL_MAX_MS)
      scheduleNext()
    }, pollDelay)
  }

  $effect(() => {
    if (!rec?.live) {
      clearTimeout(pollTimer)
      nextPollAt = null
      return
    }
    scheduleNext()
    return () => clearTimeout(pollTimer)
  })

  // A 1Hz clock purely to redraw the countdown text -- it never fetches
  // anything itself, it just keeps `nowTick` fresh enough to read against
  // `nextPollAt`.
  $effect(() => {
    const t = setInterval(() => (nowTick = Date.now()), 1000)
    return () => clearInterval(t)
  })

  let countdownSeconds = $derived(
    nextPollAt ? Math.max(0, Math.ceil((nextPollAt - nowTick) / 1000)) : null,
  )

  // A manual refresh acts as if the pending tick had fired early: it polls
  // immediately, then advances the backoff and reschedules from now, the
  // same as `scheduleNext`'s own timeout callback does. `scheduleNext`
  // cancels whatever tick was already pending, so there is nothing stale left
  // to clobber the countdown this reschedules.
  async function refreshNow() {
    await pollTick()
    if (rec?.live) {
      pollDelay = Math.min(pollDelay * 2, POLL_MAX_MS)
      scheduleNext()
    }
  }

  // -- the progress bar ------------------------------------------------------
  //
  // One (stage, status) pair drives the whole bar: `stage` names the single
  // stage the run is currently at (0 staging, 1 executing, 2 finalizing) and
  // `status` is that stage's own condition. Every stage before `stage` is
  // hardcoded green, every stage after it is hardcoded idle -- only the
  // current stage's color comes from `status`. That makes a later stage
  // reading "success" after an earlier one read "fail" structurally
  // unrepresentable: there is exactly one non-green, non-idle segment, ever.
  // Collecting results never feeds into this -- `collect()` only sets `jobId`.
  let traceFailed = $derived((trace?.failed ?? 0) > 0)
  let stateStatus = $derived.by(() => {
    if (!rec) return { stage: 0, status: 'pending' }
    const launched = !!rec.launched_at
    // A dead run never reaches `completed`; whether it ever got off the
    // ground decides whether the failure belongs to `staging` or `executing`.
    if (rec.state === 'failed' || rec.state === 'cancelled') {
      return launched ? { stage: 1, status: 'fail' } : { stage: 0, status: 'fail' }
    }
    // Before `run_number` is assigned -- staging, staged, or launching --
    // trace/log reads resolve through `logs.latest`, which can still point at
    // the *previous* run's directory until its launcher relinks it. Trusting
    // `traceFailed` here paints a failure that belongs to the prior run.
    if (rec.run_number == null) return { stage: 0, status: 'started' }
    // A task failure that Nextflow was told to ignore still lets the run
    // finish as `completed` -- but the run is not a success, so this is
    // reported as soon as it is known rather than waiting for `completed`
    // and momentarily showing blue over a failure that already happened.
    if (traceFailed) return { stage: 1, status: 'fail' }
    if (rec.state === 'running') return { stage: 1, status: 'started' }
    return { stage: 2, status: 'success' }
  })
  const STATUS_COLOR = { pending: 'idle', started: 'running', success: 'done', fail: 'failed' }
  let stageStates = $derived.by(() => {
    const { stage, status } = stateStatus
    return STAGES.map((_, i) => (i < stage ? 'done' : i > stage ? 'idle' : STATUS_COLOR[status]))
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

  // The ancestry list names a parent by its path; selecting it means finding
  // the tree node that path belongs to, since the preview reads a node.
  function nodeAt(path, node = tree?.root) {
    if (!node) return null
    if (node.path === path) return node
    for (const c of node.children ?? []) {
      const hit = nodeAt(path, c)
      if (hit) return hit
    }
    return null
  }

  function pickPath(path) {
    const hit = nodeAt(path)
    if (hit) picked = hit
  }

  async function cancel() {
    busy = true
    await attempt(async () => {
      await api.post(`/runs/${workflow}/${run}/cancel`, {})
      await load(workflow, run, loadGen)
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
      await load(workflow, run, loadGen)
      await loadRuns()
    })
  }

  // A finished job is the one moment the results folder changes under us.
  async function afterJob() {
    const w = workflow, r = run, gen = loadGen
    await load(w, r, gen)
    await Promise.all([loadTree(w, r, gen), loadTrace(w, r, gen)])
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
      </div>
    </div>

    <StageProgress stages={STAGES} {stageStates} />
    {#if rec.survivors?.length}
      <p class="small warnline">
        cancel left {rec.survivors.length} process{rec.survivors.length === 1 ? '' : 'es'}
        running on the agent, so this run is not stopped. Reclaim them with
        <code>metasmith workflow reap {rec.agent} {rec.task_key}</code>.
      </p>
    {/if}
    {#if traceFailed && rec.state !== 'staging' && rec.state !== 'launching'}
      <p class="small warnline">
        {trace.failed} task{trace.failed === 1 ? '' : 's'} failed
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
          {#if rec.preset_source}<tr><td class="muted">preset</td><td class="mono">{rec.preset_source}</td></tr>{/if}
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

    {#if rec.staged_path}
      <div class="card col" style="gap:8px">
        <h3>staged directory</h3>
        <div class="row" style="gap:8px; align-items:center">
          <p class="small mono muted" style="margin:0">{rec.staged_path}</p>
          <CopyButton text={rec.staged_path} label="copy the staged directory path" />
        </div>
      </div>
    {/if}

    <div class="card col" style="gap:8px">
      <div class="spread">
        <h3>run log</h3>
        <div class="row" style="gap:8px; align-items:center">
          {#if rec.live}
            <span class="small muted">refreshing in {countdownSeconds}s</span>
          {/if}
          <button class="small" onclick={refreshNow}>refresh now</button>
        </div>
      </div>
      {#if rec.run_number == null}
        <!-- Not just `staging`/`launching` -- `staged` sits between them, and
             `run_number` isn't assigned until the launcher script has actually
             relinked `logs.latest` to the new run's directory. Any state
             before that number lands, a tail read with no `run` still
             resolves through `logs.latest` itself, which can still point at
             the PREVIOUS run right up until the relink. Keying off the
             number rather than naming every pre-run state is what keeps this
             from quietly reopening the gap the next state gets added. -->
        <p class="small muted">nothing yet -- staging</p>
      {:else}
        {#if log.error}
          <p class="small muted">{log.error}</p>
        {/if}
        <pre class="log">{log.lines?.join('\n') || 'nothing yet'}</pre>
      {/if}
    </div>

    <div class="card col" style="gap:8px">
      <div class="spread">
        <h3>steps</h3>
        <div class="row" style="gap:8px; align-items:center">
          {#if rec.live}
            <span class="small muted">refreshing in {countdownSeconds}s</span>
          {/if}
          <button class="small" onclick={refreshNow}>refresh now</button>
        </div>
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
        <div class="scroll col" style="gap:6px">
          {#each stepRows as row}
            <details class="step-details">
              <summary class="steprow">
                <span class="chevron"></span>
                <span class="pip {row.state}"></span>
                <span class="mono">{row.process}</span>
                <span class="muted">
                  {row.tasks.length || '—'} task{row.tasks.length === 1 ? '' : 's'}
                </span>
                <span class="muted">
                  {row.state === 'idle' ? 'not started' : ''}
                  {#if row.produces.length}
                    <span class="produces">→ {row.produces.join(', ')}</span>
                  {/if}
                </span>
              </summary>
              <table class="small steps-table">
                <colgroup>
                  <col style="width:14%" /><col style="width:32%" /><col style="width:14%" />
                  <col style="width:10%" /><col style="width:15%" /><col style="width:15%" />
                </colgroup>
                <thead>
                  <tr>
                    <th>hash</th><th>name</th><th>status</th>
                    <th>exit</th><th>duration</th><th>peak rss</th>
                  </tr>
                </thead>
                <tbody>
                  {#each row.tasks as t}
                    <tr class="task">
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
                </tbody>
              </table>
            </details>
          {/each}
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
        <div class="row" style="gap:8px; align-items:center">
          <p class="small mono muted" style="margin:0">{results?.path}</p>
          <CopyButton text={results?.path} label="copy the results path" />
        </div>
        <div>
          <button onclick={collect} disabled={rec.live}>collect results</button>
        </div>
      {:else if results.error}
        <p class="small muted">
          Collected, but this does not read as a result library: {results.error}
        </p>
      {:else}
        <!-- What was asked for, before what came back: a folder with files in
             it looks like a success until it is read against the request. -->
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
        <details class="step-details" open>
          <summary class="steprow">
            <span class="chevron"></span>
            <span>collected files</span>
            <span class="muted small">
              {tree?.collected ? 'click one to look inside it' : 'not collected yet'}
            </span>
          </summary>
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
        </details>
        <div class="row" style="gap:8px; align-items:center">
          <p class="small muted mono" style="margin:0">{results.path}</p>
          <CopyButton text={results.path} label="copy the results path" />
        </div>
      {/if}
    </div>
  </div>

  <SidePanel
    id="run"
    title="selected result"
    subtitle={picked?.name ?? 'nothing selected'}
    topDefault={240}
  >
    {#snippet top()}
      <AncestryList node={picked} onpick={pickPath} />
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
  .treebox { max-height: 340px; overflow: auto; }

  /* the same four states as StageProgress's segments, as a marker beside a row */
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

  /* Each step folds shut by default -- a workflow with many steps otherwise
     turns this card into a very long always-expanded table. Modeled on the
     one other accordion in the app (WorkflowView's `.dag-details`), except
     this one starts closed rather than open. */
  .step-details { border-top: 1px solid var(--line); }
  .step-details:first-child { border-top: none; }
  .step-details > summary {
    cursor: pointer;
    user-select: none;
    list-style: none;
  }
  .step-details > summary::-webkit-details-marker { display: none; }
  .steprow {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 0;
    border-radius: 4px;
  }
  .steprow:hover { background: var(--panel-2); }

  /* the only replacement for the native disclosure triangle this suppresses --
     without it nothing but the cursor said a row was clickable */
  .chevron {
    flex: 0 0 auto;
    width: 0;
    height: 0;
    border-style: solid;
    border-width: 4px 0 4px 6px;
    border-color: transparent transparent transparent var(--muted);
    transition: transform 0.15s ease;
  }
  .step-details[open] > summary .chevron { transform: rotate(90deg); }

  /* fixed widths shared by every per-step table -- table-layout: auto would
     otherwise size each table's columns from that table's own row content,
     so columns drift out of alignment between one expanded step and the next */
  .steps-table { table-layout: fixed; }
  .steps-table th, .steps-table td {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

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
