<script>
  import { api } from '../lib/api.js'
  import { app, attempt, loadRuns, loadWorkflows, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'
  import JobLog from '../components/JobLog.svelte'
  import InputEditor from './InputEditor.svelte'
  import HintsPanel from './HintsPanel.svelte'

  let { name } = $props()

  let wf = $state(null)
  let types = $state([])
  let jobId = $state(null)
  let launching = $state(false)
  let agentChoice = $state('')
  let presetChoice = $state('')
  let presets = $state({})
  let showDag = $state(false)

  // the editable recipe, kept separate from the frozen result beside it
  let recipe = $state({ sample_type: '', target_types: [] })
  let loadedFor = $state(null)

  async function load() {
    wf = await api.get(`/workflows/${name}`)
    if (loadedFor !== name) {
      loadedFor = name
      recipe = {
        sample_type: wf.request.sample_type ?? '',
        target_types: wf.request.target_types ?? [],
      }
    }
  }

  $effect(() => {
    const n = name
    wf = null
    jobId = null
    showDag = false
    loadedFor = null
    attempt(async () => {
      types = await api.get('/project/types')
      await load()
      void n
    })
  })

  $effect(() => {
    const a = agentChoice
    presets = {}
    if (!a) return
    api.get(`/agents/${a}/presets`).then((p) => (presets = p)).catch(() => (presets = {}))
  })

  let stale = $derived(
    wf?.planned &&
      (JSON.stringify(recipe.target_types) !== JSON.stringify(wf.request.target_types ?? []) ||
        recipe.sample_type !== (wf.request.sample_type ?? '')),
  )

  function toggleTarget(full) {
    recipe.target_types = recipe.target_types.includes(full)
      ? recipe.target_types.filter((t) => t !== full)
      : [...recipe.target_types, full]
  }

  async function generate() {
    const job = await attempt(() => api.post(`/workflows/${name}/generate`, recipe))
    if (job) jobId = job.id
  }

  async function fork() {
    const out = await attempt(async () => {
      const body = await api.post(`/workflows/${name}/fork`, {})
      await loadWorkflows()
      return body
    })
    if (out) select('workflows', out.name)
  }

  async function launch() {
    launching = true
    const out = await attempt(() =>
      api.post('/runs', { workflow: name, agent: agentChoice, preset: presetChoice || null }),
    )
    launching = false
    if (out) {
      await loadRuns()
      select('runs', `${name}/${out.run.name}`)
    }
  }

  async function unarchive() {
    await attempt(async () => {
      await api.post(`/workflows/${name}/archive`, { archived: false })
      await loadWorkflows()
      await load()
    })
  }
</script>

{#if !wf}
  <p class="muted">loading…</p>
{:else}
  <div class="col" style="gap:16px">
    <div class="spread">
      <div class="row">
        <h1>{wf.name}</h1>
        {#if wf.archived_at}<span class="tag warn">archived</span>{/if}
        {#if wf.forked_from}
          <span class="tag">forked from {wf.forked_from}</span>
        {/if}
      </div>
      <div class="row">
        {#if wf.archived_at}<button onclick={unarchive}>restore</button>{/if}
        <button onclick={fork} title="a copy with a different identity, so it re-runs from scratch">
          fork
        </button>
      </div>
    </div>

    <div class="two">
      <!-- the editable recipe -->
      <div class="col" style="gap:14px">
        <div class="card col" style="gap:10px">
          <h3>recipe</h3>

          <Field
            label="what each run starts from"
            hint="the type the planner treats as one sample; every item of this type becomes its own branch"
          >
            <select bind:value={recipe.sample_type}>
              <option value="">choose a type…</option>
              {#each types as t}
                {#if t.full_name}<option value={t.full_name}>{t.full_name}</option>{/if}
              {/each}
            </select>
          </Field>

          <Field label="what you want out">
            <div class="targets">
              {#each types as t}
                {#if t.full_name}
                  <label class="small row" style="gap:4px">
                    <input
                      type="checkbox"
                      style="width:auto"
                      checked={recipe.target_types.includes(t.full_name)}
                      onchange={() => toggleTarget(t.full_name)}
                    />
                    <span class="mono">{t.full_name}</span>
                  </label>
                {/if}
              {/each}
            </div>
          </Field>

          <div class="row">
            <button
              class="primary"
              onclick={generate}
              disabled={!recipe.sample_type || recipe.target_types.length === 0}
            >{wf.planned ? 'regenerate' : 'generate'}</button>
            {#if stale}
              <span class="tag warn">recipe changed — the result below is from the old one</span>
            {/if}
          </div>
          <p class="small muted">
            Generating is always explicit: the result beside this is never
            recomputed behind your back, and regenerating mints a new task key.
          </p>
        </div>

        <div class="card">
          <InputEditor workflow={wf.name} onchange={load} />
        </div>
      </div>

      <!-- the frozen result -->
      <div class="col" style="gap:14px">
        <JobLog
          {jobId}
          onend={async () => {
            await load()
            await loadWorkflows()
          }}
        />

        {#if !wf.planned}
          <div class="card">
            <h3>result</h3>
            <p class="small muted">
              Nothing generated yet. Pick a starting type and at least one target,
              then generate — the planner works out the steps between them.
            </p>
          </div>
        {:else if wf.success}
          <div class="card col" style="gap:10px">
            <div class="spread">
              <h3>result</h3>
              <span class="tag ok">{wf.step_count} step(s)</span>
            </div>
            <table class="small">
              <tbody>
                <tr><td class="muted">task key</td><td class="mono">{wf.task_key}</td></tr>
                <tr><td class="muted">generated</td><td>{wf.generated_at}</td></tr>
                {#if wf.result.stdlib_commit}
                  <tr>
                    <td class="muted">library</td>
                    <td class="mono truncate">{wf.result.stdlib_commit.slice(0, 12)}</td>
                  </tr>
                {/if}
              </tbody>
            </table>

            <div>
              <button onclick={() => (showDag = !showDag)}>
                {showDag ? 'hide' : 'show'} diagram
              </button>
            </div>
            {#if showDag}
              <div class="dag">
                <img src={`/api/workflows/${wf.name}/dag`} alt="workflow diagram" />
              </div>
            {/if}

            <div class="scroll">
              <table class="small">
                <thead><tr><th>#</th><th>step</th><th>takes</th><th>produces</th></tr></thead>
                <tbody>
                  {#each wf.result.step_display ?? [] as step}
                    <tr>
                      <td class="muted">{step.order}</td>
                      <td class="mono">{step.transform}</td>
                      <td class="mono muted">{step.uses.join(', ') || '—'}</td>
                      <td class="mono muted">{step.produces.join(', ') || '—'}</td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          </div>

          <div class="card col" style="gap:10px">
            <h3>run it</h3>
            <Field label="on which agent">
              <select bind:value={agentChoice}>
                <option value="">choose an agent…</option>
                {#each app.agents.filter((a) => !a.archived_at) as a}
                  <option value={a.name}>{a.name}</option>
                {/each}
              </select>
            </Field>
            {#if Object.keys(presets).length}
              <Field label="nextflow preset">
                <select bind:value={presetChoice}>
                  <option value="">(agent default)</option>
                  {#each Object.keys(presets) as p}<option value={p}>{p}</option>{/each}
                </select>
              </Field>
            {/if}
            <div>
              <button class="primary" onclick={launch} disabled={!agentChoice || launching}>
                {launching ? 'launching…' : 'stage and run'}
              </button>
            </div>
            <p class="small muted">
              The same workflow can run on any agent — staging copies it there
              first, then launches and detaches.
            </p>
          </div>
        {:else}
          <div class="card">
            <HintsPanel result={wf.result} />
          </div>
        {/if}

        {#if wf.runs?.length}
          <div class="card col" style="gap:8px">
            <h3>runs</h3>
            <table class="small">
              <tbody>
                {#each wf.runs as r}
                  <tr>
                    <td>
                      <button class="link" onclick={() => select('runs', `${wf.name}/${r.name}`)}>
                        {r.name}
                      </button>
                    </td>
                    <td class="muted">{r.agent}</td>
                    <td>
                      <span class="tag" class:live={r.live} class:ok={r.state === 'completed'}
                        class:bad={r.state === 'failed'}>{r.state}</span>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}
      </div>
    </div>
  </div>
{/if}

<style>
  .two {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
    gap: 16px;
    align-items: start;
  }
  @media (max-width: 1100px) {
    .two { grid-template-columns: minmax(0, 1fr); }
  }
  .targets {
    max-height: 190px;
    overflow-y: auto;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 6px 8px;
  }
  .scroll { overflow-x: auto; }
  .dag { background: #fff; border-radius: var(--radius); padding: 8px; overflow: auto; }
  .dag img { max-width: 100%; }
  .link {
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    text-align: left;
  }
  .link:hover { text-decoration: underline; border: none; }
</style>
