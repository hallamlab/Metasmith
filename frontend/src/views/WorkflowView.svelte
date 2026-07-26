<script>
  import { api } from '../lib/api.js'
  import { app, attempt, loadRuns, loadWorkflows, notify, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'
  import JobLog from '../components/JobLog.svelte'
  import SidePanel from '../components/SidePanel.svelte'
  import EntryBuilder from './EntryBuilder.svelte'
  import HintsPanel from './HintsPanel.svelte'
  import RecipeCard from './RecipeCard.svelte'
  import TypeInspector from './TypeInspector.svelte'

  let { name } = $props()

  let wf = $state(null)
  let types = $state([])
  let index = $state(null)
  let items = $state([])
  let jobId = $state(null)
  let launching = $state(false)
  let agentChoice = $state('')
  let presetChoice = $state('')
  let presets = $state({})
  let focus = $state(null)

  // the editable recipe, kept separate from the frozen result below it
  let recipe = $state({ sample_type: '', targets: [], transform_libraries: [] })
  let loadedFor = $state(null)

  // Targets were a list of bare type names before they could carry lineage.
  // Both spellings still arrive from disk, so both are read here.
  function normalize(list) {
    return (list ?? []).map((t) =>
      typeof t === 'string'
        ? { type: t, parents: [] }
        : { type: t.type, parents: [...(t.parents ?? [])] },
    )
  }

  async function load() {
    wf = await api.get(`/workflows/${name}`)
    if (loadedFor !== name) {
      loadedFor = name
      recipe = {
        sample_type: wf.request.sample_type ?? '',
        targets: normalize(wf.request.target_types),
        transform_libraries: wf.request.transform_libraries ?? [],
      }
    }
  }

  async function loadInputs() {
    items = (await api.get(`/workflows/${name}/inputs`)).items ?? []
  }

  $effect(() => {
    const n = name
    wf = null
    jobId = null
    focus = null
    loadedFor = null
    attempt(async () => {
      types = await api.get('/project/types')
      index = await api.get('/project/type-index')
      await load()
      await loadInputs()
      void n
    })
  })

  $effect(() => {
    const a = agentChoice
    presets = {}
    if (!a) return
    api.get(`/agents/${a}/presets`).then((p) => (presets = p)).catch(() => (presets = {}))
  })

  // an empty list means every library, here and on the server -- so the filter
  // is null rather than an empty Set, which would mean the opposite
  let enabled = $derived(
    recipe.transform_libraries.length ? new Set(recipe.transform_libraries) : null,
  )

  let stale = $derived(
    wf?.planned &&
      (JSON.stringify(recipe.targets) !== JSON.stringify(normalize(wf.request.target_types)) ||
        recipe.sample_type !== (wf.request.sample_type ?? '') ||
        JSON.stringify(recipe.transform_libraries) !==
          JSON.stringify(wf.request.transform_libraries ?? [])),
  )

  function requestBody() {
    return {
      sample_type: recipe.sample_type,
      target_types: recipe.targets,
      transform_libraries: recipe.transform_libraries,
    }
  }

  // the recipe is persisted as it is built, so a reload does not lose an output
  // that was added but never generated
  async function persist() {
    await attempt(async () => {
      await api.patch(`/workflows/${name}`, requestBody())
      await loadWorkflows()
    })
  }

  async function addEntry(body) {
    if (body.role === 'output') {
      recipe.targets = [...recipe.targets, { type: body.type, parents: body.parents }]
      await persist()
      return true
    }
    const payload = { dtype: body.dtype, parents: body.parents }
    if (body.path !== undefined) payload.path = body.path
    else Object.assign(payload, { name: body.name, value: body.value })
    const ok = await attempt(() => api.post(`/workflows/${name}/inputs/items`, payload))
    if (ok) await loadInputs()
    return !!ok
  }

  async function removeInput(item) {
    await attempt(async () => {
      await api.del(
        `/workflows/${name}/inputs/items?path=${encodeURIComponent(item.path)}`,
      )
      await loadInputs()
    })
  }

  // Targets are addressed by position, so removing one has to renumber the
  // links into it. A link *to* the removed target cannot be renumbered, so it
  // is dropped -- and said out loud, because it silently changes the plan.
  async function removeTarget(i) {
    let dropped = false
    recipe.targets = recipe.targets
      .filter((_, j) => j !== i)
      .map((t) => ({
        ...t,
        parents: (t.parents ?? [])
          .filter((p) => {
            if (p !== i) return true
            dropped = true
            return false
          })
          .map((p) => (p > i ? p - 1 : p)),
      }))
    await persist()
    // after the write, not before: persist clears the notice on its way in
    if (dropped) notify('an output was descended from that one; the link was dropped', 'refused')
  }

  // The hints sit below the builder, so filling the type field from one would
  // otherwise change something the reader cannot see.
  function useType(type) {
    focus = type
    document.getElementById('msm-builder')?.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }

  function setSample(type) {
    recipe.sample_type = type
    focus = type
    persist()
  }

  function toggleLibrary(path) {
    const all = (index?.libraries ?? []).map((l) => l.path)
    const current = recipe.transform_libraries.length ? recipe.transform_libraries : all
    const next = current.includes(path)
      ? current.filter((p) => p !== path)
      : [...current, path]
    if (!next.length) {
      notify('at least one transform library has to stay enabled', 'refused')
      return
    }
    // all of them is stored as none of them, so a library added to the standard
    // library later is picked up rather than silently excluded
    recipe.transform_libraries = next.length === all.length ? [] : next
    persist()
  }

  async function generate() {
    const job = await attempt(() => api.post(`/workflows/${name}/generate`, requestBody()))
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

  // The name was made up at create time -- there is no form before this page to
  // have chosen it on -- so it is editable here, for as long as nothing is keyed
  // to it. The server decides that; this only stops offering once it has said so.
  let renaming = $state(false)
  let draft = $state('')

  let renameable = $derived(!wf?.planned && !wf?.runs?.length && !wf?.archived_at)

  function startRename() {
    draft = wf.name
    renaming = true
  }

  async function commitRename() {
    // Enter and blur both land here, and Enter causes the blur: closing the
    // field unmounts the input. Without this the rename is sent twice, and the
    // second one races the first -- it reads the workflow under the old name,
    // then tries to move it onto the directory the first one just created.
    if (!renaming) return
    renaming = false
    const next = draft.trim()
    if (!next || next === wf.name) return
    const out = await attempt(async () => {
      const body = await api.post(`/workflows/${name}/rename`, { name: next })
      await loadWorkflows()
      return body
    })
    // the name is the route: reselect so the pane reloads against the new one
    if (out) select('workflows', out.name)
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
  <div class="pane">
    <div class="col main" style="gap:14px">
      <div class="spread">
        <div class="row grow">
          {#if renaming}
            <input
              class="rename"
              bind:value={draft}
              autofocus
              spellcheck="false"
              onblur={commitRename}
              onkeydown={(e) => {
                if (e.key === 'Enter') commitRename()
                if (e.key === 'Escape') renaming = false
              }}
            />
            <span class="small muted">enter to rename</span>
          {:else if renameable}
            <!-- the heading is the field: nothing else on the page needs a name
                 box, and one that only appears once you go for it keeps the
                 emphasis on the recipe below -->
            <button class="asname" onclick={startRename} title="rename this workflow">
              <h1>{wf.name}</h1>
            </button>
          {:else}
            <h1 title={wf.planned
              ? 'generated workflows keep their name — the plan is keyed to it; fork to rename'
              : 'this workflow has runs and keeps its name'}>{wf.name}</h1>
          {/if}
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

      <div class="card">
        <RecipeCard
          {items}
          targets={recipe.targets}
          sampleType={recipe.sample_type}
          {focus}
          onfocus={(t) => (focus = t)}
          onsample={setSample}
          onremoveInput={removeInput}
          onremoveTarget={removeTarget}
        />
      </div>

      <div class="card" id="msm-builder">
        <EntryBuilder
          {types}
          {index}
          {enabled}
          libraries={index?.libraries ?? []}
          {items}
          targets={recipe.targets}
          prefill={focus}
          onadd={addEntry}
          onfocus={(t) => (focus = t)}
          ontogglelibrary={toggleLibrary}
        />
      </div>

      <div class="row wrap">
        <button
          class="primary"
          onclick={generate}
          disabled={recipe.targets.length === 0 || !recipe.sample_type}
        >{wf.planned ? 'regenerate' : 'generate'}</button>
        {#if !recipe.sample_type}
          <span class="small muted">mark which input each run starts from</span>
        {:else if recipe.targets.length === 0}
          <span class="small muted">add at least one output</span>
        {:else if stale}
          <span class="tag warn">recipe changed — the result below is from the old one</span>
        {/if}
      </div>

      <div class="card col" style="gap:10px">
        <JobLog
          {jobId}
          onend={async () => {
            await load()
            await loadWorkflows()
          }}
        />

        {#if !wf.planned}
          <h3>result</h3>
          <p class="small muted">
            Nothing generated yet. Register what you have, say what you want, then
            generate — the planner works out the steps between them.
          </p>
        {:else if wf.success}
          <div class="spread">
            <h3>result</h3>
            <div class="row">
              <span class="tag ok">{wf.step_count} step(s)</span>
              <span class="tag mono">{wf.task_key}</span>
            </div>
          </div>

          <div class="dag">
            <img src={`/api/workflows/${wf.name}/dag`} alt="workflow diagram" />
          </div>

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
          <p class="small muted">
            generated {wf.generated_at}{#if wf.result.stdlib_commit}
              · library <span class="mono">{wf.result.stdlib_commit.slice(0, 12)}</span>{/if}
          </p>
        {:else}
          <HintsPanel result={wf.result} onadd={useType} />
        {/if}
      </div>

      {#if wf.success}
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

    <SidePanel title={focus ?? 'types'} subtitle={focus ? null : 'nothing selected'}>
      <TypeInspector type={focus} {index} {enabled} onpick={(t) => (focus = t)} />
    </SidePanel>
  </div>
{/if}

<style>
  .pane { display: flex; gap: 14px; align-items: flex-start; }
  /* a heading that happens to be clickable, not a button that happens to hold
     one: no chrome until the pointer is on it */
  .asname {
    background: none;
    border: 1px solid transparent;
    padding: 1px 5px;
    margin-left: -5px;
    color: inherit;
    text-align: left;
  }
  .asname:hover { border-color: var(--line); background: var(--panel-2); }
  .rename {
    font: inherit;
    font-size: 18px;
    font-weight: 600;
    width: auto;
    max-width: 420px;
  }
  .main { flex: 1; min-width: 0; }
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
