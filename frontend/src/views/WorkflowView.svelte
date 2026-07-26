<script>
  import { api } from '../lib/api.js'
  import { app, attempt, loadRuns, loadWorkflows, notify, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'
  import JobLog from '../components/JobLog.svelte'
  import MiniGraph from '../components/MiniGraph.svelte'
  import SidePanel from '../components/SidePanel.svelte'
  import HintsPanel from './HintsPanel.svelte'
  import LibraryList from './LibraryList.svelte'
  import RecipeCard from './RecipeCard.svelte'
  import TypeInspector from './TypeInspector.svelte'
  import { isPlumbing, libraryGraph, transformGraph, typeGraph } from '../lib/graphs.js'

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

  // What the upper half of the panel is drawing. A type in focus draws its own
  // neighbourhood; picking a tool or a library takes it over until the focus
  // moves again, so clicking through the list below never fights the picture.
  let drawing = $state(null)

  // Which row of the recipe a type picked in the panel should land in. The
  // builder used to be one form with one type field, so there was nowhere else
  // for a picked type to go; now every row has a field, and the answer is the
  // one you were last in. Only rows that *can* be retyped are ever set here.
  let editing = $state(null)

  function pickType(type) {
    focus = type
    drawing = null
    applyToEditing(type)
  }

  function applyToEditing(type) {
    if (!editing) return
    if (editing.kind === 'draft') {
      if (!recipe.drafts.some((d) => d.id === editing.id)) return (editing = null)
      patchDraft(editing.id, { dtype: type })
      persist().then(commitDrafts)
    } else if (editing.kind === 'target') {
      if (editing.id >= recipe.targets.length) return (editing = null)
      patchTarget(editing.id, { type })
      persist()
    }
  }

  // a row of the recipe naming its own type: it moves the panel, and nothing else
  function showType(type) {
    focus = type
    drawing = null
  }

  function pickTransform(i) {
    drawing = { kind: 'transform', i }
  }

  function pickLibrary(path) {
    drawing = drawing?.kind === 'library' && drawing.path === path
      ? null
      : { kind: 'library', path }
  }

  // the editable recipe, kept separate from the frozen result below it
  let recipe = $state({ sample_type: '', targets: [], transform_libraries: [], drafts: [] })
  let loadedFor = $state(null)

  // Targets were a list of bare type names before they could carry lineage.
  // Both spellings still arrive from disk, so both are read here.
  function normalize(list) {
    return (list ?? []).map((t) =>
      typeof t === 'string'
        ? { type: t, parents: [] }
        : { type: t.type ?? '', parents: [...(t.parents ?? [])] },
    )
  }

  // Half-built inputs. A registered input lives in the library, which needs a
  // type *and* a path before it will take one -- so a row that is not complete
  // yet has to live somewhere else, and the request is where the outputs have
  // always waited. Read defensively: the key is new, and a workflow written
  // before it simply has none.
  function normalizeDrafts(list) {
    return (list ?? [])
      .filter((d) => d && typeof d === 'object')
      .map((d) => ({
        id: String(d.id ?? nextDraftId()),
        mode: d.mode === 'value' ? 'value' : 'file',
        path: d.path ?? '',
        name: d.name ?? '',
        value: d.value ?? '',
        dtype: d.dtype ?? '',
        parents: [...(d.parents ?? [])],
      }))
  }

  let draftSeq = 0
  const nextDraftId = () => `d${(draftSeq++).toString(36)}${Math.random().toString(36).slice(2, 7)}`

  async function load() {
    wf = await api.get(`/workflows/${name}`)
    if (loadedFor !== name) {
      loadedFor = name
      recipe = {
        sample_type: wf.request.sample_type ?? '',
        targets: normalize(wf.request.target_types),
        transform_libraries: wf.request.transform_libraries ?? [],
        drafts: normalizeDrafts(wf.request.input_drafts),
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
    drawing = null
    editing = null
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

  // laid out in the browser from the index it already holds, so a library toggle
  // redraws with no round trip
  let graph = $derived.by(() => {
    if (!index) return null
    if (drawing?.kind === 'transform') return transformGraph(index, drawing.i)
    if (drawing?.kind === 'library') return libraryGraph(index, drawing.path)
    return focus ? typeGraph(index, focus, enabled) : null
  })

  let graphFocus = $derived(
    drawing?.kind === 'transform' ? `x:${drawing.i}` : focus ? `t:${focus}` : null,
  )

  let drawingLabel = $derived.by(() => {
    if (drawing?.kind === 'transform') return index?.transforms?.[drawing.i]?.name ?? 'transform'
    if (drawing?.kind === 'library')
      return index?.libraries?.find((l) => l.path === drawing.path)?.name ?? 'library'
    return focus
  })

  let stale = $derived(
    wf?.planned &&
      (JSON.stringify(recipe.targets) !== JSON.stringify(normalize(wf.request.target_types)) ||
        recipe.sample_type !== (wf.request.sample_type ?? '') ||
        JSON.stringify(recipe.transform_libraries) !==
          JSON.stringify(wf.request.transform_libraries ?? [])),
  )

  // -- what the rows can be typed as ------------------------------------------
  //
  // The standard library's type files are the vocabulary, but a transform may
  // name a type that no type file exports; both are offerable. This used to
  // live in the builder card, which was the only place a type could be chosen.

  let allTypes = $derived(
    [
      ...new Set([
        ...types.filter((t) => t.full_name).map((t) => t.full_name),
        ...Object.keys(index?.by_type ?? {}),
      ]),
    ].sort(),
  )
  let typeNames = $derived(new Set(allTypes))

  // entries are {i, as, match} -- a transform is on this list because its
  // *properties* fit, which is not the same as having named this type
  function matching(type, side) {
    const entries = index?.by_type?.[type]?.[side] ?? []
    if (!enabled) return entries
    return entries.filter((e) => enabled.has(index.transforms[e.i]?.library))
  }

  // How many of those are there because the type system says so rather than
  // because a name lined up. Worth saying: it is the difference between "one
  // transform mentions this" and "one transform will accept this".
  function counts(type) {
    const produced = matching(type, 'produced_by')
    const consumed = matching(type, 'consumed_by')
    return {
      known: typeNames.has(type),
      produced: produced.length,
      consumed: consumed.length,
      producedVia: produced.filter((e) => e.match !== 'exact').length,
      consumedVia: consumed.filter((e) => e.match !== 'exact').length,
    }
  }

  function requestBody() {
    return {
      sample_type: recipe.sample_type,
      target_types: recipe.targets,
      transform_libraries: recipe.transform_libraries,
      input_drafts: recipe.drafts,
    }
  }

  // The recipe is persisted as it is built, so a reload does not lose an output
  // that was added but never generated.
  //
  // Serialised, because now it is written on every edit rather than once per
  // added row: leaving a field and toggling a lineage in the same moment sends
  // two writes of one file, and they would otherwise land in whichever order
  // the server finished them in.
  let writing = Promise.resolve()

  function persist() {
    writing = writing.then(() =>
      attempt(async () => {
        await api.patch(`/workflows/${name}`, requestBody())
        await loadWorkflows()
      }),
    )
    return writing
  }

  // -- drafts, and the moment they stop being drafts ---------------------------

  function addRow(kind) {
    if (kind === 'output') {
      recipe.targets = [...recipe.targets, { type: '', parents: [] }]
      persist()
      return
    }
    addDraft(kind)
  }

  function addDraft(mode, extra = {}) {
    const d = {
      id: nextDraftId(),
      mode,
      path: '',
      name: '',
      value: '',
      dtype: '',
      parents: [],
      ...extra,
    }
    recipe.drafts = [...recipe.drafts, d]
    persist()
    return d
  }

  // Typing is local; it is written back when the field is left. Persisting per
  // keystroke would be a round trip per character, and a draft is only ever
  // read back on a reload.
  function patchDraft(id, patch) {
    recipe.drafts = recipe.drafts.map((d) => (d.id === id ? { ...d, ...patch } : d))
  }

  async function removeDraft(id) {
    const key = `#${id}`
    recipe.drafts = recipe.drafts
      .filter((d) => d.id !== id)
      .map((d) => ({ ...d, parents: d.parents.filter((p) => p !== key) }))
    if (editing?.kind === 'draft' && editing.id === id) editing = null
    await persist()
  }

  function patchTarget(i, patch) {
    recipe.targets = recipe.targets.map((t, j) => (j === i ? { ...t, ...patch } : t))
  }

  // Every edit on a row is local until the field is left; this is leaving it.
  // A type field is a combobox rather than a plain input, so "left" is focus
  // moving out of the whole control, not out of the box inside it.
  async function commitRow() {
    await persist()
    await commitDrafts()
  }

  // A draft becomes a library item the moment it can: it has a type, it has an
  // identity, and everything it descends from is itself registered. That last
  // condition is what makes a chain of them commit in cascade -- filling in the
  // top row registers it, which unblocks the one below, and so on down.
  let committing = false

  function draftReady(d, registered) {
    const identity = d.mode === 'value' ? d.name.trim() : d.path.trim()
    return !!d.dtype.trim() && !!identity && d.parents.every((p) => registered.has(p))
  }

  async function commitDrafts() {
    if (committing) return
    committing = true
    try {
      // the list only ever shrinks in here, so this terminates even if a commit
      // lands somewhere the reload does not show it
      for (;;) {
        const registered = new Set(items.map((it) => it.path))
        const d = recipe.drafts.find((x) => draftReady(x, registered))
        if (!d) break
        const payload = { dtype: d.dtype.trim(), parents: d.parents }
        if (d.mode === 'value') Object.assign(payload, { name: d.name.trim(), value: d.value })
        else payload.path = d.path.trim()
        const out = await attempt(() => api.post(`/workflows/${name}/inputs/items`, payload))
        // refused: the notice says why, and the row stays a draft so it can be
        // corrected rather than lost
        if (!out) break
        const key = `#${d.id}`
        recipe.drafts = recipe.drafts
          .filter((x) => x.id !== d.id)
          .map((x) => ({ ...x, parents: x.parents.map((p) => (p === key ? out.path : p)) }))
        if (editing?.kind === 'draft' && editing.id === d.id) editing = null
        await loadInputs()
        await persist()
      }
    } finally {
      committing = false
    }
  }

  // Lineage, whichever half of the recipe the row is in. Outputs are positions
  // in the request; registered inputs are a write to the library, which is the
  // one place a parent link is stored rather than described.
  async function setParents(row, keys) {
    if (row.kind === 'target') {
      patchTarget(row.id, {
        parents: keys.map((k) => Number(k.slice(1))).sort((a, b) => a - b),
      })
      await persist()
      return
    }
    if (row.kind === 'draft') {
      patchDraft(row.id, { parents: keys })
      await persist()
      await commitDrafts()
      return
    }
    await attempt(async () => {
      await api.put(`/workflows/${name}/inputs/items/parents`, { path: row.id, parents: keys })
      await loadInputs()
    })
    // a row that descended from this one may now be registerable, or not
    await commitDrafts()
  }

  // Removing an item leaves anything that descended from it pointing at a path
  // the library no longer holds -- the library does not chase those down, and
  // the row would sit there claiming a lineage that is gone. Both halves are
  // cleared here, and both are said out loud: it changes the plan silently
  // otherwise. Same reasoning as removeTarget below.
  async function removeInput(item) {
    const orphaned = items.filter((it) => (it.parents ?? []).some((p) => p.path === item.path))
    const ok = await attempt(async () => {
      await api.del(`/workflows/${name}/inputs/items?path=${encodeURIComponent(item.path)}`)
      for (const child of orphaned) {
        await api.put(`/workflows/${name}/inputs/items/parents`, {
          path: child.path,
          parents: (child.parents ?? [])
            .map((p) => p.path)
            .filter((p) => p !== item.path && p !== child.path),
        })
      }
      await loadInputs()
      return true
    })
    const pending = recipe.drafts.filter((d) => d.parents.includes(item.path))
    if (ok && pending.length) {
      recipe.drafts = recipe.drafts.map((d) => ({
        ...d,
        parents: d.parents.filter((p) => p !== item.path),
      }))
      await persist()
    }
    const cut = orphaned.length + (ok ? pending.length : 0)
    if (ok && cut) {
      notify(`${cut} row(s) descended from that one; the link was dropped`, 'refused')
    }
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

  // A near miss in the hints is a type you probably meant to register, so this
  // makes the row for it rather than filling a field somewhere else and leaving
  // you to find it -- the hints sit well below the recipe.
  function useType(type) {
    editing = null
    showType(type)
    addDraft('file', { dtype: type })
    document.getElementById('msm-recipe')?.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }

  // Stamp a transform's input shape into the recipe: one row per requirement it
  // declares, typed as declared and wired with the lineage declared between
  // them. What lands is half-filled rows in the right relationship — the paths
  // are still yours to fill in, and each row registers itself as it completes.
  function applyTransform(i) {
    const tr = index?.transforms?.[i]
    if (!tr) return

    // "we already have one of those" is a question about properties, not names,
    // and the index has already answered it: a registered type appears under a
    // transform's `consumed_by` exactly when the solver would accept it there.
    function satisfiedBy(slot) {
      for (const it of items) {
        const entries = index.by_type?.[it.type_name]?.consumed_by ?? []
        if (entries.some((e) => e.i === i && e.as === slot.as)) return it.path
      }
      // a row already on its way to being that type counts too, or applying the
      // same transform twice would build a second copy of everything
      const d = recipe.drafts.find((x) => x.dtype === slot.as)
      return d ? `#${d.id}` : null
    }

    const stands = new Map() // slot position -> what fills it in the recipe
    const made = []
    let skipped = 0
    ;(tr.requires ?? []).forEach((slot, k) => {
      if (!slot.as || isPlumbing(slot.as)) return
      const have = satisfiedBy(slot)
      if (have) {
        stands.set(k, have)
        skipped += 1
        return
      }
      // a parent is always declared before the slot that names it -- the model
      // asserts it -- so everything this descends from already has a key
      const d = {
        id: nextDraftId(),
        mode: 'file',
        path: '',
        name: '',
        value: '',
        dtype: slot.as,
        parents: (slot.parents ?? []).map((p) => stands.get(p)).filter(Boolean),
      }
      stands.set(k, `#${d.id}`)
      made.push(d)
    })

    if (made.length) {
      recipe.drafts = [...recipe.drafts, ...made]
      persist()
    }

    // the rows themselves are the report when it is a clean addition; anything
    // that was *not* done has to be said
    const said = []
    if (!made.length) said.push(`nothing to add — every input ${tr.name} needs is already here`)
    else if (skipped) said.push(`${made.length} row(s) added · ${skipped} already satisfied`)
    if (enabled && !enabled.has(tr.library)) {
      said.push(`${tr.library_name} is not enabled, so the planner cannot reach ${tr.name}`)
    }
    if (said.length) notify(said.join(' — '), 'refused')
  }

  function setSample(type) {
    recipe.sample_type = type
    showType(type)
    persist()
  }

  // An output row exists before it has a type -- that is what "add an output"
  // makes -- so the plan has to wait for it, and say which one it is waiting on.
  let blankTarget = $derived(recipe.targets.some((t) => !t.type?.trim()))
  const lineage = (t) => JSON.stringify([...(t.parents ?? [])].sort())
  let dupTarget = $derived(
    recipe.targets.some((t, i) =>
      recipe.targets.some(
        (o, j) => j !== i && !!t.type && o.type === t.type && lineage(o) === lineage(t),
      ),
    ),
  )

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
  <p class="muted loading">loading…</p>
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
              ? 'generated workflows keep their name — the plan is keyed to it; copy it from the list to get one under a new name'
              : 'this workflow has runs and keeps its name'}>{wf.name}</h1>
          {/if}
          {#if wf.archived_at}<span class="tag warn">archived</span>{/if}
          {#if wf.forked_from}
            <span class="tag">forked from {wf.forked_from}</span>
          {/if}
        </div>
        <!-- copying one lives on its row in the rail, next to the delete: it is
             a change to the list, and it is wanted for workflows other than the
             one that happens to be open -->
        {#if wf.archived_at}
          <div class="row"><button onclick={unarchive}>restore</button></div>
        {/if}
      </div>

      <div class="card" id="msm-recipe">
        <RecipeCard
          {items}
          drafts={recipe.drafts}
          targets={recipe.targets}
          sampleType={recipe.sample_type}
          {focus}
          typeOptions={allTypes}
          {counts}
          onfocus={showType}
          onsample={setSample}
          onremoveInput={removeInput}
          onremoveDraft={removeDraft}
          onremoveTarget={removeTarget}
          ondraft={patchDraft}
          ontarget={patchTarget}
          onparents={setParents}
          oncommit={commitRow}
          ontypefocus={(row) => (editing = { kind: row.kind, id: row.id })}
          onadd={addRow}
        />
      </div>

      <div class="row wrap">
        <button
          class="primary"
          onclick={generate}
          disabled={recipe.targets.length === 0 || !recipe.sample_type || blankTarget || dupTarget}
        >{wf.planned ? 'regenerate' : 'generate'}</button>
        {#if !recipe.sample_type}
          <span class="small muted">mark which input each run starts from</span>
        {:else if recipe.targets.length === 0}
          <span class="small muted">add at least one output</span>
        {:else if blankTarget}
          <span class="small muted">an output row has no type yet</span>
        {:else if dupTarget}
          <span class="small muted">two outputs are the same type with the same lineage</span>
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

    <SidePanel
      title={drawingLabel ?? 'types'}
      subtitle={graph?.caption ?? (focus ? null : 'nothing selected')}
    >
      {#snippet action()}
        <!-- the tool being drawn is the one you are looking at, so its apply
             belongs on the picture as well as on its card in the list below -->
        {#if drawing?.kind === 'transform'}
          <button class="small" onclick={() => applyTransform(drawing.i)} title="add a row to the recipe for each input this needs">
            apply
          </button>
        {/if}
      {/snippet}

      {#snippet top()}
        <LibraryList
          libraries={index?.libraries ?? []}
          {enabled}
          viewing={drawing?.kind === 'library' ? drawing.path : null}
          ontoggle={toggleLibrary}
          onview={pickLibrary}
        />
        <MiniGraph
          {graph}
          focus={graphFocus}
          empty="Pick a type, a transform, or a library’s eye — this draws what it connects to."
          onpicktype={pickType}
          onpicktransform={pickTransform}
        />
      {/snippet}

      <TypeInspector
        type={focus}
        {index}
        {enabled}
        selected={drawing?.kind === 'transform' ? drawing.i : null}
        onpick={pickType}
        onselect={pickTransform}
        onapply={applyTransform}
      />
    </SidePanel>
  </div>
{/if}

<style>
  /* The column scrolls, not the page: that puts its scrollbar at its own right
     edge, with the panel outside it rather than behind it. `main` is in flush
     mode for this view (App.svelte) so this row owns the height. */
  .pane { display: flex; flex: 1; min-width: 0; height: 100%; align-items: stretch; }
  .main { flex: 1; min-width: 0; overflow-y: auto; padding: 18px; }
  .loading { padding: 18px; }
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
