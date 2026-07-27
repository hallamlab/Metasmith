<script>
  import { api } from '../lib/api.svelte.js'
  import { app, attempt, loadRuns, loadWorkflows, notify, select } from '../lib/state.svelte.js'
  import Ago from '../components/Ago.svelte'
  import EditableName from '../components/EditableName.svelte'
  import Field from '../components/Field.svelte'
  import JobLog from '../components/JobLog.svelte'
  import MiniGraph from '../components/MiniGraph.svelte'
  import SaveChip from '../components/SaveChip.svelte'
  import SidePanel from '../components/SidePanel.svelte'
  import HintsPanel from './HintsPanel.svelte'
  import LibraryList from './LibraryList.svelte'
  import RecipeCard from './RecipeCard.svelte'
  import TypeInspector from './TypeInspector.svelte'
  import ParamRows from '../components/ParamRows.svelte'
  import { isPlumbing, libraryGraph, transformGraph, typeGraph } from '../lib/graphs.js'
  import { runSuffix } from '../lib/runname.js'
  import { paramRows, sameParams, toParams } from '../lib/params.js'

  let { name } = $props()

  let wf = $state(null)
  let types = $state([])
  let index = $state(null)
  let items = $state([])
  let jobId = $state(null)
  let launching = $state(false)
  let agentChoice = $state('')
  let presetChoice = $state('')
  // This run's params, pre-filled from the chosen agent so what will be sent is
  // visible rather than implied, and `seededParams` is what was put there -- how
  // the page tells "still the agent's defaults" from "someone typed over them".
  let runParams = $state([])
  let seededParams = $state({})
  // Per-step resources, keyed by step position, which is the only form that
  // produces a selector for one step rather than for every step of a transform.
  // Boxes are strings; the server reads and checks the numbers.
  let overrides = $state({})
  let focus = $state(null)

  // What the upper half of the panel is drawing. A type in focus draws its own
  // neighbourhood; picking a tool or a library takes it over until the focus
  // moves again, so clicking through the list below never fights the picture.
  let drawing = $state(null)

  // Which row of the recipe a type picked in the panel should land in. The
  // builder used to be one form with one type field, so there was nowhere else
  // for a picked type to go; now every row has a field, and the answer is the
  // one you were last in. Only rows the *request* holds are ever set here: a
  // registered row's field commits a library write, and a pick landing in it
  // long after the field closed would be a retype nobody asked for.
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
  let recipe = $state({ targets: [], transform_libraries: [], drafts: [] })
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
        targets: normalize(wf.request.target_types),
        transform_libraries: wf.request.transform_libraries ?? [],
        drafts: normalizeDrafts(wf.request.input_drafts),
      }
    }
  }

  async function loadInputs() {
    items = (await api.get(`/workflows/${name}/inputs`)).items ?? []
  }

  // Four reads, and only one of them is the page: the workflow itself decides
  // whether anything renders, while the type vocabulary and the index behind
  // the panel are what fill it in. Run together rather than in a chain, so the
  // recipe is up as soon as the workflow lands instead of after the slowest of
  // the four -- a newly-created workflow is empty, and waiting on the standard
  // library to describe itself before drawing an empty recipe is all lag.
  $effect(() => {
    const n = name
    wf = null
    jobId = null
    focus = null
    drawing = null
    editing = null
    loadedFor = null
    attempt(async () => {
      await Promise.all([
        load(),
        loadInputs(),
        api.get('/project/types').then((v) => (types = v)),
        api.get('/project/type-index').then((v) => (index = v)),
      ])
      void n
    })
  })

  // The runs card is a live list too, for the same reason the rail is: a run
  // advances on the agent and lands on disk, and a page that only reads it once
  // shows `staging` until someone clicks something. Same cadence as the rail
  // and the run detail; stops the moment nothing listed is live.
  const RUN_POLL_MS = 8000
  $effect(() => {
    if (!wf?.runs?.some((r) => r.live)) return
    const t = setInterval(() => load().catch(() => {}), RUN_POLL_MS)
    return () => clearInterval(t)
  })

  // The chosen agent, off the list already loaded. Nothing is fetched for this:
  // `/agents` carries both the presets and the one the agent declares, and an
  // extra round trip on every change of a dropdown bought nothing.
  let chosenAgent = $derived(app.agents.find((a) => a.name === agentChoice) ?? null)
  let presets = $derived(Object.keys(chosenAgent?.config_presets ?? {}))
  // what leaving the box alone will actually use, so the blank option can say it
  let agentPreset = $derived(chosenAgent?.default_preset ?? 'local')

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

  // `sample_type` is written out as null on purpose rather than left off: the
  // server merges a request over the stored one, so omitting the key would keep
  // whatever a previous version of this page (or the CLI) put there. The page
  // does not offer sampling -- everything registered is one sample -- and this
  // is what makes that true of a workflow that once had a type marked.
  function requestBody() {
    return {
      sample_type: null,
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

  // What the chip beside the name reads off. Two things count as unsaved and
  // both have to: a write still in flight, and an edit made in a field that has
  // not been left yet -- typing is local until blur, so a chip watching only the
  // network would say "saved" over a box holding something the server has never
  // seen.
  let pending = $state(0)
  let touched = $state(false)
  let dirty = $derived(pending > 0 || touched)

  const touch = () => (touched = true)

  function persist() {
    pending += 1
    writing = writing.then(async () => {
      // read the body first, then clear: everything typed up to this point is
      // in what goes out, and anything after it belongs to the next write
      const body = requestBody()
      touched = false
      const ok = await attempt(async () => {
        // no `name` in the body, so this only ever saves the recipe
        await api.put(`/workflows/${name}`, body)
        await loadWorkflows()
        return true
      })
      // a refused write leaves the edit where it was: unsaved, and said so
      if (!ok) touched = true
      pending -= 1
    })
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
    touch()
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
    touch()
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

  // Correcting a registered row, in place. Both are one route and a reload: the
  // row keeps its position, its lineage, and everything that descends from it --
  // which delete-and-add-again could not, and which is why these exist.
  //
  // Neither touches the user's file. A path in the library is a pointer, so
  // re-pointing one is a manifest edit; only a value's own file (which the
  // library wrote) is ever moved, and that one is the library's to move.
  async function retypeInput(item, dtype) {
    await attempt(async () => {
      await api.put(`/workflows/${name}/inputs/items/type`, { path: item.path, dtype })
      await loadInputs()
    })
    // a draft waiting on a parent of a particular type may now be registerable
    await commitDrafts()
  }

  async function repointInput(item, path) {
    const out = await attempt(async () => {
      const body = await api.put(`/workflows/${name}/inputs/items/path`, {
        path: item.path, new_path: path,
      })
      // before anything reads the row keys again: a registered row's identity
      // *is* its path, here and in every other row's lineage options
      await loadInputs()
      return body
    })
    if (!out) return
    // the library follows the link for its own items; a draft naming the old
    // path is ours to follow, or its lineage silently stops resolving
    const pending = recipe.drafts.some((d) => d.parents.includes(item.path))
    if (pending) {
      recipe.drafts = recipe.drafts.map((d) => ({
        ...d,
        parents: d.parents.map((p) => (p === item.path ? out.new : p)),
      }))
      await persist()
    }
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
  async function applyTransform(i) {
    const tr = index?.transforms?.[i]
    if (!tr) return

    // "we already have one of those" is a question about properties, not names,
    // and the index has already answered it: a registered type appears under a
    // transform's `consumed_by` exactly when the solver would accept it there.
    //
    // A filler is *consumed*: what is credited to one requirement is not offered
    // to the next, or one file would satisfy three slots. And a draft only
    // stands in once it has an identity -- a blank row this button made itself is
    // a row you still have to fill in, not an input you have. It still occupies
    // the requirement, so pressing apply again does not stamp a second copy;
    // it is reported as blank rather than counted as present.
    const identity = (d) => (d.mode === 'value' ? d.name : d.path).trim()
    const used = new Set()

    function claim(slot) {
      for (const it of items) {
        if (used.has(it.path)) continue
        const entries = index.by_type?.[it.type_name]?.consumed_by ?? []
        if (entries.some((e) => e.i === i && e.as === slot.as)) {
          return { key: it.path, by: it.path }
        }
      }
      for (const d of recipe.drafts) {
        const key = `#${d.id}`
        if (used.has(key) || d.dtype !== slot.as) continue
        const id = identity(d)
        return { key, by: id, blank: !id }
      }
      return null
    }

    const stands = new Map() // slot position -> what fills it in the recipe
    const made = []
    const filled = [] // requirements something in the recipe already answers
    const blank = [] // ...and ones a row is here for but has nothing in it yet
    ;(tr.requires ?? []).forEach((slot, k) => {
      if (!slot.as || isPlumbing(slot.as)) return
      const have = claim(slot)
      if (have) {
        used.add(have.key)
        stands.set(k, have.key)
        ;(have.blank ? blank : filled).push({ as: slot.as, by: have.by })
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
      used.add(`#${d.id}`)
      made.push(d)
    })

    if (made.length) {
      recipe.drafts = [...recipe.drafts, ...made]
      // awaited, not fired: persist clears the notice on its way in, so a report
      // written before it lands is a report nobody ever sees
      await persist()
    }

    // A report rather than an assertion. The rows it added speak for themselves;
    // what it *skipped* has to name what stands in for it, because "every input
    // is already here" is a claim you cannot check from where you are reading it
    // — and it was wrong once, off a row that was counted while still blank.
    const said = []
    if (made.length) said.push(`${made.length} row(s) added`)
    for (const f of filled) said.push(`${f.as} ← ${f.by}`)
    for (const b of blank) said.push(`${b.as} — a row is here, still blank`)
    if (!made.length && !filled.length && !blank.length) {
      said.push(`nothing to add — ${tr.name} takes nothing you would register`)
    }
    if (enabled && !enabled.has(tr.library)) {
      said.push(`${tr.library_name} is not enabled, so the planner cannot reach ${tr.name}`)
    }
    if (said.length) notify(said.join(' · '), 'refused')
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

  async function solve() {
    const job = await attempt(() => api.post(`/workflows/${name}/generate`, requestBody()))
    if (job) jobId = job.id
  }

  // Seeding is a convenience, so it never costs work: rows that are still
  // exactly what was seeded are replaced, and rows someone has typed over keep
  // what they say and only gain the keys the new agent names that they do not.
  function seedFromAgent(nextName) {
    const defaults = (app.agents.find((a) => a.name === nextName) ?? null)?.default_params ?? {}
    if (sameParams(toParams(runParams), seededParams)) {
      runParams = paramRows(defaults)
    } else {
      const have = new Set(runParams.map((r) => (r.key ?? '').trim()))
      let id = runParams.reduce((m, r) => Math.max(m, r.id ?? 0), 0)
      runParams = [
        ...runParams,
        ...paramRows(defaults)
          .filter((r) => !have.has(r.key))
          .map((r) => ({ ...r, id: ++id })),
      ]
    }
    seededParams = defaults
  }

  const OVERRIDE_FIELDS = ['cpus', 'memory_gb', 'duration_h']

  // Only the boxes with something in them, and only the steps with such a box.
  // An empty string sent as a value would be a resource directive of nothing.
  function overridePayload() {
    const out = {}
    for (const [step, spec] of Object.entries(overrides)) {
      const kept = {}
      for (const f of OVERRIDE_FIELDS) {
        const v = (spec?.[f] ?? '').toString().trim()
        if (v) kept[f] = v
      }
      if (Object.keys(kept).length) out[step] = kept
    }
    return Object.keys(out).length ? out : null
  }

  async function launch() {
    launching = true
    const params = toParams(runParams)
    const out = await attempt(() =>
      api.post('/runs', {
        workflow: name,
        agent: agentChoice,
        preset: presetChoice || null,
        params: Object.keys(params).length ? params : null,
        resource_overrides: overridePayload(),
      }),
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
  let renameable = $derived(!wf?.planned && !wf?.runs?.length && !wf?.archived_at)

  async function commitRename(next) {
    const out = await attempt(async () => {
      // the same PUT the recipe saves through: a workflow's name is a field of
      // it, and an id in the body that differs from the url is a rename
      const body = await api.put(`/workflows/${name}`, { name: next })
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
          <EditableName
            value={wf.name}
            editable={renameable}
            title="rename this workflow"
            lockedTitle={wf.planned
              ? 'solved workflows keep their name — the plan is keyed to it; copy it from the list to get one under a new name'
              : 'this workflow has runs and keeps its name'}
            oncommit={commitRename}
          />
          <SaveChip {dirty} />
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
          typeOptions={allTypes}
          {counts}
          onfocus={showType}
          onremoveInput={removeInput}
          onremoveDraft={removeDraft}
          onremoveTarget={removeTarget}
          ondraft={patchDraft}
          ontarget={patchTarget}
          onparents={setParents}
          onretype={retypeInput}
          onrepoint={repointInput}
          oncommit={commitRow}
          ontypefocus={(row) =>
            (editing = row.kind === 'item' ? null : { kind: row.kind, id: row.id })}
          onadd={addRow}
        />
      </div>

      <div class="row wrap">
        <button
          class="primary"
          onclick={solve}
          disabled={recipe.targets.length === 0 || blankTarget || dupTarget}
        >{wf.planned ? 'solve again' : 'solve'}</button>
        {#if recipe.targets.length === 0}
          <span class="small muted">add at least one output</span>
        {:else if blankTarget}
          <span class="small muted">an output row has no type yet</span>
        {:else if dupTarget}
          <span class="small muted">two outputs are the same type with the same lineage</span>
        {:else if stale}
          <span class="tag warn">recipe changed — the result below is from the old one</span>
        {:else if wf.planned}
          <!-- solving locks the name and nothing else. Said out loud because the
               plan below reads as the finished article, and a page that only
               shows a result looks like it stopped taking edits. -->
          <span class="small muted">the recipe is still editable — solving again replans it</span>
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
            Nothing solved yet. Register what you have, say what you want, then
            solve — the planner works out the steps between them.
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
            solved <Ago iso={wf.generated_at} />{#if wf.result.stdlib_commit}
              · library <span class="mono">{wf.result.stdlib_commit.slice(0, 12)}</span>{/if}
          </p>
        {:else}
          <HintsPanel result={wf.result} onadd={useType} />
        {/if}
      </div>

      {#if wf.success}
        <div class="card col" style="gap:10px">
          <h3>run it</h3>
          <!-- An agent that is still being filled in is listed and disabled,
               not hidden: "the one I made is missing" is a worse thing to work
               out than "the one I made says it has no host yet". The route
               refuses the same agents, so this is a signpost, not the check.
               Never having been deployed is on that list too -- it is not one
               of the agent's `problems`, because the deploy button reads those
               and would disable itself, but it stops a run just as surely. -->
          <Field label="on which agent">
            <select
              bind:value={agentChoice}
              onchange={(e) => seedFromAgent(e.currentTarget.value)}
            >
              <option value="">choose an agent…</option>
              {#each app.agents.filter((a) => !a.archived_at) as a}
                {@const said = [
                  ...(a.problems ?? []),
                  ...(a.deployed === false ? ['has not been deployed yet'] : []),
                ]}
                <option value={a.name} disabled={said.length > 0}>
                  {a.name}{said.length ? ` — ${said.join(', ')}` : ''}
                </option>
              {/each}
            </select>
          </Field>
          {#if presets.length}
            <!-- the blank option names what it resolves to. It used to say
                 "(agent default)" for a thing agents could not declare, so it
                 silently meant `local` on every cluster login node. -->
            <Field label="nextflow preset">
              <select bind:value={presetChoice}>
                <option value="">{agentPreset} — this agent's default</option>
                {#each presets as p}<option value={p}>{p}</option>{/each}
              </select>
            </Field>
          {/if}
          <!-- Pre-filled from the agent, so what will be sent is on the screen
               rather than implied. Editing a row here changes this run only;
               the agent keeps what it declares. -->
          <div class="field">
            <span class="small muted">params</span>
            <ParamRows bind:rows={runParams} inherited={chosenAgent?.default_params ?? {}} />
            <span class="small muted hint">
              this run only — the agent's defaults are already here, and a key
              typed over one of them wins
            </span>
          </div>

          {#if wf.result?.step_display?.length}
            <!-- Keyed by position, which is what makes a selector address one
                 step. Empty is "as the transform declared", which is what the
                 greyed number in each box is. -->
            <div class="field">
              <span class="small muted">resources</span>
              <table class="small res">
                <thead>
                  <tr>
                    <th></th><th>step</th><th>cpus</th><th>memory (GB)</th><th>time (h)</th>
                  </tr>
                </thead>
                <tbody>
                  {#each wf.result.step_display as step}
                    <tr>
                      <td class="muted">{step.order}</td>
                      <td class="mono truncate" title={step.process ?? step.transform}>
                        {step.transform}
                      </td>
                      {#each OVERRIDE_FIELDS as f}
                        <td>
                          <input
                            class="num"
                            inputmode="decimal"
                            placeholder={step.declared_resources?.[f] ?? '—'}
                            aria-label={`${f} for step ${step.order}`}
                            value={overrides[step.order]?.[f] ?? ''}
                            oninput={(e) => {
                              overrides[step.order] = {
                                ...(overrides[step.order] ?? {}),
                                [f]: e.currentTarget.value,
                              }
                            }}
                          />
                        </td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
              <span class="small muted hint">
                left empty, a step gets what its transform declared
              </span>
            </div>
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
                    <!-- the suffix alone: the heading of this page is the
                         workflow, and the rest of every one of these names is
                         that same word -->
                    <button class="link" onclick={() => select('runs', `${wf.name}/${r.name}`)}>
                      {runSuffix(r.name, wf.name)}
                    </button>
                  </td>
                  <td class="muted">{r.agent}</td>
                  <td class="muted"><Ago iso={r.launched_at ?? r.created_at} /></td>
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

  /* the same shape `Field` renders, for the two blocks that hold rows rather
     than a single control and so cannot be a <label> */
  .field { display: flex; flex-direction: column; gap: 3px; }
  .hint { line-height: 1.3; }
  /* fixed layout so the step name truncates instead of pushing the number
     boxes off the card -- a table cell will not shrink on its own */
  .res { width: 100%; table-layout: fixed; }
  .res th { font-weight: normal; color: var(--muted); text-align: left; }
  .res th:first-child { width: 2em; }
  .res th:nth-child(n + 3) { width: 5.5em; }
  .res td { padding: 1px 4px 1px 0; }
  .res .num { width: 100%; min-width: 0; text-align: right; }
</style>
