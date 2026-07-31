<script>
  import { api } from '../lib/api.svelte.js'
  import { app, attempt, loadRuns, loadWorkflows, notify, select } from '../lib/state.svelte.js'
  import Ago from '../components/Ago.svelte'
  import EditableName from '../components/EditableName.svelte'
  import Field from '../components/Field.svelte'
  import JobLog from '../components/JobLog.svelte'
  import DagRail from '../components/DagRail.svelte'
  import MiniGraph from '../components/MiniGraph.svelte'
  import SaveChip from '../components/SaveChip.svelte'
  import SampleTable from '../components/SampleTable.svelte'
  import ShareOut from '../components/ShareOut.svelte'
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
  // a readout of what the last solve built the library into, not a form:
  // the recipe's rows are the form
  let items = $state([])
  let jobId = $state(null)
  let sharing = $state(false)
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

  // Picking a type in the panel moves the panel, and nothing else. It used to
  // also write that type into whichever recipe row was last focused -- a
  // holdover from the builder card, which had one type field and nowhere else
  // for a pick to go. Now every row has its own field, and clicking a row's
  // type to look at it is what puts that row in focus: so reading around the
  // graph afterwards retyped the row you had just been reading about, quietly,
  // once per click. The panel is for looking; the row is where you type.
  // The task key is how this plan is named on the server side of a bug
  // report or a log line, and typing it out by hand is where a transposed
  // character comes from -- so the chip that shows it is the way to get it,
  // not just a label. `execCommand` is the fallback for a browser that
  // refuses clipboard permission even on localhost.
  async function copyTaskKey(key) {
    if (!key) return
    try {
      await navigator.clipboard.writeText(key)
    } catch {
      const ta = document.createElement('textarea')
      ta.value = key
      ta.style.cssText = 'position:fixed;opacity:0'
      document.body.appendChild(ta)
      ta.select()
      document.execCommand('copy')
      ta.remove()
    }
    notify(`copied [${key}]`, 'info')
  }

  function pickType(type) {
    focus = type
    drawing = null
  }

  // a row of the recipe naming its own type: the same thing, from the other side
  const showType = pickType

  function pickTransform(i) {
    drawing = { kind: 'transform', i }
  }

  function pickLibrary(path) {
    drawing = drawing?.kind === 'library' && drawing.path === path
      ? null
      : { kind: 'library', path }
  }

  // the editable recipe, kept separate from the frozen result below it
  let recipe = $state({ targets: [], transform_libraries: [], rows: [] })
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

  // The input rows. These are the recipe: the input library is built from them
  // when the workflow is solved, so a row is never anything else and never
  // stops being editable. Read defensively -- a workflow written before the key
  // existed simply has none, and one written before it was the whole story gets
  // the rest of its rows from the server on the first read.
  function normalizeRows(list) {
    return (list ?? [])
      .filter((d) => d && typeof d === 'object')
      .map((d) => ({
        id: String(d.id ?? nextRowId()),
        mode: d.mode === 'value' ? 'value' : 'file',
        path: d.path ?? '',
        // Carried, never shown, never set on a new row. A value row states no
        // name any more, but a recipe written when it did is the only thing
        // that can still say where its *array* items are -- those are not in
        // the record's row map, only in its generation list, so dropping this
        // before a sync has run would re-mint every one of them. Delete it a
        // release after `minted` is populated everywhere.
        name: d.name ?? '',
        value: d.value ?? '',
        dtype: d.dtype ?? '',
        parents: [...(d.parents ?? [])],
      }))
  }

  // -- the sample table --------------------------------------------------------
  //
  // A row of the recipe whose path (or a value row's name or value) names a
  // column of the attached sheet is a *sample array*: it never registers as it
  // stands. What puts one library item per sheet row down is solving, not a
  // separate step here to remember -- `generate_workflow` re-syncs the
  // registered items against the current table and rows on every solve, so
  // there is no "expanded" state on this side of the wire to go stale, and
  // nothing here unregisters anything by hand either. Which makes a row an
  // array is the token, not a flag -- there is one list of input rows, and a
  // row stops being an array the moment its last token goes.
  const TOKEN = /\{[^{}]*\}/
  const isArrayRow = (d) =>
    d.mode === 'value' ? TOKEN.test(d.value ?? '')
                       : TOKEN.test(d.path ?? '')

  let table = $state(null)

  async function loadTable() {
    table = await api.get(`/workflows/${name}/table`)
  }

  async function attachTable(file, text) {
    const ok = await attempt(async () => {
      if (file) {
        const form = new FormData()
        form.append('file', file)
        await api.upload(`/workflows/${name}/table`, form)
      } else {
        await api.post(`/workflows/${name}/table`, { text })
      }
      return true
    })
    if (ok) await loadTable()
  }

  async function detachTable() {
    await attempt(() => api.del(`/workflows/${name}/table`))
    await loadTable()
  }

  // Which rows every sample should see. Held as row references (`#id`), not as
  // paths: a row may not have a path yet, which is the normal state of a fresh
  // recipe, and the generate turns each one into a path between building the
  // library and solving from it. The key name is the spec's own, because both
  // the create and generate routes filter incoming bodies against that list.
  let sharedPaths = $derived(wf?.request?.shared_input_paths ?? [])

  async function setShared(key, on) {
    const next = on
      ? [...new Set([...sharedPaths, key])]
      : sharedPaths.filter((p) => p !== key)
    await attempt(async () => {
      await api.put(`/workflows/${name}`, { shared_input_paths: next })
      return true
    })
    await load()
  }

  // What stops a solve. Solving is what registers a sample row now, and it
  // refuses the same way the old manual expand did -- surfaced here too, so
  // the button says why rather than a solve starting and failing on the same
  // thing a moment later.
  let arrayCount = $derived(recipe.rows.filter(isArrayRow).length)
  let tableProblem = $derived.by(() => {
    if (!table?.attached || !arrayCount) return null
    if (table.problems?.length) return table.problems[0].message
    return null
  })

  let rowSeq = 0
  const nextRowId = () => `d${(rowSeq++).toString(36)}${Math.random().toString(36).slice(2, 7)}`

  async function load() {
    wf = await api.get(`/workflows/${name}`)
    if (loadedFor !== name) {
      loadedFor = name
      recipe = {
        targets: normalize(wf.request.target_types),
        transform_libraries: wf.request.transform_libraries ?? [],
        rows: normalizeRows(wf.request.input_drafts),
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
    table = null
    jobId = null
    focus = null
    drawing = null
    loadedFor = null
    attempt(async () => {
      await Promise.all([
        load(),
        loadInputs(),
        loadTable(),
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

  // -- the plan's own drawing ----------------------------------------------
  //
  // Laid out by the server when the plan was solved and stored beside the
  // result, so a page load costs no layout and the drawing cannot disagree with
  // the step rows beside it. A data node's id *is* its type name, which is what
  // the panel addresses a type by; a step node carries the index of the
  // transform it runs, resolved server-side against the library index rather
  // than string-matched here.
  let planGraph = $derived(wf?.result?.plan_graph ?? null)

  let planCy = $derived(
    new Map(
      (planGraph?.nodes ?? []).filter((n) => n.step != null).map((n) => [n.step, n.cy]),
    ),
  )

  let planMeta = $derived(
    new Map(
      (planGraph?.nodes ?? []).map((n) => [
        n.id,
        {
          kind: n.kind === 'transform' ? 'transform' : 'type',
          // the synthetic `given` node, and any step whose library is not the
          // indexed one, have nothing on the right to be shown
          disabled: n.kind === 'transform' && n.transform_index == null,
        },
      ]),
    ),
  )

  let planFocus = $derived.by(() => {
    const nodes = planGraph?.nodes ?? []
    if (drawing?.kind === 'transform')
      return nodes.find((n) => n.transform_index === drawing.i)?.id ?? null
    return focus && nodes.some((n) => n.id === focus) ? focus : null
  })

  function pickPlanNode(id) {
    const n = (planGraph?.nodes ?? []).find((x) => x.id === id)
    if (!n) return
    if (n.kind !== 'transform') pickType(n.id)
    else if (n.transform_index != null) pickTransform(n.transform_index)
  }

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

  // entries are {i, as, match} -- a transform is on this list because its
  // *properties* fit, which is not the same as having named this type
  function matching(type, side) {
    const entries = index?.by_type?.[type]?.[side] ?? []
    if (!enabled) return entries
    return entries.filter((e) => enabled.has(index.transforms[e.i]?.library))
  }

  // The standard library's type files are offerable regardless of which
  // transform libraries are toggled -- they aren't owned by any of them. A
  // type that only exists in the index because a transform named it is
  // different: that name came from a library, so switching that library off
  // should take the name off the list too.
  let allTypes = $derived(
    [
      ...new Set([
        ...types.filter((t) => t.full_name).map((t) => t.full_name),
        ...Object.keys(index?.by_type ?? {}).filter(
          (t) => matching(t, 'produced_by').length || matching(t, 'consumed_by').length,
        ),
      ]),
    ].sort(),
  )
  let typeNames = $derived(new Set(allTypes))

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

  // `sample_type` is written out as null, always, rather than left off: the
  // server merges a request over the stored one, so omitting the key would
  // keep whatever a previous version of this page (or the CLI) put there. The
  // table never derives one -- every table-driven solve is one unified view
  // over the whole DAG the sheet describes.
  function requestBody() {
    return {
      sample_type: null,
      target_types: recipe.targets,
      transform_libraries: recipe.transform_libraries,
      input_drafts: recipe.rows,
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

  // -- editing the recipe ------------------------------------------------------

  function addRow(kind) {
    if (kind === 'output') {
      recipe.targets = [...recipe.targets, { type: '', parents: [] }]
      persist()
      return
    }
    // 'input' is the only add-input gesture -- what the row holds, file or
    // value, is a field on the row itself (the mode switch), not a choice made
    // up front. 'file' is just the starting mode.
    addInput(kind === 'value' ? 'value' : 'file')
  }

  function addInput(mode, extra = {}) {
    const d = {
      id: nextRowId(),
      mode,
      path: '',
      name: '',
      value: '',
      dtype: '',
      parents: [],
      ...extra,
    }
    recipe.rows = [...recipe.rows, d]
    persist()
    return d
  }

  // Typing is local; it is written back when the field is left. Persisting per
  // keystroke would be a round trip per character, and the field is the truth
  // until then either way.
  function patchRow(id, patch) {
    recipe.rows = recipe.rows.map((d) => (d.id === id ? { ...d, ...patch } : d))
    touch()
  }

  // The row goes, and so does every link into it -- here and in the shared
  // list, which is keyed the same way. What it registered as goes at the next
  // solve: the library is built from the rows, so a row that is not there
  // registers nothing.
  async function removeRow(id) {
    const key = `#${id}`
    recipe.rows = recipe.rows
      .filter((d) => d.id !== id)
      .map((d) => ({ ...d, parents: d.parents.filter((p) => p !== key) }))
    if (sharedPaths.includes(key)) {
      await attempt(() =>
        api.put(`/workflows/${name}`, {
          shared_input_paths: sharedPaths.filter((p) => p !== key),
        }),
      )
    }
    await persist()
    await load()
  }

  function patchTarget(i, patch) {
    recipe.targets = recipe.targets.map((t, j) => (j === i ? { ...t, ...patch } : t))
    touch()
  }

  // Every edit on a row is local until the field is left; this is leaving it,
  // and saving the recipe is the whole of what it does. There is no second step
  // and nothing changes shape: the row is what the user is editing, and the
  // input library is built from it when the workflow is solved.
  //
  // A type field is a combobox rather than a plain input, so "left" is focus
  // moving out of the whole control, not out of the box inside it.
  const commitRow = persist

  // Lineage, whichever half of the recipe the row is in. Both are positions in
  // the request now -- an input by its row id, an output by its index.
  async function setParents(row, keys) {
    if (row.kind === 'target') {
      patchTarget(row.id, {
        parents: keys.map((k) => Number(k.slice(1))).sort((a, b) => a - b),
      })
    } else {
      patchRow(row.id, { parents: keys })
    }
    await persist()
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
    showType(type)
    addInput('file', { dtype: type })
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
    // and the index has already answered it: a type appears under a transform's
    // `consumed_by` exactly when the solver would accept it there. A row whose
    // type is not in the index at all falls back to naming the slot's type
    // outright, which is what a row typed by hand against an unknown library is.
    //
    // A filler is *consumed*: what is credited to one requirement is not offered
    // to the next, or one file would satisfy three slots. A row with nothing in
    // it still occupies the requirement, so pressing apply again does not stamp
    // a second copy; it is reported as blank rather than counted as present.
    const identity = (d) => (d.mode === 'value' ? d.value : d.path).trim()
    const used = new Set()

    function fits(dtype, slot) {
      const entries = index.by_type?.[dtype]?.consumed_by ?? []
      return entries.length ? entries.some((e) => e.i === i && e.as === slot.as)
                            : dtype === slot.as
    }

    function claim(slot) {
      for (const d of recipe.rows) {
        const key = `#${d.id}`
        if (used.has(key) || !d.dtype || !fits(d.dtype, slot)) continue
        const id = identity(d)
        return { key, by: id || `a new ${d.dtype}`, blank: !id }
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
        id: nextRowId(),
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
      recipe.rows = [...recipe.rows, ...made]
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

  // The pitch a step's row falls back to when the backend sent no geometry at
  // all (an old cached result, or a plan whose geometry failed) -- stacked in
  // order rather than left to collide at the top. Whenever `dag_geometry` is
  // there its own `row_pitch` is used instead: this is the diagram's spacing
  // and guessing at it is how the two stopped agreeing.
  const ROW_H = 26

  // The column header's own height. It has to sit *inside* the rows box and in
  // normal flow, because it is the only thing there that is -- absolutely
  // placed rows contribute no width, so it is what sizes the box. It is then
  // nudged down by `position: relative` onto the diagram's first row, which
  // costs the layout nothing: the rows' origin stays the image's own top, and
  // the header takes the space beside a node that never has a row of its own.
  const HEAD_H = 18

  // What the ∞ button puts in the time box. An empty box already means
  // something -- "whatever the transform declared" -- so "no limit at all"
  // needs a value of its own rather than the absence of one. The server knows
  // this token by name; see `UNLIMITED` in gui/api.py.
  const UNLIMITED = 'unlimited'

  function setOverride(order, field, value) {
    overrides[order] = { ...(overrides[order] ?? {}), [field]: value }
  }

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
  // to it. The server decides that; past that point the *directory* keeps its
  // name (the plan is keyed to it), but the label everyone reads does not have
  // to be — `display_name` rides in `request.yml` beside it, an ordinary field
  // `write_request` already merges through, and changes it without moving
  // anything a run or a cache key points at.
  let renameable = $derived(!wf?.planned && !wf?.runs?.length && !wf?.archived_at)
  let displayName = $derived(wf?.request?.display_name || wf?.name || '')

  async function commitRename(next) {
    if (renameable) {
      const out = await attempt(async () => {
        // the same PUT the recipe saves through: a workflow's name is a field
        // of it, and an id in the body that differs from the url is a rename
        const body = await api.put(`/workflows/${name}`, { name: next })
        await loadWorkflows()
        return body
      })
      // the name is the route: reselect so the pane reloads against the new one
      if (out) select('workflows', out.name)
      return
    }
    // locked: the label changes, the directory does not, so there is nothing
    // to reselect -- just a field of the same record to reload
    await attempt(async () => {
      await api.put(`/workflows/${name}`, { display_name: next })
      await load()
      return true
    })
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
            value={displayName}
            editable={!wf.archived_at}
            title={renameable
              ? 'rename this workflow'
              : 'give this workflow a label — the plan underneath keeps its own name'}
            lockedTitle="an archived workflow keeps its name"
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
             one that happens to be open. Sharing is the opposite -- it is about
             this workflow and what is in it -- so it is here. -->
        <div class="row">
          {#if wf.archived_at}<button onclick={unarchive}>restore</button>{/if}
          <button class="small" onclick={() => (sharing = true)}>share</button>
        </div>
      </div>

      <div class="card" id="msm-recipe">
        <RecipeCard
          {items}
          rows={recipe.rows}
          targets={recipe.targets}
          typeOptions={allTypes}
          {counts}
          {sharedPaths}
          columns={table?.columns ?? []}
          rowCount={table?.row_count ?? 0}
          expansion={table?.expansion ?? null}
          onshared={setShared}
          onfocus={showType}
          onremoveRow={removeRow}
          onremoveTarget={removeTarget}
          onrow={patchRow}
          ontarget={patchTarget}
          onparents={setParents}
          oncommit={commitRow}
          onadd={addRow}
        >
          {#snippet tableStrip()}
            <SampleTable {table} onattach={attachTable} ondetach={detachTable} />
          {/snippet}
        </RecipeCard>
      </div>

      <div class="row wrap">
        <button
          class="primary"
          onclick={solve}
          disabled={recipe.targets.length === 0 || blankTarget || dupTarget || !!tableProblem}
        >{wf.planned ? 'solve again' : 'solve'}</button>
        {#if recipe.targets.length === 0}
          <span class="small muted">add at least one output</span>
        {:else if blankTarget}
          <span class="small muted">an output row has no type yet</span>
        {:else if dupTarget}
          <span class="small muted">two outputs are the same type with the same lineage</span>
        {:else if tableProblem}
          <span class="small muted">{tableProblem}</span>
        {:else if arrayCount}
          <span class="small muted">
            one unified solve over the sheet's {arrayCount}
            {arrayCount === 1 ? 'column' : 'columns'}
          </span>
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
        <!-- one log for both jobs this page starts: a solve and a bundle
             expand are the same shape of thing to watch, and only one of them
             runs at a time -->
        <JobLog
          {jobId}
          onend={async () => {
            // four independent reads, not a chain: solving is ~400ms of server
            // and this used to add three sequential round trips to the end of it
            await Promise.all([load(), loadInputs(), loadTable(), loadWorkflows()])
          }}
        />

        {#if !wf.planned}
          <h3>plan</h3>
          <p class="small muted">
            Nothing solved yet. Register what you have, say what you want, then
            solve — the planner works out the steps between them.
          </p>
        {:else if wf.success}
          <div class="spread">
            <h3>plan</h3>
            <div class="row">
              <span class="tag ok">{wf.step_count} step(s)</span>
              <button
                class="tag mono copyable"
                onclick={() => copyTaskKey(wf.task_key)}
                title="copy this plan's task key"
              >{wf.task_key}</button>
            </div>
          </div>

          <!-- The DAG is the plan, stated once; the steps beside it are the
               only other thing that used to restate it as `# / step / takes /
               produces`, so their rows are pinned to the same vertical
               position as the node they describe rather than a copy of the
               drawing in words -- which is also why a row carries no name: the
               node level with it is the name. No height cap: this grows with
               the plan, and only a diagram wider than the card scrolls,
               sideways. It does fold, though -- behind a `<details>`, open by
               default -- because a plan with enough steps to need that room is
               also tall enough to push the run controls below it off screen,
               and closing it is the way back to them without scrolling past.

               The drawing is `DagRail`, the same component the recipe's
               lineage rails and the info panel use, over placement the server
               stored when it solved. It used to be an `<img>` of a rendered
               SVG, with each step's controls pinned to a `cy` measured off
               that image; drawn here, a step's row is an ordinary sibling
               placed at the same pitch, and the nodes themselves are clickable
               into the panel on the right. No pan and no zoom -- the diagram
               is its natural size and the card scrolls. -->
          {@const pitch = planGraph?.row_pitch ?? ROW_H}
          {@const dagHeight = planGraph?.height ?? (wf.result?.step_display?.length ?? 0) * pitch}
          {@const topCy = planGraph?.nodes?.length
            ? Math.min(...planGraph.nodes.map((n) => n.cy))
            : HEAD_H / 2}
          <details class="dag-details" open>
            <summary class="small muted">diagram</summary>
            <div class="dag-scroll">
              <div class="dag-box">
                <div class="dag-body">
                  {#if planGraph}
                    <DagRail
                      geo={planGraph}
                      focus={planFocus}
                      meta={planMeta}
                      ground="var(--panel-2)"
                      onpick={pickPlanNode}
                    />
                  {/if}

                  {#if wf.result?.step_display?.length}
                    <!-- Keyed by position, which is what makes a selector
                         address one step. Empty is "as the transform
                         declared", which is what the greyed number in each box
                         is. The rows and the drawing are flex siblings sharing
                         a top, so a row's offset is its node's `cy` less half a
                         pitch -- and both numbers come from the placement the
                         server stored, never from a constant here. -->
                    <div class="res-body" style={`height: ${dagHeight}px`}>
                      <!-- level with the diagram's first node, which is the
                           synthetic `given` and so never has a row of its own -->
                      <div
                        class="res-head"
                        style={`height: ${HEAD_H}px; top: ${topCy - HEAD_H / 2}px`}
                      >
                        <span>cpus</span><span>memory (GB)</span><span>time (h)</span><span></span>
                      </div>
                      {#each wf.result.step_display as step, i}
                        {@const cy = planCy.get(step.order) ?? (i + 0.5) * pitch}
                        <div
                          class="res-row"
                          style={`top: ${cy - pitch / 2}px; height: ${pitch}px`}
                          title={step.process ?? step.transform}
                          data-step={step.order}
                          data-transform={step.transform}
                        >
                          {#each OVERRIDE_FIELDS as f}
                            {@const v = overrides[step.order]?.[f] ?? ''}
                            {#if f === 'duration_h' && v === UNLIMITED}
                              <!-- The box cannot show a number for this, and
                                   showing an empty one would read as the other
                                   meaning, so it says which it is. -->
                              <span class="unlimited mono" title="no time limit">∞ no limit</span>
                            {:else}
                              <input
                                class="num"
                                inputmode="decimal"
                                placeholder={step.declared_resources?.[f] ?? '—'}
                                aria-label={`${f} for step ${step.order}`}
                                value={v}
                                oninput={(e) => setOverride(step.order, f, e.currentTarget.value)}
                              />
                            {/if}
                          {/each}
                          <!-- a column of its own rather than a passenger in
                               the time cell: the three headings then sit right
                               over the three numbers, which are right-aligned -->
                          <button
                            class="inf"
                            class:on={overrides[step.order]?.duration_h === UNLIMITED}
                            aria-pressed={overrides[step.order]?.duration_h === UNLIMITED}
                            title={overrides[step.order]?.duration_h === UNLIMITED
                              ? 'back to a time limit'
                              : 'run with no time limit at all'}
                            aria-label={`no time limit for step ${step.order}`}
                            onclick={() =>
                              setOverride(
                                step.order,
                                'duration_h',
                                overrides[step.order]?.duration_h === UNLIMITED ? '' : UNLIMITED,
                              )}
                          >∞</button>
                        </div>
                      {/each}
                    </div>
                  {/if}
                </div>
              </div>
            </div>
          </details>

          <p class="small muted">
            solved <Ago iso={wf.generated_at} />{#if wf.result.stdlib_commit}
              · library <span class="mono">{wf.result.stdlib_commit.slice(0, 12)}</span>{/if}
          </p>

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

          <div>
            <button class="primary" onclick={launch} disabled={!agentChoice || launching}>
              {launching ? 'launching…' : 'stage and run'}
            </button>
          </div>
          <p class="small muted">
            The same workflow can run on any agent — staging copies it there
            first, then launches and detaches.
          </p>
        {:else}
          <HintsPanel result={wf.result} onadd={useType} />
        {/if}
      </div>

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
      id="workflow"
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

      <!-- One column, one scrollbar. These three used to be two sections with a
           grip between them: the libraries and the drawing pinned at a
           remembered height, the inspector scrolling in whatever was left. That
           made reading the bottom of the inspector a matter of first resizing
           the top, and the drawing was squeezed by whatever the last drag had
           left it. Now the panel scrolls as a whole and the drawing is a fixed
           frame within it. `SidePanel` still has the split -- the run page's
           tree and preview want it. -->
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

  {#if sharing}
    <ShareOut kind="workflow" name={wf.name} onclose={() => (sharing = false)} />
  {/if}
{/if}

<style>
  /* The column scrolls, not the page: that puts its scrollbar at its own right
     edge, with the panel outside it rather than behind it. `main` is in flush
     mode for this view (App.svelte) so this row owns the height. */
  .pane { display: flex; flex: 1; min-width: 0; height: 100%; align-items: stretch; }
  .main { flex: 1; min-width: 0; overflow-y: auto; padding: 18px; }
  .loading { padding: 18px; }
  /* The diagram sits on the card's own ground: it is drawn with no plate of its
     own, and one painted under it was never any colour but this card's -- which
     is also what a hollow marker is filled with, since hollow reads hollow only
     where the fill and the ground agree. The block it makes with the step rows is
     narrower than the card, so it is centred as one thing -- and the scroller
     around it is what keeps a plan wider than the card from widening the card
     instead of scrolling. No fold and no height cap: this grows with the plan. */
  .dag-scroll { overflow-x: auto; margin-top: 8px; }
  .dag-box { width: max-content; margin-inline: auto; display: flex; flex-direction: column; gap: 4px; }
  /* the drawing and the rows box, flex siblings with nothing between them: they
     share a top by construction, which is the whole of the alignment */
  .dag-body { display: flex; align-items: flex-start; gap: 10px; }
  /* the fold around the diagram: a `summary` its own row, not indented under
     the drawing the way a browser default reads, since this one is a sibling
     of the run controls it is standing between the plan and */
  .dag-details summary {
    cursor: pointer;
    width: fit-content;
    user-select: none;
  }
  .dag-details summary:hover { color: var(--text); }
  .link {
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    text-align: left;
  }
  .link:hover { text-decoration: underline; border: none; }

  /* a `.tag` that happens to be a `<button>`: reset to the plain pill it looks
     like everywhere else, then say so is only a hover away */
  .tag.copyable {
    font: inherit;
    font-size: 11px;
    cursor: pointer;
  }
  .tag.copyable:hover { color: var(--text); border-color: var(--accent); }

  /* the same shape `Field` renders, for the two blocks that hold rows rather
     than a single control and so cannot be a <label> */
  .field { display: flex; flex-direction: column; gap: 3px; }
  .hint { line-height: 1.3; }
  /* the steps beside the diagram: rows can't be independently positioned
     inside an actual <table>, so each one is an absolutely placed grid row
     instead, `top:` pinned to its transform's `dag_cy`. The header is the only
     thing here in normal flow, which is deliberate -- it is what gives this box
     its width, since absolutely placed rows contribute none. Make it absolute
     and the box collapses and the centring goes with it. Both share one column
     template so they line up like a table's columns did; there is no name
     column, because the node level with the row is the name. */
  .res-head,
  .res-row {
    display: grid;
    /* rem, not em: the header is 12px and a row is the body's 14px, so an
       em-based track resolves to two different widths and the rows overflow
       the box the header sized -- which is how the ∞ button ended up outside
       its own row's outline. The last track is the ∞ button's own, and it is
       fixed rather than `auto` for the same reason: the header's fourth cell is
       empty, so an `auto` track is nothing there and a button's width here. */
    grid-template-columns: 4.5rem 5.75rem 4.5rem 1.75rem;
    align-items: center;
    gap: 4px;
  }
  /* relative, not absolute: it still occupies its place in flow -- which is
     what sizes the box -- and is only painted lower */
  .res-head {
    position: relative;
    font-weight: normal;
    color: var(--muted);
    font-size: 12px;
    padding: 0 6px;
    white-space: nowrap;
    text-align: center;
  }
  .res-body { position: relative; }
  /* the outline is what lets a value be followed back to the node it sits
     level with; its height is the diagram's own row pitch, set inline */
  .res-row {
    position: absolute;
    left: 0;
    right: 0;
    box-sizing: border-box;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 0 6px;
  }
  /* trimmed to clear the row's outline: growing the row instead would break
     the alignment, since its height is the diagram's pitch */
  .res-row .num { width: 100%; min-width: 0; text-align: center; padding: 2px 6px; }
  .unlimited {
    font-size: 11px;
    color: var(--accent);
    white-space: nowrap;
    text-align: center;
  }
  .inf {
    padding: 2px 6px;
    line-height: 1;
    font-size: 13px;
    background: none;
    color: var(--muted);
  }
  .inf.on { color: var(--accent); border-color: var(--accent); }
</style>
