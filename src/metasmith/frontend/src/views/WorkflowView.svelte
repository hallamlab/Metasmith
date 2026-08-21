<script>
  import { tick, untrack } from 'svelte'
  import { api } from '../lib/api.svelte.js'
  import {
    app, attempt, cachedWorkflow, cacheWorkflow, loadRuns, loadTypeIndex,
    loadTypes, loadWorkflows, notify, patchWorkflowSummary, renameWorkflow, saveOverrides,
    select, setLastAgent, ui, workflowRenameable,
  } from '../lib/state.svelte.js'
  import Ago from '../components/Ago.svelte'
  import ConfigEditor from '../components/ConfigEditor.svelte'
  import EditableName from '../components/EditableName.svelte'
  import Field from '../components/Field.svelte'
  import Icon from '../components/Icon.svelte'
  import JobLog from '../components/JobLog.svelte'
  import DagRail from '../components/DagRail.svelte'
  import MiniGraph from '../components/MiniGraph.svelte'
  import SaveAsTemplate from '../components/SaveAsTemplate.svelte'
  import SaveChip from '../components/SaveChip.svelte'
  import SampleTable from '../components/SampleTable.svelte'
  import ShareOut from '../components/ShareOut.svelte'
  import SidePanel from '../components/SidePanel.svelte'
  import Spinner from '../components/Spinner.svelte'
  import StageProgress from '../components/StageProgress.svelte'
  import HintsPanel from './HintsPanel.svelte'
  import LibraryList from './LibraryList.svelte'
  import RecipeCard from './RecipeCard.svelte'
  import ParamRows from '../components/ParamRows.svelte'
  import { fingerprint } from '../lib/fingerprint.js'
  import { isPlumbing, libraryGraph, transformGraph, typeGraph } from '../lib/graphs.js'
  import { around, children, parents } from '../lib/highlight.js'
  import { mintTargetId, refId, refKey, targetsFromWire, targetsToWire } from '../lib/lineage.js'
  import { runSuffix } from '../lib/runname.js'
  import { paramRows, sameParams, toParams } from '../lib/params.js'
  import { entries as rowEntries, normalize as normalizeRowList } from '../lib/rows.js'

  let { name } = $props()

  let wf = $state(null)
  // the standard-library type vocabulary is the same on every workflow page;
  // loaded once per session in state.svelte.js, not refetched per mount
  let types = $derived(app.types)
  let index = $derived(app.index)
  // a readout of what the last solve built the library into, not a form:
  // the recipe's rows are the form
  let items = $state([])
  let jobId = $state(null)
  let jobStatus = $state(null)
  let jobPhase = $state(null)
  // Which of the two jobs this page starts the log below is showing. The bar
  // and the summary line are read off different kinds of result, and only one
  // job runs at a time, so the page has to know which one it is watching.
  let jobKind = $state('solve')
  let jobRunning = $derived(!!jobId && jobStatus !== 'done' && jobStatus !== 'failed')

  // The solve button's own busy state, and the bar beside it. There's no
  // percentage worth showing -- the solver itself is one call we can't see
  // inside -- but the job genuinely does move through these three steps in
  // order (see the `PHASE:` markers `_work` emits in gui/api.py), so the bar
  // says which one it's on rather than faking a fill. It stays visible
  // through a failure (the failed segment is the point), but drops away on
  // success, once `jobStatus` reaches `'done'`.
  const SOLVE_STAGES = ['syncing', 'solving', 'finishing']
  // Covers the gap between the click and the POST resolving with a job id --
  // otherwise the button would sit un-busy for the first leg of the round
  // trip, which is exactly the moment a second click is most tempting.
  let requestingSolve = $state(false)
  let solving = $derived(requestingSolve || (jobKind === 'solve' && jobRunning))
  let showSolveBar = $derived(solving || (jobKind === 'solve' && jobStatus === 'failed'))
  let solveStage = $derived(Math.max(0, SOLVE_STAGES.indexOf(jobPhase)))
  let solveStageStates = $derived.by(() => {
    if (!showSolveBar) return SOLVE_STAGES.map(() => 'idle')
    return SOLVE_STAGES.map((_, i) =>
      i < solveStage ? 'done' : i > solveStage ? 'idle' : jobStatus === 'failed' ? 'failed' : 'running',
    )
  })
  // The same gap the solve button covers, for the setup button.
  let requestingSetup = $state(false)
  let settingUp = $derived(requestingSetup || (jobKind === 'environment' && jobRunning))
  // What the last setup on this page reported, rendered under the button.
  let envReport = $state(null)
  let sharing = $state(false)
  let savingTemplate = $state(false)
  let launching = $state(false)
  // Starts on the agent last used anywhere, not blank -- picking one every
  // time you open a workflow tab is a chore once you mostly run on one agent.
  let agentChoice = $state(ui.lastAgent)

  // This workflow's own copy of a Nextflow config preset, editable as raw
  // text -- decoupled from the shared package preset it was chosen from --
  // reusing `ConfigEditor` rather than a new editor component. `presetSource`
  // is the dropdown's own value: which preset this workflow is editing and
  // will stage and run with, persisted server-side (`/preset/adopt` writes it
  // into the workflow's request, the same file `target_types` and
  // `input_drafts` live in) exactly like the recipe and the resource
  // overrides are. Content is autosaved (see the effect below) rather than
  // held behind a save button.
  let presetLoaded = $state(null)
  let presetContent = $state('')
  let presetSource = $state('')
  let presetSaved = $state(false)
  // Locked to hidden until a preset is chosen -- there is nothing of this
  // workflow's own to show before then.
  let presetOpen = $state(false)
  let allPresets = $state([])
  let resetArmed = $state(false)
  let resetTimer

  async function loadPreset() {
    const body = await attempt(() => api.get(`/workflows/${name}/preset`))
    if (!body) return
    presetContent = body.content
    presetLoaded = body.content
    presetSource = body.preset_source ?? ''
  }

  async function savePreset() {
    await attempt(async () => {
      const body = await api.put(`/workflows/${name}/preset`, { content: presetContent })
      presetContent = body.content
      presetLoaded = body.content
      presetSaved = true
      setTimeout(() => (presetSaved = false), 1500)
      return true
    })
  }

  // Selecting a preset from the dropdown adopts it immediately -- there is
  // nothing to confirm, since picking one is picking which one you are
  // looking at, not a step you can get wrong. `reset` (below) is the
  // destructive move: re-adopting the preset already selected, discarding
  // whatever this workflow has since edited into it.
  async function adoptPreset(source) {
    if (!source) return
    await attempt(async () => {
      const body = await api.post(`/workflows/${name}/preset/adopt`, { source })
      presetContent = body.content
      presetLoaded = body.content
      presetSource = body.preset_source
      return true
    })
  }

  // Armed the same way `DeleteControl` is: a first press only proposes it, so
  // discarding this workflow's own edits back to the stock preset takes a
  // deliberate second press.
  function armReset() {
    if (!presetSource) return
    if (resetArmed) {
      clearTimeout(resetTimer)
      resetArmed = false
      adoptPreset(presetSource)
      return
    }
    resetArmed = true
    resetTimer = setTimeout(() => (resetArmed = false), 2000)
  }

  // A second after typing in the preset stops, it saves itself -- the same
  // debounce as a keystroke-driven save anywhere else on this page, so there
  // is nothing left to press once the box says what you want.
  $effect(() => {
    const content = presetContent
    if (presetLoaded === null || content === presetLoaded) return
    const t = setTimeout(savePreset, 1000)
    return () => clearTimeout(t)
  })
  // This run's params, pre-filled from the chosen agent so what will be sent is
  // visible rather than implied, and `seededParams` is what was put there -- how
  // the page tells "still the agent's defaults" from "someone typed over them".
  let runParams = $state([])
  let seededParams = $state({})
  // Per-step resources, keyed by step position, which is the only form that
  // produces a selector for one step rather than for every step of a transform.
  // Boxes are strings; the server reads and checks the numbers. Seeded from
  // what was typed in last time this workflow was open; the `name` effect
  // below reloads it whenever the workflow changes.
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

  // a row of the recipe naming its own type: the same thing, from the other
  // side. It settles the panel, so it is also what ends a preview -- the type
  // it names *is* the answer now, and there is nothing left to put back.
  const showType = (type) => {
    previewFrom = null
    pickType(type)
  }

  // Reading around an open type list moves the panel with you, which means
  // `focus` stops describing what the row holds for as long as that list is
  // open. What was there is snapshotted on the first preview and restored if
  // the list closes without a pick -- `drawing` as well as `focus`, because
  // `pickType` clears it, so a transform pinned in the panel would otherwise be
  // torn down by a scroll through a list.
  let previewFrom = $state(null)

  const previewType = (type) => {
    if (!previewFrom) previewFrom = { focus, drawing }
    pickType(type)
  }

  const endPreview = () => {
    if (!previewFrom) return
    focus = previewFrom.focus
    drawing = previewFrom.drawing
    previewFrom = null
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
  let recipe = $state({ targets: [], transform_libraries: [], rows: [] })
  let loadedFor = $state(null)

  // The input rows. These are the recipe: the input library is built from them
  // when the workflow is solved, so a row is never anything else and never stops
  // being editable. A workflow written before the key existed simply has none,
  // and one written before it was the whole story gets the rest of its rows from
  // the server on the first read. `rows.normalize` is the shape itself.
  const normalizeRows = (list) => normalizeRowList(list, nextRowId)

  // -- the sample table --------------------------------------------------------
  //
  // A sheet attached makes every row of the recipe a *sample array*: it never
  // registers as it stands, and each of its fields reads the column it binds.
  // What puts one library item per sheet row down is solving, not a separate
  // step here to remember -- `generate_workflow` re-syncs the registered items
  // against the current table and rows on every solve, so there is no
  // "expanded" state on this side of the wire to go stale, and nothing here
  // unregisters anything by hand either. Detaching is therefore all it takes to
  // put every row back to the text it was holding before.

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

  async function editTable() {
    const out = await attempt(() => api.get(`/workflows/${name}/table/raw`))
    return out?.text ?? ''
  }

  // Which rows every sample should see. Held as row references (`#id`), not as
  // paths: a row under a sheet registers one path per distinct set of cells and
  // none of them exists until the solve, so the request says which *row* and the
  // generate turns it into every path that row made. The key name is the spec's
  // own, because both the create and generate routes filter incoming bodies
  // against that list.
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
  // thing a moment later. A row that has chosen no column yet is *not* here: it
  // is a blank in the recipe, drawn on the row itself, and a half-filled recipe
  // is how a plan gets worked out.
  let tableProblem = $derived.by(() => {
    if (!table?.attached) return null
    if (table.problems?.length) return table.problems[0].message
    return null
  })

  // ...and what stops a *launch*, which is a different question with a
  // different answer: the blanks in the recipe the stored plan was solved from,
  // recorded at solve time and read back here. Absent on a result from before
  // this existed, which means none.
  let recipeProblems = $derived(wf?.result?.recipe_problems ?? [])

  // Every run of this workflow shares one task_key workspace on the agent, so
  // only one can actually be live at a time -- see the guard in store.py's
  // create_run. Surfacing it here keeps the button from being the way someone
  // discovers that the hard way.
  let liveRun = $derived(wf?.runs?.find((r) => r.live) ?? null)

  let rowSeq = 0
  const nextRowId = () => `d${(rowSeq++).toString(36)}${Math.random().toString(36).slice(2, 7)}`

  // `withAttachments` folds in what used to be `loadInputs`/`loadTable` as
  // separate requests -- one `include=inputs,table` call instead of three,
  // for a caller (the mount effect) that wants all of it. A caller that wants
  // only `wf` (a rename, an unarchive, the live-run poll) asks for that alone.
  async function load(withAttachments = false) {
    const q = withAttachments ? '?include=inputs,table' : ''
    const out = await api.get(`/workflows/${name}${q}`)
    if (withAttachments) {
      items = out.inputs?.items ?? []
      table = out.table ?? { attached: false }
      delete out.inputs
      delete out.table
    }
    wf = out
    if (loadedFor !== name) {
      loadedFor = name
      recipe = {
        targets: targetsFromWire(wf.request.target_types),
        transform_libraries: wf.request.transform_libraries ?? [],
        rows: normalizeRows(wf.request.input_drafts),
      }
      overrides = wf.overrides ?? {}
    }
    cacheWorkflow(name, { wf, items, table })
  }

  // Three reads, and only one of them is the page: the workflow (with its
  // inputs and table folded in, one request) decides whether anything
  // renders, while the type vocabulary and the index behind the panel are
  // what fill it in. Run together rather than in a chain, so the recipe is up
  // as soon as the workflow lands instead of after the slowest of the three.
  $effect(() => {
    const n = name
    // A workflow visited in the last 8 renders instantly from the cache while
    // the fetch below still runs behind it and overwrites both the live state
    // and the cache entry once it resolves -- stale-while-revalidate, not a
    // substitute for the fetch: the CLI can still write to a workflow between
    // visits, and this is a single-user local tool with no other way to notice.
    const hit = cachedWorkflow(n)
    if (hit) {
      wf = hit.wf
      items = hit.items
      table = hit.table
    } else {
      wf = null
      table = null
    }
    jobId = null
    jobStatus = null
    envReport = null
    focus = null
    drawing = null
    overrides = hit ? (hit.wf.overrides ?? {}) : {}
    // reset unconditionally, cache hit or not: this is what lets the one-time
    // recipe rebuild in `load()` still run on the background revalidation
    // fetch, so a recipe edited outside the browser surfaces even on a hit
    loadedFor = null
    planFocus = null
    presetLoaded = null
    presetContent = ''
    presetSource = ''
    presetOpen = false
    attempt(async () => {
      await Promise.all([load(true), loadTypes(), loadTypeIndex(), loadPreset()])
      void n
    })
  })

  // Loaded once, eagerly, on mount rather than on the dropdown's first open --
  // the same package-wide vocabulary on every workflow page, so there is
  // nothing per-workflow to key this effect off of.
  $effect(() => {
    attempt(async () => (allPresets = await api.get('/presets')))
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
  // an extra round trip on every change of a dropdown bought nothing.
  let chosenAgent = $derived(app.agents.find((a) => a.name === agentChoice) ?? null)

  // `agentChoice` starts pre-filled from the remembered agent, but nothing
  // seeded its params yet -- that only otherwise happens on the select's own
  // `onchange`. Fires again once `app.agents` (fetched separately) actually
  // has the entry; `seedFromAgent` is idempotent, so repeats are harmless.
  //
  // `seedFromAgent` reads and writes `runParams`/`seededParams` itself, and
  // its write is a fresh array/object every time even when nothing changed
  // -- so tracking those reads here would make the effect its own trigger,
  // looping forever instead of settling. `untrack` keeps this effect keyed
  // to only `chosenAgent`/`agentChoice`, which is the actual condition for
  // re-seeding.
  $effect(() => {
    if (!chosenAgent) return
    const next = agentChoice
    untrack(() => seedFromAgent(next))
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

  function pickPlanNode(id) {
    const n = (planGraph?.nodes ?? []).find((x) => x.id === id)
    if (!n) return
    // a click pins the lighting to this node, so it survives the pointer
    // leaving -- opening the panel is a kind of pointing too, and one that
    // outlasts the mouse
    planFocus = id
    if (n.kind !== 'transform') pickType(n.id)
    else if (n.transform_index != null) pickTransform(n.transform_index)
  }

  // Which way the diagram reads around whatever the pointer is on. Both
  // directions at once is what a plan diagram is *already* showing -- every
  // line is on the page -- so the useful question is one of "what does this
  // need" and "what needs this", answered one at a time. One hop: two hops on a
  // 73-node plan lights half the drawing, which is the same as lighting none.
  let planUpstream = $state(true)
  // whether the drawing and its step rows are showing at all -- see the switch
  // in `.dag-controls`, which is the only thing left up when this is off
  let dagOpen = $state(true)
  let planPointed = $state(null)

  // -- folding the diagram without the page moving under the pointer ---------
  //
  // The control row is sticky *inside* the section, so while you are scrolled
  // into a tall diagram it is pinned to the top of the column. Fold the
  // diagram away and the section becomes a few pixels tall: the row has
  // nothing left to be stuck to and drops back to wherever that short block
  // sits, which is usually off the top of the screen -- the switch you just
  // clicked is gone, and so is the plan you were reading around it.
  //
  // So the row is put back where it was. Collapsing the section also takes most
  // of the column's height with it, and the scroll position that puts the row
  // back is often past the end of what is left, so a spacer at the tail lends
  // the column the missing room. It is temporary: it is dropped on the first
  // scroll that no longer needs it to stand where it is, which is the only
  // moment removing it moves nothing.
  let dagEl = $state(null)
  let dagControlsEl = $state(null)
  let scrollPad = $state(0)

  const dagScroller = () => dagEl?.closest('.main') ?? null

  // Whether the control row has left its place and is riding the top of the
  // column. Only then is there a drawing behind it to blur; sitting where it
  // belongs it is over the card, and hazing that is a smudge over nothing.
  //
  // Read off a sentinel at the top of the section rather than by measuring the
  // row on every scroll event -- `getBoundingClientRect` in a scroll handler is
  // a forced layout per frame, and this is a yes/no that changes twice a page.
  let dagStuck = $state(false)
  let dagSentinel = $state(null)

  $effect(() => {
    const mark = dagSentinel
    const root = dagEl?.closest('.main')
    if (!mark || !root) return
    const io = new IntersectionObserver(([e]) => (dagStuck = !e.isIntersecting), { root })
    io.observe(mark)
    return () => io.disconnect()
  })

  async function toggleDag() {
    const scroller = dagScroller()
    const before = dagControlsEl?.getBoundingClientRect().top
    dagOpen = !dagOpen
    scrollPad = 0
    if (!scroller || before == null) return
    await tick()
    const want = scroller.scrollTop + (dagControlsEl.getBoundingClientRect().top - before)
    const room = scroller.scrollHeight - scroller.clientHeight
    if (want > room) {
      scrollPad = Math.ceil(want - room)
      await tick()
    }
    scroller.scrollTop = want
  }

  // Safe exactly when the column would still reach this scroll position without
  // the spacer; anything earlier and dropping it hauls the page up by the
  // difference, which is the jump the spacer was borrowed to prevent.
  function releasePad(e) {
    if (!scrollPad) return
    const el = e.currentTarget
    if (el.scrollTop <= Math.max(0, el.scrollHeight - scrollPad - el.clientHeight)) scrollPad = 0
  }
  // the last node clicked: kept lit once the pointer leaves, so a click reads
  // as a decision rather than a hover that happened to land on a button. The
  // live pointer still wins while it is somewhere on the drawing.
  let planFocus = $state(null)
  let planMarks = $derived(
    around(planGraph, {
      pointed: planPointed ?? planFocus,
      relation: planUpstream ? parents : children,
    }),
  )

  // -- taking the diagram away -------------------------------------------
  //
  // What the page shows and what a person wants to keep are different
  // pictures, so the download asks for its own rather than saving the one on
  // screen. Two of the three choices are the reason: the page is drawn in the
  // theme you are reading it in and on a ground its own card paints, and a
  // transparent dark-theme diagram dropped on a white slide is pale text on
  // white. So both default to what is in front of you -- the theme you are
  // reading in, on a ground of its own -- and the common case is one press.
  let dlTheme = $state(ui.theme)
  let dlFilled = $state(true)
  let downloading = $state(null)

  // mirrors the server's own cache naming, so two variants of one plan do not
  // land in the downloads folder as `plan.svg` and `plan (1).svg`
  let dlStem = $derived(
    `${name}${dlTheme === 'light' ? '' : '.dark'}${dlFilled ? '.filled' : ''}`,
  )

  function saveBlob(blob, filename) {
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filename
    a.click()
    // not immediately: Safari reads the href after the click returns
    setTimeout(() => URL.revokeObjectURL(url), 0)
  }

  // The PNG is the SVG, rasterized here. `RenderDAG` can write one -- but its
  // raster arm goes out through graphviz, which lays the graph out its own way,
  // so asking the server for a PNG would hand back a different picture under
  // the same name. A canvas draws exactly what the SVG says.
  async function svgToPng(svg, scale = 2) {
    const url = URL.createObjectURL(new Blob([svg], { type: 'image/svg+xml' }))
    try {
      const img = new Image()
      await new Promise((ok, fail) => {
        img.onload = ok
        img.onerror = () => fail(new Error('the diagram could not be rasterized'))
        img.src = url
      })
      // the renderer always writes width/height, and a canvas sized 0 saves a
      // blank file rather than failing, so this is checked rather than assumed
      const w = img.naturalWidth
      const h = img.naturalHeight
      if (!w || !h) throw new Error('the diagram has no size to rasterize at')
      const canvas = document.createElement('canvas')
      canvas.width = Math.round(w * scale)
      canvas.height = Math.round(h * scale)
      const ctx = canvas.getContext('2d')
      ctx.scale(scale, scale)
      ctx.drawImage(img, 0, 0)
      return await new Promise((ok, fail) =>
        canvas.toBlob((b) => (b ? ok(b) : fail(new Error('the PNG could not be encoded'))), 'image/png'),
      )
    } finally {
      URL.revokeObjectURL(url)
    }
  }

  async function downloadDag(format) {
    downloading = format
    await attempt(async () => {
      const url =
        `/api/workflows/${name}/dag?theme=${dlTheme}&background=${dlFilled ? 1 : 0}`
      const res = await fetch(url)
      // fetch resolves on a 404 as happily as on a 200, and a saved error page
      // named `plan.svg` is the worst possible way to find that out
      if (!res.ok) throw new Error(`the diagram could not be drawn (${res.status})`)
      const svg = await res.text()
      saveBlob(
        format === 'svg' ? new Blob([svg], { type: 'image/svg+xml' }) : await svgToPng(svg),
        `${dlStem}.${format}`,
      )
    })
    downloading = null
  }

  let drawingLabel = $derived.by(() => {
    if (drawing?.kind === 'transform') return index?.transforms?.[drawing.i]?.name ?? 'transform'
    if (drawing?.kind === 'library')
      return index?.libraries?.find((l) => l.path === drawing.path)?.name ?? 'library'
    return focus
  })

  // The recipe as it stands, in eleven characters -- taken off the exact body a
  // solve would send, so it cannot describe anything other than what the server
  // would be given. `requestBody` is a hoisted declaration, which is what lets
  // this sit beside the comparison it feeds rather than below the writer.
  let recipeFingerprint = $derived(fingerprint(requestBody()))

  // Is the plan below still the one this recipe produces?
  //
  // A solve stamps the fingerprint of the recipe it was handed into its own
  // result, so this is one string compare and the two sides are one
  // implementation. Comparing against the stored *request* instead -- which is
  // what this did -- asks the wrong record twice over: `persist` rewrites it on
  // every commit, so it tracks the editor rather than the plan, and the page's
  // copy of it is not reloaded after a solve, so the badge could neither appear
  // when the recipe had genuinely moved nor clear once it had been re-solved.
  //
  // A result with no fingerprint cannot answer -- one written before this
  // existed, or by the CLI, which mints none. Silent rather than permanently
  // accusing: "changed" is a claim, and there is no evidence for it here.
  let stale = $derived(
    wf?.planned &&
      !!wf.result?.recipe_fingerprint &&
      wf.result.recipe_fingerprint !== recipeFingerprint,
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
  //
  // `targetsToWire` is the one place the outputs are put in an order and the
  // one place a parent becomes a position -- and this is its only caller, so a
  // write path that skipped it would ship an id where the file wants an int and
  // be refused by name rather than saved as a plausible wrong number.
  //
  // `input_drafts` goes out through `normalizeRows` for the same reason it comes
  // in through it: this body is fingerprinted, and a fingerprint over a shape
  // that depends on which gesture built the row is a fingerprint that changes on
  // its own.
  function requestBody() {
    return {
      sample_type: null,
      target_types: targetsToWire(recipe.targets),
      transform_libraries: recipe.transform_libraries,
      input_drafts: normalizeRows(recipe.rows),
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
      recipe.targets = [...recipe.targets, { id: mintTargetId(), type: '', parents: [] }]
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
      // two answers per field, one live at a time -- the text, and the sheet
      // column. Which is live is the sheet's presence and nothing else.
      path: '',
      column: '',
      name: '',
      values: [{ key: '', value: '', column: '' }],
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
    const key = refKey(id)
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

  // Spread, never rebuilt: the id is what the rows are keyed on, so losing it
  // would restart the element a reorder is meant to animate.
  function patchTarget(id, patch) {
    recipe.targets = recipe.targets.map((t) => (t.id === id ? { ...t, ...patch } : t))
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

  // Lineage, whichever half of the recipe the row is in. One line, no branch on
  // the half: both store the keys of what they descend from, and only the
  // serialiser knows an output's parents end up as numbers.
  async function setParents(row, keys) {
    // an input row already stores its parents as references, because they share
    // a list with paths; an output's ids never leave the page, so they are kept
    // bare and the reference is put back on when the rows are built
    if (row.kind === 'target') patchTarget(row.id, { parents: keys.map(refId) })
    else patchRow(row.id, { parents: keys })
    await persist()
  }

  // The output goes, and so does every link into it. Nothing is renumbered --
  // the links name ids, and the positions are worked out fresh on the way to
  // disk -- but a link *to* the removed output is genuinely lost, and that is
  // said out loud because it silently changes the plan.
  async function removeTarget(id) {
    let dropped = false
    recipe.targets = recipe.targets
      .filter((t) => t.id !== id)
      .map((t) => ({
        ...t,
        parents: (t.parents ?? []).filter((p) => {
          if (p !== id) return true
          dropped = true
          return false
        }),
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
    const identity = (d) =>
      String(d.mode === 'value' ? rowEntries(d)[0]?.value : d.path).trim()
    const used = new Set()

    function fits(dtype, slot) {
      const entries = index.by_type?.[dtype]?.consumed_by ?? []
      return entries.length ? entries.some((e) => e.i === i && e.as === slot.as)
                            : dtype === slot.as
    }

    function claim(slot) {
      for (const d of recipe.rows) {
        const key = refKey(d.id)
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
        values: [{ key: '', value: '' }],
        dtype: slot.as,
        parents: (slot.parents ?? []).map((p) => stands.get(p)).filter(Boolean),
      }
      stands.set(k, refKey(d.id))
      used.add(refKey(d.id))
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

  // Re-copies the standard library from whatever `metasmith_libraries` is
  // installed over the one already cloned into the project -- the everyday
  // path (`bootstrap_project`) only clones once and never looks again, so
  // this is the only way an edited or upgraded library reaches a project that
  // already has one. Every open workflow's own copy of the type vocabulary is
  // resynced against it server-side, so this page's `index` is what is stale
  // afterward, not anything written to disk.
  let syncingLibs = $state(false)
  function syncLibraries() {
    syncingLibs = true
    attempt(() => api.post('/project/libraries/sync')).then((job) => {
      if (!job) {
        syncingLibs = false
        return
      }
      api.stream(job.id, () => {}, async (summary) => {
        syncingLibs = false
        const result = summary?.result
        if (summary?.status === 'failed' || result?.updated === false) {
          notify(result?.error ?? summary?.error ?? 'library sync failed', 'refused')
          return
        }
        await Promise.all([loadTypes(true), loadTypeIndex(true)])
        notify('standard library updated', 'info')
      })
    })
  }

  async function solve() {
    requestingSolve = true
    jobKind = 'solve'
    // Cleared here, not left to `JobLog`'s own reset -- that only fires once
    // `jobId` changes below, and the request round trip happens before that.
    // Without this, solving again after a failed (or even a successful) solve
    // would draw the bar from the *previous* job's last phase/status for that
    // whole gap -- a bar that looks finished, or failed, before the new job
    // has said anything at all.
    jobStatus = null
    jobPhase = null
    // Land the recipe first. `generate` takes the spec fields off this body but
    // builds the input library from the rows *on disk*, so a field left in the
    // same gesture that started the solve has to be written before the solve
    // reads it -- and that is also what makes the fingerprint below true: it
    // names the recipe the server will actually plan from.
    await persist()
    const body = requestBody()
    const job = await attempt(() =>
      api.post(`/workflows/${name}/generate`, { ...body, recipe_fingerprint: fingerprint(body) }),
    )
    requestingSolve = false
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

  // What the ∞ button puts in the time box. An empty box already means
  // something -- "whatever the transform declared" -- so "no limit at all"
  // needs a value of its own rather than the absence of one. The server knows
  // this token by name; see `UNLIMITED` in gui/api.py.
  const UNLIMITED = 'unlimited'

  // Keyed by the transform, not `step.order`: order is a position in the
  // CURRENT plan, and regenerating a workflow (e.g. pointing a given at a
  // different source, which drops or adds upstream steps) renumbers every
  // step after the change. A numeric key then silently reattaches to
  // whichever step now sits at that position -- not an error, just the wrong
  // step getting the override while the one it was meant for gets none. The
  // transform name is what backend/workflow_ops.py's string-keyed branch
  // already matches processes by (`.*__{tr}`), so this needs no server change.
  function stepKey(step) {
    return (step?.transform ?? '').replace(/\.py$/, '')
  }

  function setOverride(key, field, value) {
    overrides[key] = { ...(overrides[key] ?? {}), [field]: value }
    saveOverrides(name, overrides)
  }

  // Only the boxes with something in them, and only the steps with such a box.
  // An empty string sent as a value would be a resource directive of nothing.
  // Also drops any key that names no step in the CURRENT plan -- a leftover
  // from before a regenerate reshuffled step order, which must not be sent
  // under a stale key and land on whatever step now occupies it.
  function overridePayload() {
    const valid = new Set((wf?.result?.step_display ?? []).map(stepKey))
    const out = {}
    for (const [step, spec] of Object.entries(overrides)) {
      if (!valid.has(step)) continue
      const kept = {}
      for (const f of OVERRIDE_FIELDS) {
        const v = (spec?.[f] ?? '').toString().trim()
        if (v) kept[f] = v
      }
      if (Object.keys(kept).length) out[step] = kept
    }
    return Object.keys(out).length ? out : null
  }

  // Prepare the chosen agent for this workflow: the images its steps need if it
  // runs containers, the conda envs they name if it does not. The endpoint
  // stages first, because the manifest that answers "which ones" is written by
  // staging -- so pressing this and then `stage and run` does not stage twice.
  async function setupEnvironment(force = false) {
    requestingSetup = true
    jobKind = 'environment'
    jobStatus = null
    jobPhase = null
    envReport = null
    const job = await attempt(() =>
      api.post(`/workflows/${name}/environment`, { agent: agentChoice, force }),
    )
    requestingSetup = false
    if (job) jobId = job.id
  }

  async function launch() {
    launching = true
    const params = toParams(runParams)
    const out = await attempt(() =>
      api.post('/runs', {
        workflow: name,
        agent: agentChoice,
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
  // have chosen it on -- so it is editable here. Only the title the field wears
  // depends on which of the two names is still free to move; the rule itself,
  // and the rename, are `state.svelte.js`'s, since the rail renames too.
  let renameable = $derived(workflowRenameable(wf))
  let displayName = $derived(wf?.request?.display_name || wf?.name || '')

  async function commitRename(next) {
    // The rail renames too, so the rule for which of the two names moves lives
    // in `state.svelte.js` rather than here. A rename that moved the directory
    // reselects, which remounts this pane against the new route and reloads on
    // its own; a label change leaves the route alone and has to be re-read.
    const now = await renameWorkflow(wf, next)
    if (now === name) await load()
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
    <div class="col main" style="gap:14px" onscroll={releasePad}>
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
          <button class="small" onclick={() => (savingTemplate = true)}>save as template</button>
          <button class="small" onclick={() => (sharing = true)}>share</button>
        </div>
      </div>

      <div class="card col" style="gap:10px" id="msm-recipe">
        <RecipeCard
          {items}
          rows={recipe.rows}
          targets={recipe.targets}
          typeOptions={allTypes}
          {counts}
          {sharedPaths}
          columns={table?.columns ?? []}
          rowCount={table?.row_count ?? 0}
          rowUniques={table?.row_uniques ?? {}}
          expansion={table?.expansion ?? null}
          onshared={setShared}
          onfocus={showType}
          onpreview={previewType}
          onpreviewend={endPreview}
          onremoveRow={removeRow}
          onremoveTarget={removeTarget}
          onrow={patchRow}
          ontarget={patchTarget}
          onparents={setParents}
          oncommit={commitRow}
          onadd={addRow}
        >
          {#snippet tableStrip()}
            <SampleTable {table} onattach={attachTable} ondetach={detachTable} onedit={editTable} />
          {/snippet}
        </RecipeCard>

        <div class="row wrap">
          <button
            class="primary"
            onclick={solve}
            disabled={solving || settingUp || recipe.targets.length === 0 || blankTarget || dupTarget || !!tableProblem}
          >
            {#if solving}<Spinner />{/if}
            {solving ? 'solving…' : wf.planned ? 'solve again' : 'solve'}
          </button>
          {#if recipe.targets.length === 0}
            <span class="small muted">add at least one output</span>
          {:else if blankTarget}
            <span class="small muted">an output row has no type yet</span>
          {:else if dupTarget}
            <span class="small muted">two outputs are the same type with the same lineage</span>
          {:else if tableProblem}
            <span class="small muted">{tableProblem}</span>
          {:else if stale}
            <!-- ahead of the sheet's line below, which is a description rather
                 than a warning: a sheet-attached recipe can go stale exactly like
                 any other, and the line saying how the solve will read it was
                 hiding the one saying the plan is not from this recipe -->
            <span class="tag warn">recipe changed — the result below is from the old one</span>
          {:else if table?.attached}
            <span class="small muted">
              one unified solve over the sheet's {table.row_count}
              {table.row_count === 1 ? 'row' : 'rows'}
            </span>
          {/if}
        </div>

        {#if showSolveBar}
          <StageProgress stages={SOLVE_STAGES} stageStates={solveStageStates} />
        {/if}

        {#if jobId}
          <!-- one log for both jobs this page starts: a solve and a bundle
               expand are the same shape of thing to watch, and only one of
               them runs at a time. Closed by default -- watching it is what
               you came for while a job is running, but once it is not, the
               plan below it is, and this is the same amount of the card an
               already-solved workflow used to lose to a wall of scrollback. -->
          <details class="log-details">
            <summary class="small muted">log</summary>
            <JobLog
              {jobId}
              header={false}
              bind:status={jobStatus}
              bind:phase={jobPhase}
              onend={async (summary) => {
                if (jobKind === 'environment') {
                  // Nothing on the page is derived from this -- it is a report
                  // about the agent, not about the workflow -- so it is shown
                  // as it came back and nothing is refetched.
                  envReport = summary?.result ?? null
                  if (summary?.status === 'failed') {
                    notify(summary?.error ?? 'setup failed', 'refused')
                  }
                  return
                }
                // The SSE stream's own final payload already carries what a
                // solve produced -- plan_graph, step_display, given, targets,
                // hints, all of it, written to disk before the stream said
                // done -- so the plan updates from that directly rather than
                // paying a fresh `GET /workflows/<name>` to see what the
                // server just told us. `_workflow_summary`'s mirror fields
                // are rebuilt from the same result rather than re-read.
                const r = summary?.result
                if (r) {
                  wf = {
                    ...wf,
                    planned: true,
                    success: !!r.success,
                    task_key: r.task_key ?? null,
                    step_count: r.step_count ?? null,
                    // `write_result` stamps this at write time; the job's own
                    // `finished_at` lands at the same moment and is what the
                    // SSE summary actually carries
                    generated_at: summary.finished_at ?? new Date().toISOString(),
                    result: r,
                  }
                  // updates the one sidebar row a solve can change, rather
                  // than refetching and re-deriving every row's run count
                  patchWorkflowSummary(name, {
                    planned: wf.planned,
                    success: wf.success,
                    step_count: wf.step_count,
                    generated_at: wf.generated_at,
                    task_key: wf.task_key,
                  })
                }
                // Not skippable: `op_inputs.sync` runs before the solve and
                // can genuinely change registered-item counts and
                // sample-array expansion, so the existing items/table state
                // can be stale relative to what this solve just did. One
                // batched request stands in for the old `loadInputs` +
                // `loadTable` pair.
                const fresh = await api.get(`/workflows/${name}?include=inputs,table`)
                items = fresh.inputs?.items ?? []
                table = fresh.table ?? { attached: false }
                cacheWorkflow(name, { wf, items, table })
                if (summary?.status === 'failed') {
                  notify(summary?.error ?? 'solve failed', 'refused')
                }
              }}
            />
          </details>
        {/if}
      </div>

      <!-- The solved artifact: the DAG a solve produced, regardless of
           success/failure/pending. Kept apart from the solve card above (the
           action) and the stage-and-run card below (what a successful plan
           unlocks). -->
      <div class="card col" style="gap:10px">
        {#if !wf.planned}
          <h3>plan</h3>
          <p class="small muted">
            Nothing solved yet. Register what you have, say what you want, then
            solve — the planner works out the steps between them.
          </p>
        {:else if wf.success}
          <div class="spread">
            <div class="row">
              <h3>plan</h3>
              <span class="tag ok">success</span>
            </div>
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
               sideways. It does fold, though -- open by default -- because a
               plan with enough steps to need that room is also tall enough to
               push the run controls below it off screen, and closing it is the
               way back to them without scrolling past. The fold is a chip in
               the control row rather than a `<summary>`: the row has to stay up
               while the diagram is down, or there is nothing left to click to
               get it back, and a disclosure that outlives what it discloses is
               a stranger thing than a switch.

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
          <div class="dag-details" bind:this={dagEl}>
            <!-- where the control row sits when it is not riding the top of the
                 column; once this has scrolled out, the row is stuck -->
            <div class="dag-mark" bind:this={dagSentinel} aria-hidden="true"></div>
            {#if planGraph}
              <!-- Outside `.dag-scroll` on purpose: a plan wider than the card
                   scrolls sideways, and a control inside that scroller leaves
                   the corner it is meant to sit in the moment you use it. Two
                   labelled halves, one of them lit -- the same shape as the
                   recipe's file/value switch, so a direction is a thing you
                   pick rather than a single button whose own label is the only
                   record of which way it is currently pointed. The download
                   sits beside it rather than getting its own sticky corner --
                   two elements independently sticking to the same offset would
                   fight once both were stuck. -->
              <div class="dag-controls" class:stuck={dagStuck} bind:this={dagControlsEl}>
                <!-- three stacked panes of blurred drawing, strongest and
                     smallest last; they compound, so the blur falls off from
                     the corner the row is pinned to -- see `.dag-haze` -->
                <div class="dag-haze" aria-hidden="true">
                  <span></span><span></span><span></span>
                </div>
                <div class="dag-group">
                  <span class="dag-group-label">diagram</span>
                  <div class="dag-chips">
                    <div class="dag-dir" role="group" aria-label="show or fold away the diagram">
                      <button
                        type="button"
                        class:on={dagOpen}
                        title="the drawing and its step rows, at their natural size"
                        onclick={() => !dagOpen && toggleDag()}
                      >show</button>
                      <button
                        type="button"
                        class:on={!dagOpen}
                        title="fold it away — the run controls are below it"
                        onclick={() => dagOpen && toggleDag()}
                      >hide</button>
                    </div>
                    <div class="dag-dir" role="group" aria-label="which way the diagram lights">
                      <button
                        type="button"
                        class:on={planUpstream}
                        title="hovering a step lights what it needs"
                        onclick={() => (planUpstream = true)}
                      >parents</button>
                      <button
                        type="button"
                        class:on={!planUpstream}
                        title="hovering a step lights what needs it"
                        onclick={() => (planUpstream = false)}
                      >children</button>
                    </div>
                  </div>
                </div>
                <!-- The server keeps its own rendering of this same plan around
                     (`GET /workflows/<name>/dag`, cached beside the bundle) --
                     `DagRail` draws the nodes as buttons for the click-to-panel
                     behaviour above, which is not a file a person can keep, so
                     the download reaches past it for the plain SVG instead of
                     trying to serialize the interactive one.

                     Set apart from the direction switch by a gap, and then
                     gathered under a heading and a rule of its own: the two
                     toggles on the left of it choose what gets saved, and
                     reading them as more ways to light the diagram is the one
                     mistake this row can make. It goes with the diagram: what
                     it saves is a picture that is not on the screen, and a
                     folded section is not the moment to be offered one. -->
                {#if dagOpen}
                <div class="dag-group dag-export">
                  <span class="dag-group-label">export</span>
                  <div class="dag-chips">
                    <div class="dag-dir" role="group" aria-label="which theme to save in">
                      <button
                        type="button"
                        class:on={dlTheme === 'light'}
                        title="dark ink on a light ground"
                        onclick={() => (dlTheme = 'light')}
                      >light</button>
                      <button
                        type="button"
                        class:on={dlTheme === 'dark'}
                        title="light ink on a dark ground"
                        onclick={() => (dlTheme = 'dark')}
                      >dark</button>
                    </div>
                    <div class="dag-dir" role="group" aria-label="what ground to save on">
                      <button
                        type="button"
                        class:on={dlFilled}
                        title="carry the theme's own background"
                        onclick={() => (dlFilled = true)}
                      >filled</button>
                      <button
                        type="button"
                        class:on={!dlFilled}
                        title="no background — takes the colour of whatever it is placed on"
                        onclick={() => (dlFilled = false)}
                      >transparent</button>
                    </div>
                    <button
                      type="button"
                      class="dag-download"
                      disabled={!!downloading}
                      onclick={() => downloadDag('svg')}
                      title="download this diagram as an SVG"
                    >{downloading === 'svg' ? '…' : 'svg ⭳'}</button>
                    <button
                      type="button"
                      class="dag-download"
                      disabled={!!downloading}
                      onclick={() => downloadDag('png')}
                      title="download this diagram as a PNG, at twice its drawn size"
                    >{downloading === 'png' ? '…' : 'png ⭳'}</button>
                  </div>
                </div>
                {/if}
                {#if dagOpen && wf.result?.step_display?.length}
                  <!-- The one column heading, now: `.res-head` used to draw a
                       second copy of it in flow over the rows, pixel-chased
                       onto the diagram's first node, but a plan that scrolled
                       the page past that point had already carried this bar up
                       here to answer "which column is cpus" -- so the in-flow
                       one was always the redundant half. Pushed to the far
                       right of the row by `margin-left: auto` rather than
                       inline after the export group: the row's columns are
                       right-justified against the same edge (see `.dag-row`),
                       and matching `--res-cols` here is what keeps this bar
                       sitting directly over them instead of just labelling the
                       row from wherever it happens to end. -->
                  <div class="dag-group dag-resources">
                    <span class="dag-group-label">resources</span>
                    <div class="dag-chips dag-res-head">
                      <span>cpus</span><span>memory (GB)</span><span>time (h)</span><span></span>
                    </div>
                  </div>
                {/if}
              </div>
            {/if}
            {#if dagOpen}
            <!-- Right-justified: the resource columns are a fixed width, so
                 they anchor the row's right edge and the diagram grows to
                 their left as the plan does. Only `.dag-scroll` scrolls --
                 the columns are a flex sibling outside it, so they and the
                 header above stay in place while the diagram itself pans. -->
            <div class="dag-row">
              <div class="dag-scroll">
                {#if planGraph}
                  <DagRail
                    geo={planGraph}
                    marks={planMarks}
                    meta={planMeta}
                    ground="var(--panel)"
                    onpick={pickPlanNode}
                    onhover={(id) => (planPointed = id)}
                  />
                {/if}
              </div>

              {#if wf.result?.step_display?.length}
                    <!-- Keyed by position, which is what makes a selector
                         address one step. Empty is "as the transform
                         declared", which is what the greyed number in each box
                         is. The rows and the drawing are flex siblings sharing
                         a top, so a row's offset is its node's `cy` less half a
                         pitch -- and both numbers come from the placement the
                         server stored, never from a constant here. -->
                    <div class="res-body" style={`height: ${dagHeight}px`}>
                      <!-- Nothing else here is in normal flow -- the guides and
                           rows below are all absolutely placed, which is how a
                           row can sit at its node's own `cy` instead of the
                           next slot in a stack -- so this is what gives the box
                           its width. Sized off `--res-cols`, the same template
                           the header above and the rows below both use, rather
                           than a number restated here that could drift from
                           theirs. -->
                      <div class="res-sizer" aria-hidden="true"></div>
                      <!-- One line down the middle of each value column, so a
                           number can be followed to its heading across the gap
                           the rows leave between them. Runs the full height of
                           the box; on the same grid template as the rows,
                           which is the only thing keeping it centred. -->
                      <div class="res-guides" aria-hidden="true">
                        <span></span><span></span><span></span>
                      </div>
                      {#each wf.result.step_display as step, i}
                        {@const cy = planCy.get(step.order) ?? (i + 0.5) * pitch}
                        {@const key = stepKey(step)}
                        <div
                          class="res-row"
                          style={`top: ${cy - pitch / 2}px; height: ${pitch}px`}
                          title={step.process ?? step.transform}
                          data-step={step.order}
                          data-transform={step.transform}
                        >
                          {#each OVERRIDE_FIELDS as f}
                            {@const v = overrides[key]?.[f] ?? ''}
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
                                oninput={(e) => setOverride(key, f, e.currentTarget.value)}
                              />
                            {/if}
                          {/each}
                          <!-- a column of its own rather than a passenger in
                               the time cell: the three headings then sit right
                               over the three numbers, which are right-aligned -->
                          <button
                            class="inf"
                            class:on={overrides[key]?.duration_h === UNLIMITED}
                            aria-pressed={overrides[key]?.duration_h === UNLIMITED}
                            title={overrides[key]?.duration_h === UNLIMITED
                              ? 'back to a time limit'
                              : 'run with no time limit at all'}
                            aria-label={`no time limit for step ${step.order}`}
                            onclick={() =>
                              setOverride(
                                key,
                                'duration_h',
                                overrides[key]?.duration_h === UNLIMITED ? '' : UNLIMITED,
                              )}
                          >∞</button>
                        </div>
                      {/each}
                    </div>
                  {/if}
            </div>
            {/if}
          </div>

          <p class="small muted">
            solved <Ago iso={wf.generated_at} />{#if wf.result.stdlib_commit}
              against library <span class="mono">{wf.result.stdlib_commit.slice(0, 12)}</span>{/if}
          </p>
        {:else}
          <HintsPanel result={wf.result} onadd={useType} />
        {/if}
      </div>

      {#if wf.success}
      <div class="card col" style="gap:10px">
        <h3>stage and run</h3>
        <!-- Pre-filled from the agent, so what will be sent is on the screen
             rather than implied. Editing a row here changes this run only;
             the agent keeps what it declares. -->
        <div class="field">
          <span class="small muted">params</span>
          <ParamRows bind:rows={runParams} inherited={chosenAgent?.default_params ?? {}} />
        </div>

        <!-- An agent that is still being filled in is listed and disabled,
             not hidden: "the one I made is missing" is a worse thing to work
             out than "the one I made says it has no host yet". The route
             refuses the same agents, so this is a signpost, not the check.
             Never having been deployed is on that list too -- it is not one
             of the agent's `problems`, because the deploy button reads those
             and would disable itself, but it stops a run just as surely. -->
        <Field label="agent">
          <select
            bind:value={agentChoice}
            onchange={(e) => {
              seedFromAgent(e.currentTarget.value)
              setLastAgent(e.currentTarget.value)
            }}
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
        <div class="field">
          <div class="row wrap" style="gap:8px; align-items:center;">
            <span class="small muted">preset</span>
            <div class="dag-dir" role="group" aria-label="show or hide the preset editor">
              <button
                type="button"
                class:on={presetOpen}
                disabled={!presetSource}
                onclick={() => presetSource && (presetOpen = true)}
              >show</button>
              <button
                type="button"
                class:on={!presetOpen}
                disabled={!presetSource}
                onclick={() => (presetOpen = false)}
              >hide</button>
            </div>
            <select
              class="small preset-select"
              bind:value={presetSource}
              onchange={(e) => adoptPreset(e.currentTarget.value)}
            >
              <option value="" disabled>choose a preset…</option>
              {#each allPresets as p}<option value={p}>{p}</option>{/each}
            </select>
            <button
              type="button"
              class="small"
              disabled={!presetSource}
              onclick={armReset}
              title={resetArmed
                ? "click again to confirm — this discards this workflow's own edits"
                : 'reset this preset to its stock content, discarding this workflow\'s own edits'}
            >{resetArmed ? 'confirm reset?' : 'reset'}</button>
            {#if presetSaved}<span class="tag ok">saved</span>{/if}
          </div>
          {#if presetOpen}
            <ConfigEditor
              bind:value={presetContent}
              rows={30}
              resizable={false}
              language="plain"
              label="nextflow preset"
            />
          {/if}
        </div>

        <div class="row" style="gap:8px">
          <button
            class="primary"
            onclick={launch}
            disabled={!agentChoice || launching || settingUp || recipeProblems.length > 0 || !!liveRun}
          >
            {launching ? 'launching…' : 'stage and run'}
          </button>
        </div>
        {#if liveRun}
          <!-- Every run of this workflow stages into the same task_key
               workspace on the agent -- one PID.lock, one process group. A
               second run launched now wouldn't run alongside the live one, it
               would silently take over that workspace, so cancelling either
               run afterwards could kill the wrong one. -->
          <p class="small muted">
            run <strong>{liveRun.name}</strong> is still {liveRun.state} — cancel it before starting another.
          </p>
        {/if}
        {#if recipeProblems.length}
          <!-- A blank in the recipe is reported and never refused, right up to
               here: a deferred input has no file to stage and a nameless pair
               has no key to be read under. Off the solve that made this plan,
               not off what the boxes say now -- so the way out is to fill it
               in and solve again, which is also what puts the fix in the
               bundle. The route refuses the same thing; this is a signpost. -->
          <p class="small bad">
            This plan was solved from an unfinished recipe: {recipeProblems.join('; ')}.
            Fill them in and solve again.
          </p>
        {/if}
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
                    <!-- The watcher couldn't tell if this run is still going --
                         same repeating failure every tick, since nothing about
                         it changes on its own -- so surface it rather than let
                         a wedged run sit there looking identical to a healthy
                         one. -->
                    {#if r.live && r.probe_error}
                      <span class="tag warn" title={r.probe_error}>can't check status</span>
                    {/if}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}

      <!-- borrowed room, so folding the diagram away can put the control row
           back where it was; see `toggleDag` -->
      {#if scrollPad}
        <div class="scroll-pad" aria-hidden="true" style={`height: ${scrollPad}px`}></div>
      {/if}
    </div>

    <SidePanel
      id="workflow"
      fill
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
        <button
          class="lib-sync"
          disabled={syncingLibs}
          onclick={syncLibraries}
          title={syncingLibs ? 'syncing…' : 're-copy the standard library from what is installed, and resync every workflow against it'}
          aria-label="sync the standard library"
        >
          <span class:spin={syncingLibs}><Icon name="regenerate" size={13} /></span>
        </button>
      {/snippet}

      <!-- Two things, and the second one takes what the first leaves. There was
           a third below the drawing, listing what the library adds up to; those
           numbers are under the chips now, which is where the switches that
           move them are, and the drawing gets the height it was giving up to
           them. `SidePanel` still has its own split -- the run page's tree and
           preview want it. -->
      <LibraryList
        {index}
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
    </SidePanel>
  </div>

  {#if sharing}
    <ShareOut kind="workflow" name={wf.name} onclose={() => (sharing = false)} />
  {/if}

  {#if savingTemplate}
    <SaveAsTemplate
      workflowName={wf.name}
      suggestedName={displayName}
      onclose={() => (savingTemplate = false)}
    />
  {/if}
{/if}

<style>
  /* The column scrolls, not the page: that puts its scrollbar at its own right
     edge, with the panel outside it rather than behind it. `main` is in flush
     mode for this view (App.svelte) so this row owns the height. */
  .pane { display: flex; flex: 1; min-width: 0; height: 100%; align-items: stretch; }
  .main { flex: 1; min-width: 0; overflow-y: auto; padding: 18px; }
  /* the column is a flex box with a gap, and an empty tail item would still be
     given one; this has to be exactly the height it is asked for */
  .scroll-pad { flex: 0 0 auto; margin-top: -14px; }
  .loading { padding: 18px; }
  /* The diagram sits on the card's own ground: it is drawn with no plate of its
     own, and one painted under it was never any colour but this card's -- which
     is also what a hollow marker is filled with, since hollow reads hollow only
     where the fill and the ground agree. The resource columns are a fixed
     width, so the row is right-justified against them instead of centred: the
     diagram grows to their left as the plan does, and only once it runs out of
     room does it scroll -- the columns stay put rather than being carried off
     sideways with it. No fold and no height cap on the row itself: it grows
     with the plan. */
  .dag-row { display: flex; align-items: flex-start; justify-content: flex-end; gap: 10px; margin-top: 8px; }
  /* shrinks below its own content width before it grows the row -- which is
     what lets it scroll instead of pushing `.res-body` off the right edge */
  .dag-scroll { overflow-x: auto; min-width: 0; flex: 0 1 auto; }
  /* the fixed-width half of the row; never shrinks, which is what anchors the
     right edge `.dag-row` justifies against */
  .res-body { flex: 0 0 auto; position: relative; }
  /* the fold around the job log: a summary its own
     row with the status pill riding beside it, so a job's outcome reads
     without opening the scrollback that produced it */
  .log-details summary {
    cursor: pointer;
    width: fit-content;
    user-select: none;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .log-details summary:hover { color: var(--text); }
  /* the corner the direction toggle sits in. Anchored to the fold and not to
     the scroller inside it, so a wide plan scrolled sideways leaves it where
     it was. Top left, not top right: the direction is read before the
     diagram, not after it -- and top right is where the info panel's own
     grip sits when this same diagram is reused there. */
  .dag-details {
    position: relative;
    /* the one column template the sticky header, the guide lines and the rows
       all share -- restated in any one of them and it can drift out of step
       with the others, which is exactly the "two headers" this replaces */
    --res-cols: 4.5rem 6rem 4.5rem 1.75rem;
  }
  /* zero, as far as the layout is concerned: it exists to be watched */
  .dag-mark { height: 1px; margin-bottom: -1px; }
  .dag-controls {
    position: sticky;
    z-index: 5;
    top: 0;
    left: 0;
    display: flex;
    width: 100%;
    box-sizing: border-box;
    /* the export block is a heading taller than the direction switch; bottom
       alignment is what keeps the one pill level with the row of chips rather
       than floating against the middle of the taller block */
    align-items: flex-end;
    gap: 6px;
  }
  /* The row floats over the drawing it controls, and small type over a diagram
     is hard to read; a plate under it would fix that and put a hard-edged card
     across the middle of the plan. So: the drawing itself, blurred -- and the
     blur itself eased off toward the rim rather than a single blurred patch
     faded out, which still ends in a ring you can see the edge of.

     One layer per step of that ramp. Each is masked to a smaller patch than the
     one below, and since `backdrop-filter` takes in everything already painted
     under it, the four compound: ~3.5px at the middle, ~1.75px at the widest
     ring, nothing at all by the rim.

     The tint is the panel's own colour, which makes the whole thing invisible
     rather than a smudge when the diagram is folded away and there is nothing
     behind it to blur. */
  /* Anchored to the top corners the row is pinned between. Hard on three sides
     and soft on one: flush left and flush right at the card's own borders --
     the row now runs the diagram chips out to the resources header, edge to
     edge -- and flush top, where the column's overflow clips it against the
     nav bar. All three are edges the page already draws, so an edge there
     reads as the card, not as a plate. It fades out only downwards, over the
     drawing below.

     Only while the row is riding the top of the column. Docked, it is over the
     card with nothing behind it to blur. */
  .dag-haze { display: none; }
  .dag-controls.stuck .dag-haze {
    display: block;
    position: absolute;
    inset: -24px -14px -24px -14px;
    z-index: -1;
    pointer-events: none;
    background: linear-gradient(
      135deg,
      color-mix(in srgb, var(--panel) 55%, transparent) 20%,
      transparent 80%
    );
  }
  /* Each pane is blurrier and stops sooner than the one under it, and they
     compound -- `backdrop-filter` reads in whatever is already painted below.
     So the blur *strength* steps down down the box rather than one uniform
     blur being faded out, which only ever reads as a plate with a soft rim.
     Full width on every pane, left edge to right edge alike -- there is
     nothing left of the row to taper toward once both sides are flush with
     the card. */
  .dag-haze span {
    position: absolute;
    left: 0;
    right: 0;
    top: 0;
    bottom: 0;
    -webkit-mask-image: linear-gradient(to bottom, #000 var(--vcore), transparent var(--vedge));
    mask-image: linear-gradient(to bottom, #000 var(--vcore), transparent var(--vedge));
  }
  .dag-haze span:nth-child(1) {
    --vcore: 45%;
    --vedge: 100%;
    -webkit-backdrop-filter: blur(1.5px);
    backdrop-filter: blur(1.5px);
  }
  .dag-haze span:nth-child(2) {
    --vcore: 18%;
    --vedge: 55%;
    -webkit-backdrop-filter: blur(2.5px);
    backdrop-filter: blur(2.5px);
  }
  .dag-haze span:nth-child(3) {
    --vcore: 0%;
    --vedge: 26%;
    -webkit-backdrop-filter: blur(3.5px);
    backdrop-filter: blur(3.5px);
  }
  .dag-dir {
    display: inline-flex;
    border: 1px solid var(--line);
    border-radius: 999px;
    overflow: hidden;
    background: var(--panel);
    opacity: 0.75;
  }
  .dag-dir:hover { opacity: 1; }
  .dag-dir button {
    border: none;
    background: none;
    color: var(--muted);
    padding: 1px 8px;
    font-size: 11px;
  }
  .dag-dir button.on { background: var(--accent); color: var(--panel); }
  /* The global `select { width: 100% }` rule (app.css) is meant for a select
     alone in a field, not one sharing a row with a chip and a button -- left
     unset here it claims the row's full width and pushes everything after it
     onto its own line, so the "inline" row wraps one control per line. */
  .preset-select { width: auto; }
  /* Each cluster under a heading and a rule of its own, so what a chip acts on
     is read off the group rather than guessed from the chip. */
  .dag-group {
    display: inline-flex;
    flex-direction: column;
    align-items: stretch;
    gap: 2px;
  }
  .dag-group-label {
    color: var(--muted);
    font-size: 10px;
    line-height: 1;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  .dag-chips {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding-top: 4px;
    border-top: 1px solid var(--line);
  }
  /* the gap that says these choose what is saved rather than what is drawn */
  .dag-export { margin-left: 18px; }
  /* pushed to the row's own right edge rather than a fixed gap after export --
     that is what keeps it flush with `.res-body` below, which is anchored to
     the same edge by `.dag-row`'s `justify-content: flex-end` */
  .dag-resources { margin-left: auto; }
  /* "resources" names the whole grid below, not just its left edge */
  .dag-resources .dag-group-label { text-align: center; }
  .dag-res-head {
    display: grid;
    grid-template-columns: var(--res-cols);
    /* the same box model as `.res-row` and `.res-guides` below -- border and
       padding both count toward the grid's own width, so a header sized any
       other way sits shifted from the columns it names */
    box-sizing: border-box;
    border-left: 1px solid transparent;
    border-right: 1px solid transparent;
    padding: 0 6px;
    color: var(--muted);
    font-size: 12px;
    text-align: center;
  }
  /* a lone pill, where `.dag-dir` is a pair of them: one gesture whose result
     arrives as a file, so there is no state for a second half to name */
  .dag-download {
    display: inline-block;
    border: 1px solid var(--line);
    border-radius: 999px;
    background: var(--panel);
    color: var(--muted);
    padding: 1px 8px;
    font-size: 11px;
    text-decoration: none;
    opacity: 0.75;
  }
  .dag-download:hover { opacity: 1; color: var(--text); }
  .dag-download:disabled { opacity: 0.4; }

  /* beside the panel's title, at the weight of the fold button next to it --
     an action on the project's library, not on the workflow the panel is
     otherwise about */
  .lib-sync {
    display: flex;
    padding: 4px;
    background: none;
    border-color: transparent;
    color: var(--muted);
  }
  .lib-sync:hover:not(:disabled) { color: var(--text); background: var(--panel-2); }
  .lib-sync .spin { display: flex; animation: lib-sync-spin 0.9s linear infinite; }
  @keyframes lib-sync-spin {
    to { transform: rotate(360deg); }
  }
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
  /* why the launch button is off, in the colour the rest of the page refuses in */
  p.bad { color: var(--bad); line-height: 1.3; }
  /* the steps beside the diagram: rows can't be independently positioned
     inside an actual <table>, so each one is an absolutely placed grid row
     instead, `top:` pinned to its transform's `dag_cy`. The sizer is the only
     thing here in normal flow, which is deliberate -- it is what gives this box
     its width, since absolutely placed rows contribute none. Make it absolute
     and the box collapses and the centring goes with it. All three share one
     column template so they line up like a table's columns did; there is no
     name column, because the node level with the row is the name. */
  .res-sizer,
  .res-row,
  .res-guides {
    display: grid;
    /* rem, not em: the header is 12px and a row is the body's 14px, so an
       em-based track resolves to two different widths and the rows overflow
       the box the sizer measured -- which is how the ∞ button ended up outside
       its own row's outline. The last track is the ∞ button's own, and it is
       fixed rather than `auto` for the same reason: the sizer's fourth cell is
       empty, so an `auto` track is nothing there and a button's width here. */
    grid-template-columns: var(--res-cols);
    align-items: center;
    gap: 4px;
  }
  /* In normal flow and otherwise empty -- everything else in `.res-body` is
     absolutely placed, which is how a row lands at its node's own `cy`
     instead of the next slot in a stack, and that leaves this the only thing
     here sizing the box. Zero height so it takes no visual space: the header
     that used to live in flow here now rides in `.dag-controls` instead, and
     restating its rem widths there off the same `--res-cols` is what keeps
     the two from drifting apart. */
  .res-sizer {
    height: 0;
    overflow: hidden;
    box-sizing: border-box;
    border-left: 1px solid transparent;
    border-right: 1px solid transparent;
    padding: 0 6px;
  }
  /* the outline is what lets a value be followed back to the node it sits
     level with; its height is the diagram's own row pitch, set inline */
  .res-row {
    position: absolute;
    z-index: 1;
    left: 0;
    right: 0;
    box-sizing: border-box;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 0 6px;
  }
  /* the guides, under everything above. Border and padding are the row's, not
     the sizer's: a track has to land where the numbers are, and the row's 1px
     outline shifts its content box by that much. Runs the full height of the
     box now that nothing above it needs the room. */
  .res-guides {
    position: absolute;
    z-index: 0;
    inset: 0;
    box-sizing: border-box;
    border: 1px solid transparent;
    padding: 0 6px;
    align-items: stretch;
    pointer-events: none;
    /* well under the row outlines they run between: a guide is for following a
       column, not for being looked at */
    opacity: 0.45;
  }
  .res-guides span {
    background: linear-gradient(var(--line), var(--line)) center / 1px 100% no-repeat;
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
