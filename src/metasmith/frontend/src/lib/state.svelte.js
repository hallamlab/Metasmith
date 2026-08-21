import { api } from './api.svelte.js'

export const SECTIONS = [
  // in setup order: you need a host before an agent, an agent before a run
  { id: 'ssh', label: 'SSH' },
  { id: 'agents', label: 'Agents' },
  { id: 'workflows', label: 'Workflows' },
  { id: 'runs', label: 'Runs' },
]

// -- the url's hash: the one thing that survives a reload on its own ------
//
// `#<section>/<id>` -- a run's id already has a slash in it (`workflow/run`),
// so the id half is percent-encoded and only that half. Read once, before
// `app` picks its starting values, so the first render lands on what the url
// says rather than on `ssh` and a beat later somewhere else.

function parseHash() {
  const raw = window.location.hash.replace(/^#\/?/, '')
  const cut = raw.indexOf('/')
  const section = cut === -1 ? raw : raw.slice(0, cut)
  if (!SECTIONS.some((s) => s.id === section)) return { section: 'ssh', id: null }
  const id = cut === -1 ? null : decodeURIComponent(raw.slice(cut + 1))
  return { section, id: id || null }
}

function writeHash(section, id) {
  const next = `#${section}${id ? `/${encodeURIComponent(id)}` : ''}`
  // a plain assignment jumps the page to an element with that id if one
  // exists; replaceState never scrolls and never grows history, so clicking
  // through fifty workflows does not turn the back button into fifty steps
  if (window.location.hash !== next) history.replaceState(null, '', next)
}

const fromUrl = parseHash()

export const app = $state({
  section: fromUrl.section,
  // one remembered selection per section, so switching tabs does not lose your place
  selected: { ssh: null, agents: null, workflows: null, runs: null, [fromUrl.section]: fromUrl.id },
  showArchived: false,
  project: null,
  hosts: [],
  sshPath: '',
  agents: [],
  workflows: [],
  runs: [],
  notice: null,
  loading: false,
  // the standard-library type vocabulary: identical for every workflow in the
  // project, so it is loaded once per session rather than once per workflow
  types: [],
  index: null,
})

// -- the left rail's width -------------------------------------------------
// One value for all four sections: the rail is the same furniture whichever tab
// you are on, and a width that changed under you when you switched would read as
// a glitch. Kept out of `app` because nothing reloads it from the server.

export const RAIL_DEFAULT = 280
export const RAIL_MIN = 180
export const RAIL_MAX = 720

const RAIL_KEY = 'metasmith.railWidth'

export function clampRail(w) {
  return Math.min(RAIL_MAX, Math.max(RAIL_MIN, Math.round(w)))
}

function storedRailWidth() {
  try {
    const raw = Number(localStorage.getItem(RAIL_KEY))
    return Number.isFinite(raw) && raw > 0 ? clampRail(raw) : RAIL_DEFAULT
  } catch {
    return RAIL_DEFAULT
  }
}

// -- the side panels' geometry ---------------------------------------------
// Same furniture on the other edge as the rail, and the same reasoning within
// one panel: a width kept across workflows, and an open state remembered
// because someone who closed it does not want it back on the next thing they
// open.
//
// Unlike the rail, this is keyed *per panel*. There is more than one now -- the
// workflow's type inspector and the run's file preview -- and they are not the
// same furniture: one holds a graph and one holds a file tree, so their useful
// heights differ, and having closed one says nothing about the other. Sharing
// one set of keys made each panel's geometry a side effect of having visited
// the other page.

export const PANEL_DEFAULT = 320
export const PANEL_MIN = 240
export const PANEL_MAX = 720

// The panel is split across, not just down: a graph above, the list below. The
// height of the upper half is remembered the same way the widths are -- a graph
// wants more room on a tall screen than on a laptop, and being told that once
// should be enough.
export const PANEL_TOP_DEFAULT = 420
export const PANEL_TOP_MIN = 96
export const PANEL_TOP_MAX = 1200

// The keys the single shared panel used. Read once, as the starting value for
// a panel that has none of its own, so nobody's remembered width is thrown
// away by the split. Never written to again.
const LEGACY_KEYS = {
  width: 'metasmith.panelWidth',
  open: 'metasmith.panelOpen',
  top: 'metasmith.panelTop',
}

const panelKey = (id, part) => `metasmith.panel.${id}.${part}`

export function clampPanel(w) {
  return Math.min(PANEL_MAX, Math.max(PANEL_MIN, Math.round(w)))
}

export function clampPanelTop(h) {
  return Math.min(PANEL_TOP_MAX, Math.max(PANEL_TOP_MIN, Math.round(h)))
}

// -- collapsed rail groups -------------------------------------------------
// The runs rail is grouped by the workflow each run belongs to, and a project
// with a dozen workflows is otherwise one undifferentiated list. Which groups
// are shut is remembered because it is a statement about what you are working
// on, not about this page load.
//
// Stored as the *collapsed* set rather than the expanded one so a workflow that
// appears after you last looked comes in open.

const COLLAPSED_KEY = 'metasmith.runs.collapsed'

function storedCollapsed() {
  try {
    const raw = JSON.parse(localStorage.getItem(COLLAPSED_KEY) ?? '[]')
    return new Set(Array.isArray(raw) ? raw.filter((s) => typeof s === 'string') : [])
  } catch {
    return new Set()
  }
}

// -- light / dark ----------------------------------------------------------
// The preference is *tri*-state -- follow the OS, light, dark -- because "match
// what the machine is set to" and "I picked this one" are two different things
// and collapsing them loses the first: a page that stored `dark` on load can
// never tell a deliberate dark from an OS that happened to be dark that day.
// The button is only two-way, though: it flips to the opposite of what is
// showing, which pins a choice. Nothing cycles back to following the OS except
// clearing the key.

// -- last agent used to run a workflow --------------------------------------
// Which agent was picked last, so opening a new workflow tab starts on the
// agent you actually use rather than the empty "choose an agent…" prompt
// every time. One value across all workflows -- the choice is about which
// machine you are working from today, not about this particular workflow.

const LAST_AGENT_KEY = 'metasmith.lastAgent'

const THEME_KEY = 'metasmith.theme'

function osTheme() {
  try {
    // dark is the fallback, not light: it is what the page was before there
    // was a choice, so a browser that cannot answer keeps the incumbent
    return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
  } catch {
    return 'dark'
  }
}

function stored(key, fallback, parse) {
  try {
    const raw = localStorage.getItem(key)
    return raw === null ? fallback : parse(raw)
  } catch {
    return fallback
  }
}

export const ui = $state({
  railWidth: storedRailWidth(),
  // one entry per panel id, filled in on first use by `panelState`
  panels: {},
  // names of the workflows whose group in the runs rail is shut
  collapsedRuns: storedCollapsed(),
  // what was asked for: 'system', 'light' or 'dark'
  themePref: stored(THEME_KEY, 'system', (r) => (r === 'light' || r === 'dark' ? r : 'system')),
  // what is actually showing: never 'system'. Everything that draws reads this
  theme: 'dark',
  // the agent last used to run a workflow, off any workflow tab
  lastAgent: stored(LAST_AGENT_KEY, '', (r) => r),
})

function remember(key, value) {
  try {
    localStorage.setItem(key, value)
  } catch {
    // a browser with storage denied still resizes; it just forgets on reload
  }
}

export function setRailWidth(w) {
  ui.railWidth = clampRail(w)
  remember(RAIL_KEY, String(ui.railWidth))
}

export function setLastAgent(name) {
  ui.lastAgent = name
  remember(LAST_AGENT_KEY, name)
}

/** One panel's remembered geometry, hydrated on first ask.
 *
 * `top` defaults per panel: the caller knows what is above the split -- a
 * graph and a file tree do not want the same room -- and the stored value
 * wins over it once there is one. */
export function panelState(id, topDefault = PANEL_TOP_DEFAULT) {
  if (!ui.panels[id]) {
    const num = (key, legacy, fallback, clamp) =>
      stored(key, stored(legacy, fallback, (r) => {
        const n = Number(r)
        return Number.isFinite(n) && n > 0 ? clamp(n) : fallback
      }), (r) => {
        const n = Number(r)
        return Number.isFinite(n) && n > 0 ? clamp(n) : fallback
      })
    ui.panels[id] = {
      width: num(panelKey(id, 'width'), LEGACY_KEYS.width, PANEL_DEFAULT, clampPanel),
      top: num(panelKey(id, 'top'), LEGACY_KEYS.top, clampPanelTop(topDefault), clampPanelTop),
      open: stored(
        panelKey(id, 'open'),
        stored(LEGACY_KEYS.open, true, (r) => r !== '0'),
        (r) => r !== '0',
      ),
    }
  }
  return ui.panels[id]
}

export function setPanelWidth(id, w) {
  const st = panelState(id)
  st.width = clampPanel(w)
  remember(panelKey(id, 'width'), String(st.width))
}

export function setPanelTop(id, h) {
  const st = panelState(id)
  st.top = clampPanelTop(h)
  remember(panelKey(id, 'top'), String(st.top))
}

export function setPanelOpen(id, open) {
  const st = panelState(id)
  st.open = !!open
  remember(panelKey(id, 'open'), st.open ? '1' : '0')
}

/** Shut or open one workflow's group in the runs rail. */
export function toggleRunGroup(workflow) {
  const next = new Set(ui.collapsedRuns)
  if (next.has(workflow)) next.delete(workflow)
  else next.add(workflow)
  ui.collapsedRuns = next
  remember(COLLAPSED_KEY, JSON.stringify([...next]))
}

/** Force one group open -- used when something inside it becomes the selection,
 *  so a run can never be selected and invisible at the same time. */
export function openRunGroup(workflow) {
  if (!ui.collapsedRuns.has(workflow)) return
  toggleRunGroup(workflow)
}

export function applyTheme() {
  ui.theme = ui.themePref === 'system' ? osTheme() : ui.themePref
  try {
    // one place decides which palette is showing, and it is an attribute rather
    // than a class so index.html can stamp it before any of this has loaded
    document.documentElement.setAttribute('data-theme', ui.theme)
  } catch {
    // no document (a test importing the module); the state is still right
  }
}

export function setTheme(pref) {
  ui.themePref = pref === 'light' || pref === 'dark' ? pref : 'system'
  if (ui.themePref === 'system') {
    try {
      localStorage.removeItem(THEME_KEY)
    } catch {
      /* storage denied; the choice lasts this session */
    }
  } else {
    remember(THEME_KEY, ui.themePref)
  }
  applyTheme()
}

/** Flip to the opposite of what is showing. Pins an explicit choice -- there is
 *  deliberately no third press that goes back to following the OS. */
export function toggleTheme() {
  setTheme(ui.theme === 'dark' ? 'light' : 'dark')
}

/** Track the OS for as long as nobody has picked. Called once, at startup. */
export function watchOsTheme() {
  try {
    const mq = window.matchMedia('(prefers-color-scheme: light)')
    mq.addEventListener('change', () => {
      if (ui.themePref === 'system') applyTheme()
    })
  } catch {
    /* no matchMedia: the theme is whatever it resolved to at load */
  }
}

// Kinds that stay up until something replaces or clears them. A failure is a
// thing to act on, and one that cleared itself while you were reading the log
// under it is a failure you have to reproduce to read again. Everything else --
// a "copied", a refusal you have already had the answer to -- is about a moment,
// and a banner about a moment that outlives it is read as a standing state.
const NOTICE_STICKY = new Set(['error', 'offline'])
const NOTICE_TTL_MS = 60_000

let noticeTimer = null

export function notify(message, kind = 'error') {
  clearTimeout(noticeTimer)
  noticeTimer = null
  app.notice = message ? { message, kind } : null
  if (app.notice && !NOTICE_STICKY.has(kind)) {
    noticeTimer = setTimeout(clearNotice, NOTICE_TTL_MS)
  }
}

export function clearNotice() {
  clearTimeout(noticeTimer)
  noticeTimer = null
  app.notice = null
}

// A "the server is not answering" banner is about a moment, not a state, and it
// used to outlive the moment: the dot went back to green on the next heartbeat
// while the red bar stayed until something else was clicked, so the page said
// both things at once. Only that one notice is cleared -- a refusal or a bug
// report is still worth reading after the connection comes back.
api.onrecover = () => {
  if (app.notice?.kind === 'offline') clearNotice()
}

// Wrap an action so a refusal reaches the user instead of the console.
//
// Clearing on the way in is right for an action -- pressing a button should not
// leave the last failure standing beside the new result. It is wrong for a
// *background* read: a view that fetches something on mount would otherwise
// wipe whatever the action that navigated there had just said. Those pass
// `quiet`, which keeps the standing notice and still reports their own failure.
export async function attempt(fn, { onSuccess, quiet = false } = {}) {
  if (!quiet) clearNotice()
  try {
    const out = await fn()
    onSuccess?.(out)
    return out
  } catch (e) {
    notify(e.message, e.kind ?? 'error')
    return null
  }
}

const archived = () => (app.showArchived ? '?archived=1' : '')

export async function loadProject() {
  app.project = await api.get('/project')
}

export async function loadSsh() {
  const body = await api.get('/ssh/hosts')
  app.hosts = body.hosts
  app.sshPath = body.path
}

export async function loadAgents() {
  app.agents = await api.get(`/agents${archived()}`)
}

export async function loadWorkflows() {
  app.workflows = await api.get(`/workflows${archived()}`)
}

/** Patch one row of the sidebar in place, for a change (a solve finishing)
 *  that alters only a few of its fields -- rather than a full `loadWorkflows`,
 *  which re-derives every row's run count from scratch to update the one that
 *  changed. Fields a solve cannot touch (`run_count`, `live_runs`, `display_name`,
 *  ...) are left as they are. */
export function patchWorkflowSummary(name, fields) {
  const i = app.workflows.findIndex((w) => w.name === name)
  if (i === -1) return
  app.workflows[i] = { ...app.workflows[i], ...fields }
}

// -- recently-viewed workflows: an LRU cache, capacity 8 ---------------------
//
// A `Map` gives LRU for free: delete-then-reinsert on a hit moves a key to the
// end, and the first key iterated is always the least recently used. This is a
// single-user local tool where the CLI can still write to a workflow between
// visits, so a hit is only ever shown immediately -- the caller still fires a
// real fetch behind it and overwrites both the live state and this entry once
// it resolves (stale-while-revalidate), rather than trusting the cache alone.

const WORKFLOW_CACHE_SIZE = 8
const workflowCache = new Map()

export function cachedWorkflow(name) {
  const hit = workflowCache.get(name)
  if (!hit) return null
  workflowCache.delete(name)
  workflowCache.set(name, hit)
  return hit
}

export function cacheWorkflow(name, snapshot) {
  workflowCache.delete(name)
  workflowCache.set(name, snapshot)
  while (workflowCache.size > WORKFLOW_CACHE_SIZE) {
    workflowCache.delete(workflowCache.keys().next().value)
  }
}

// -- per-step resource overrides --------------------------------------------
// cpus/memory/duration typed into the launch panel, keyed by workflow name --
// persisted server-side in `workflows/<name>/overrides.yml`, the same way the
// rest of a workflow is, so they survive a reload in any browser/profile. The
// panel keys each entry by transform name, not step order -- order is a
// position in the current plan, and a regenerate that adds or drops upstream
// steps renumbers everything after the change, which used to leave a stale
// numeric key silently reattached to whatever step now sits at that
// position. `WorkflowView.svelte`'s `overridePayload()` also drops any key
// naming no step in the current plan, so an entry from a since-removed step
// is dropped rather than sent under a name nothing matches.
//
// `setOverride` fires on every keystroke, so the network write is debounced
// per workflow rather than sent on every call -- the in-memory `overrides`
// state the UI reads from is updated synchronously by the caller regardless.
const OVERRIDE_SAVE_DEBOUNCE_MS = 600
const overrideSaveTimers = new Map()

export function saveOverrides(name, overrides) {
  clearTimeout(overrideSaveTimers.get(name))
  overrideSaveTimers.set(name, setTimeout(() => {
    overrideSaveTimers.delete(name)
    api.put(`/workflows/${name}/overrides`, { resource_overrides: overrides }).catch(() => {})
  }, OVERRIDE_SAVE_DEBOUNCE_MS))
}

export async function loadRuns() {
  app.runs = await api.get(`/runs${archived()}`)
}

// Both are the same vocabulary on every workflow page, so unlike the loaders
// above (called deliberately, on a section switch) these are called on every
// workflow mount and must not refetch just because the component remounted.
// The in-flight/resolved promise itself is the memo -- a bare `if (app.types.length)`
// guard would still fire twice for two workflows mounting back to back.
let typesPromise = null
let typeIndexPromise = null

export function loadTypes(force = false) {
  if (force || !typesPromise) {
    typesPromise = api.get('/project/types').then((v) => (app.types = v))
  }
  return typesPromise
}

export function loadTypeIndex(force = false) {
  if (force || !typeIndexPromise) {
    typeIndexPromise = api.get('/project/type-index' + (force ? '?refresh=1' : '')).then((v) => (app.index = v))
  }
  return typeIndexPromise
}

export async function refresh(section = app.section) {
  app.loading = true
  try {
    if (section === 'ssh') await loadSsh()
    if (section === 'agents') await loadAgents()
    if (section === 'workflows') await loadWorkflows()
    // the Runs rail needs agents to offer a launch target, and Workflows shows
    // run counts, so these two are always loaded together
    if (section === 'runs' || section === 'workflows') {
      await loadRuns()
      await loadAgents()
    }
  } catch (e) {
    notify(e.message, e.kind ?? 'error')
  } finally {
    app.loading = false
  }
}

export function select(section, id) {
  app.section = section
  app.selected[section] = id
  writeHash(section, id)
}

// The tabs themselves change the section without touching what is selected in
// it -- so this is the other half of `select`, not a call to it with the old
// id repeated back.
export function selectSection(section) {
  app.section = section
  writeHash(section, app.selected[section])
}

// An agent is made the same way a workflow is: on click, under a generated
// name, with a home named after it -- and you land on it with every field
// editable, the name included. There was a form in front of this, and it asked
// for exactly what the server would have defaulted, on a screen you could not
// deploy or ping from.
export async function createAgent() {
  const out = await attempt(() => api.post('/agents', {}))
  if (out) {
    // land on it first, reload the list behind you: the rail is a list of things
    // that already exist, and waiting for it to say so before opening the thing
    // you just made is a second round trip you watch
    select('agents', out.name)
    loadAgents()
  }
  return out
}

// A workflow is made under a name picked for you, and you land on it. The name
// is editable on the page you arrive at, so the only thing the new-workflow
// modal asks for is what to start from: `{ template }`, or nothing at all for
// the blank workflow that was always the whole of this. Held here rather than in
// the rail because creating one is a change to the list, not a thing the rail
// knows how to do.
export async function createWorkflow(body = {}) {
  const out = await attempt(() => api.post('/workflows', body))
  if (out) {
    select('workflows', out.name)
    loadWorkflows()
  }
  return out
}

// A fork is a copy of a workflow under a fresh identity, so it belongs on the
// row it copies -- beside the delete that is its opposite -- rather than inside
// the workflow it makes a second of. Same reasoning as createWorkflow for
// living here: both are changes to the list, and both land you on the result.
export async function forkWorkflow(name) {
  const out = await attempt(() => api.post(`/workflows/${name}/fork`, {}))
  if (out) {
    select('workflows', out.name)
    loadWorkflows()
  }
  return out
}

/** Whether a workflow still owns its directory name.
 *
 *  A workflow that has never been planned and has no runs does: nothing points
 *  at the directory yet, so the rename can move it. Once either exists, the
 *  directory is what the runs and the task cache are addressed by, and only the
 *  label on top of it is free to change.
 */
export function workflowRenameable(wf) {
  return !!wf && !wf.planned && !(wf.run_count || wf.runs?.length) && !wf.archived_at
}

/** Rename a workflow by whichever of its two names is still free to move.
 *
 *  Lives here rather than in the view because both the rail and the workflow
 *  page rename, and the rule above is the kind that goes wrong quietly if the
 *  two ever hold their own copy of it. Returns the name the caller should now
 *  address the workflow by, or `null` if nothing was written.
 */
export async function renameWorkflow(wf, next) {
  const from = wf.name
  if (workflowRenameable(wf)) {
    // the same PUT the recipe saves through: a workflow's name is a field of
    // it, and an id in the body that differs from the url is a rename
    const out = await attempt(async () => {
      const body = await api.put(`/workflows/${from}`, { name: next })
      await loadWorkflows()
      return body
    })
    if (!out) return null
    // the name is the route, so the open pane has to be moved onto the new one
    // -- but only if this was the row that was open. Renaming some other row
    // should not steal the selection.
    if (app.selected.workflows === from) select('workflows', out.name)
    return out.name
  }
  // Locked: the label moves and the directory does not. `display_name` rides in
  // `request.yml` beside the rest of the spec, an ordinary field `write_request`
  // already merges through, so this changes nothing a run or a cache key points
  // at -- and there is nothing to reselect.
  const ok = await attempt(async () => {
    await api.put(`/workflows/${from}`, { display_name: next })
    await loadWorkflows()
    return true
  })
  return ok ? from : null
}

/** Rename an agent. The reply is authoritative, not the name that was sent:
 *  typing one is also what stops an agent's name following its host, and the
 *  server settles the result (see `update_agent`). */
export async function renameAgent(agent, next) {
  const from = agent.name
  const out = await attempt(async () => {
    const body = await api.put(`/agents/${from}`, { name: next })
    await loadAgents()
    return body
  })
  if (!out?.name) return null
  if (app.selected.agents === from) select('agents', out.name)
  return out.name
}

export const selection = () => app.selected[app.section]

export function runId(run) {
  return `${run.workflow}/${run.name}`
}
