import { api } from './api.svelte.js'

export const SECTIONS = [
  // in setup order: you need a host before an agent, an agent before a run
  { id: 'ssh', label: 'SSH' },
  { id: 'agents', label: 'Agents' },
  { id: 'workflows', label: 'Workflows' },
  { id: 'runs', label: 'Runs' },
]

export const app = $state({
  section: 'ssh',
  // one remembered selection per section, so switching tabs does not lose your place
  selected: { ssh: null, agents: null, workflows: null, runs: null },
  showArchived: false,
  project: null,
  hosts: [],
  sshPath: '',
  agents: [],
  workflows: [],
  runs: [],
  notice: null,
  loading: false,
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

// -- the type inspector's width --------------------------------------------
// Same furniture on the other edge, and the same reasoning: one width, kept
// across workflows. Whether it is open is remembered too -- someone who closed
// it does not want it back on the next workflow they open.

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

const PANEL_KEY = 'metasmith.panelWidth'
const PANEL_OPEN_KEY = 'metasmith.panelOpen'
const PANEL_TOP_KEY = 'metasmith.panelTop'

export function clampPanel(w) {
  return Math.min(PANEL_MAX, Math.max(PANEL_MIN, Math.round(w)))
}

export function clampPanelTop(h) {
  return Math.min(PANEL_TOP_MAX, Math.max(PANEL_TOP_MIN, Math.round(h)))
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
  panelWidth: stored(PANEL_KEY, PANEL_DEFAULT, (r) => {
    const n = Number(r)
    return Number.isFinite(n) && n > 0 ? clampPanel(n) : PANEL_DEFAULT
  }),
  panelOpen: stored(PANEL_OPEN_KEY, true, (r) => r !== '0'),
  panelTop: stored(PANEL_TOP_KEY, PANEL_TOP_DEFAULT, (r) => {
    const n = Number(r)
    return Number.isFinite(n) && n > 0 ? clampPanelTop(n) : PANEL_TOP_DEFAULT
  }),
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

export function setPanelWidth(w) {
  ui.panelWidth = clampPanel(w)
  remember(PANEL_KEY, String(ui.panelWidth))
}

export function setPanelTop(h) {
  ui.panelTop = clampPanelTop(h)
  remember(PANEL_TOP_KEY, String(ui.panelTop))
}

export function setPanelOpen(open) {
  ui.panelOpen = !!open
  remember(PANEL_OPEN_KEY, ui.panelOpen ? '1' : '0')
}

export function notify(message, kind = 'error') {
  app.notice = message ? { message, kind } : null
}

export function clearNotice() {
  app.notice = null
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

export async function loadRuns() {
  app.runs = await api.get(`/runs${archived()}`)
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
}

// An agent is made the same way a workflow is: on click, under a generated
// name, with a home named after it -- and you land on it with every field
// editable, the name included. There was a form in front of this, and it asked
// for exactly what the server would have defaulted, on a screen you could not
// deploy or ping from.
export async function createAgent() {
  const out = await attempt(async () => {
    const body = await api.post('/agents', {})
    await loadAgents()
    return body
  })
  if (out) select('agents', out.name)
  return out
}

// A workflow is made the moment it is asked for, under a name picked for you,
// and you land on it. There is nothing to fill in first: a workflow starts empty
// whatever it is called, and the one field a create form had -- the name -- is
// editable on the page you arrive at. Held here rather than in the rail because
// creating one is a change to the list, not a thing the rail knows how to do.
export async function createWorkflow() {
  const out = await attempt(async () => {
    const body = await api.post('/workflows', {})
    await loadWorkflows()
    return body
  })
  if (out) select('workflows', out.name)
  return out
}

// A fork is a copy of a workflow under a fresh identity, so it belongs on the
// row it copies -- beside the delete that is its opposite -- rather than inside
// the workflow it makes a second of. Same reasoning as createWorkflow for
// living here: both are changes to the list, and both land you on the result.
export async function forkWorkflow(name) {
  const out = await attempt(async () => {
    const body = await api.post(`/workflows/${name}/fork`, {})
    await loadWorkflows()
    return body
  })
  if (out) select('workflows', out.name)
  return out
}

export const selection = () => app.selected[app.section]

export function runId(run) {
  return `${run.workflow}/${run.name}`
}
