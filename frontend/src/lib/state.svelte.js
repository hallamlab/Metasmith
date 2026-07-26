import { api } from './api.js'

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

const PANEL_KEY = 'metasmith.panelWidth'
const PANEL_OPEN_KEY = 'metasmith.panelOpen'

export function clampPanel(w) {
  return Math.min(PANEL_MAX, Math.max(PANEL_MIN, Math.round(w)))
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
export async function attempt(fn, { onSuccess } = {}) {
  clearNotice()
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

export const selection = () => app.selected[app.section]

export function runId(run) {
  return `${run.workflow}/${run.name}`
}
