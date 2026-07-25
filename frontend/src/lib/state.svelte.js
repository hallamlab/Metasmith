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

export const ui = $state({ railWidth: storedRailWidth() })

export function setRailWidth(w) {
  ui.railWidth = clampRail(w)
  try {
    localStorage.setItem(RAIL_KEY, String(ui.railWidth))
  } catch {
    // a browser with storage denied still resizes; it just forgets on reload
  }
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

export const selection = () => app.selected[app.section]

export function runId(run) {
  return `${run.workflow}/${run.name}`
}
