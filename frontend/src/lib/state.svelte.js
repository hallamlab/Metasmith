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
