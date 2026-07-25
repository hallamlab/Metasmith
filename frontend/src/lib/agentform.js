// Translating between the agent form and the single `home` URI the ops layer
// stores. The canonical remote spelling is `ssh://host:path` -- what SshSource
// renders and what Source.Parse reads back -- so both directions use it.

export const DEFAULT_HOME = '~/msm_home'

export function blankForm(overrides = {}) {
  return {
    name: '',
    kind: 'local',
    host: '',
    path: DEFAULT_HOME,
    runtime: 'APPTAINER',
    container: '',
    setup: '',
    ...overrides,
  }
}

export function homeUri(form) {
  const path = (form.path ?? '').trim()
  if (form.kind !== 'ssh') return path
  return form.host && path ? `ssh://${form.host}:${path}` : ''
}

export function formFromAgent(agent) {
  const home = agent.home ?? ''
  const remote = home.startsWith('ssh://')
  // split on the first ':' after the scheme; a path may contain one, a host may not
  const rest = remote ? home.slice('ssh://'.length) : ''
  const cut = remote ? rest.indexOf(':') : -1
  return {
    name: agent.name ?? '',
    kind: remote ? 'ssh' : 'local',
    host: cut >= 0 ? rest.slice(0, cut) : '',
    path: remote ? (cut >= 0 ? rest.slice(cut + 1) : rest) : home,
    runtime: agent.runtime ?? 'APPTAINER',
    container: agent.container ?? '',
    setup: (agent.setup_commands ?? []).join('\n'),
  }
}

export function agentPayload(form) {
  return {
    name: form.name,
    home: homeUri(form),
    runtime: form.runtime,
    container: form.container.trim() || null,
    setup_commands: form.setup.split('\n').map((s) => s.trim()).filter(Boolean),
  }
}
