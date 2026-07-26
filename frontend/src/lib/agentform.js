// Translating between the agent form and the single `home` URI the ops layer
// stores. The canonical remote spelling is `ssh://host:path` -- what SshSource
// renders and what Source.Parse reads back -- so both directions use it.

// A home is named after the agent, because a host with three agents on it
// otherwise has three directories called the same thing. Kept in step with
// `api.default_agent_home`; the server sends its own spelling in
// `/defaults/agent` and this is what recognises "still the default".
export const HOME_PREFIX = '~/msm.'

export function defaultHome(name) {
  return `${HOME_PREFIX}${(name ?? '').trim()}`
}

// Whether a path is still the one the name would have made. `Source.Parse`
// expands `~` for a local home, so what comes back from a save is
// `/home/you/msm.<name>` where `~/msm.<name>` went in -- comparing against the
// literal default alone would mean the path stops following the name the
// moment the agent is saved once, which is immediately.
export function isDefaultHome(path, name) {
  const tail = `msm.${(name ?? '').trim()}`
  const p = (path ?? '').trim()
  return p === `~/${tail}` || p.endsWith(`/${tail}`)
}

export function blankForm(overrides = {}) {
  return {
    name: '',
    kind: 'local',
    host: '',
    path: '',
    runtime: 'APPTAINER',
    // not drawn anywhere: the image is a developer's field, and pinning one is
    // done from the CLI. It is carried so that saving the form does not erase
    // a value someone deliberately set -- an update sends the whole object.
    container: '',
    setup: '',
    ...overrides,
  }
}

// A remote agent with no host chosen still has a home: `ssh://:<path>`. It
// round-trips, it reads as unfinished rather than as local, and the server
// reports it as a problem. Refusing to spell it would mean an agent cannot be
// saved between "this one is going on a cluster" and "the cluster is set up",
// which is a gap of days.
export function homeUri(form) {
  const path = (form.path ?? '').trim()
  if (form.kind !== 'ssh') return path
  return `ssh://${(form.host ?? '').trim()}:${path}`
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
    container: form.container?.trim() || null,
    // blank lines are dropped, but a line that is only a comment is not: the
    // shebang the box starts with is one, and so is every note left beside a
    // module load
    setup_commands: form.setup.split('\n').map((s) => s.trimEnd()).filter((s) => s.trim()),
  }
}

// What the page can say for itself, before the server has been asked. The
// server's own `problems` list is the authority -- it can see the ssh config --
// so this is only what would make the save itself meaningless.
export function formProblems(form) {
  const out = []
  if (!form.name?.trim()) out.push('no name')
  if (!(form.path ?? '').trim()) out.push('no home directory')
  if (form.kind === 'ssh' && !(form.host ?? '').trim()) out.push('no host chosen')
  return out
}
