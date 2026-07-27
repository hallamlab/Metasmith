// Translating between the agent form and the single `home` URI the ops layer
// stores. The canonical remote spelling is `ssh://host:path` -- what SshSource
// renders and what Source.Parse reads back -- so both directions use it.

// A home is named after the agent, because a host with three agents on it
// otherwise has three directories called the same thing. Kept in step with
// `api.default_agent_home`, which is what the field offers as its placeholder.
//
// Whether a *stored* home is still the default one is not decided here:
// `Source.Parse` expands `~` for a local home, so what comes back from a save is
// `/home/you/msm.<name>` where `~/msm.<name>` went in, and only the server knows
// what `~` was. It answers with `home_is_default` on the agent.
export const HOME_PREFIX = '~/msm.'

export function defaultHome(name) {
  return `${HOME_PREFIX}${(name ?? '').trim()}`
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
//
// An empty path is the default one -- what the field's placeholder says it is.
// The default is made out of the name, so a home left empty keeps following a
// rename, and the box never holds a string the page put there for you to delete.
export function homeUri(form) {
  const path = (form.path ?? '').trim() || defaultHome(form.name)
  if (form.kind !== 'ssh') return path
  return `ssh://${(form.host ?? '').trim()}:${path}`
}

export function formFromAgent(agent) {
  const home = agent.home ?? ''
  const remote = home.startsWith('ssh://')
  // split on the first ':' after the scheme; a path may contain one, a host may not
  const rest = remote ? home.slice('ssh://'.length) : ''
  const cut = remote ? rest.indexOf(':') : -1
  const path = remote ? (cut >= 0 ? rest.slice(cut + 1) : rest) : home
  const name = agent.name ?? ''
  return {
    name,
    kind: remote ? 'ssh' : 'local',
    host: cut >= 0 ? rest.slice(0, cut) : '',
    // The default one is spelled as the empty box the placeholder describes, so
    // that what is drawn is "still whatever the name makes it" rather than a
    // path that happens to agree with the name today. The server decides that
    // -- it is the side that knows what `~` expanded to -- and anything it does
    // not vouch for stays written out, because an emptied box saves as the
    // default and that would be someone else's path silently replaced.
    path: agent.home_is_default ? '' : path,
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
  // a blank box is the default home, and the default is made out of the name --
  // so the only way to have no home at all is to have no name either
  if (!(form.path ?? '').trim() && !form.name?.trim()) out.push('no home directory')
  if (form.kind === 'ssh' && !(form.host ?? '').trim()) out.push('no host chosen')
  return out
}
