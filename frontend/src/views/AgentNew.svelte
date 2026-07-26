<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, loadAgents, select } from '../lib/state.svelte.js'
  import { agentPayload, blankForm, homeUri } from '../lib/agentform.js'
  import AgentFields from './AgentFields.svelte'

  // Opens on a working agent rather than an empty form: a generated name and a
  // home you would probably have typed anyway. Everything is editable, so the
  // defaults are a starting point, not a decision made for you.
  let form = $state(blankForm())
  let ready = $state(false)

  $effect(() => {
    attempt(async () => {
      const d = await api.get('/defaults/agent')
      form = blankForm({ name: d.name, path: d.home, runtime: d.runtime })
      ready = true
    })
  })

  let home = $derived(homeUri(form))

  async function create() {
    const out = await attempt(async () => {
      const body = await api.post('/agents', agentPayload(form))
      await loadAgents()
      return body
    })
    if (out) select('agents', out.name)
  }
</script>

<div class="col" style="gap:14px; max-width:720px">
  <div class="spread">
    <h1>new agent</h1>
    <button class="primary" onclick={create} disabled={!ready || !form.name.trim() || !home}>
      create
    </button>
  </div>

  <p class="small muted" style="max-width:70ch">
    An agent is a place metasmith can run work: a directory on this machine or on a
    host you reach over ssh. Nothing is installed there until you deploy it.
  </p>

  <AgentFields bind:form />

  <div class="row small muted">
    <span>home</span>
    <span class="mono">{home || '—'}</span>
  </div>
</div>
