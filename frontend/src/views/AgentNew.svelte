<script>
  import { api } from '../lib/api.js'
  import { app, attempt, loadAgents, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'

  let form = $state({
    name: '',
    kind: 'local',
    host: '',
    path: '',
    runtime: 'APPTAINER',
    setup: '',
  })

  let home = $derived(
    form.kind === 'ssh'
      ? form.host && form.path
        ? `ssh://${form.host}:${form.path}`
        : ''
      : form.path,
  )

  async function create() {
    const out = await attempt(async () => {
      const body = await api.post('/agents', {
        name: form.name,
        home,
        runtime: form.runtime,
        setup_commands: form.setup.split('\n').map((s) => s.trim()).filter(Boolean),
      })
      await loadAgents()
      return body
    })
    if (out) select('agents', out.name)
  }
</script>

<div class="col" style="gap:14px; max-width:600px">
  <h1>new agent</h1>
  <div class="card col" style="gap:10px">
    <Field label="name">
      <input bind:value={form.name} placeholder="smith" />
    </Field>

    <Field label="where it lives">
      <select bind:value={form.kind}>
        <option value="local">this machine</option>
        <option value="ssh">a remote host over ssh</option>
      </select>
    </Field>

    {#if form.kind === 'ssh'}
      <Field label="host" hint="an alias from the SSH section">
        <select bind:value={form.host}>
          <option value="">choose a host…</option>
          {#each app.hosts as h}
            <option value={h.alias}>{h.alias}</option>
          {/each}
        </select>
      </Field>
    {/if}

    <Field
      label="home directory"
      hint={form.kind === 'ssh'
        ? 'an absolute path on that host; on a cluster put it on scratch, not on a home quota'
        : 'a path on this machine'}
    >
      <input bind:value={form.path} placeholder={form.kind === 'ssh' ? '/scratch/you/msm_home' : './msm_home'} />
    </Field>

    <Field label="container runtime">
      <select bind:value={form.runtime}>
        <option value="APPTAINER">apptainer</option>
        <option value="DOCKER">docker</option>
      </select>
    </Field>

    <Field
      label="setup commands"
      hint="run before anything else on that host — one per line, e.g. module load"
    >
      <textarea bind:value={form.setup} rows="3" spellcheck="false"></textarea>
    </Field>

    <div class="row">
      <button class="primary" onclick={create} disabled={!form.name || !home}>create</button>
      {#if home}<span class="small muted mono truncate">{home}</span>{/if}
    </div>
  </div>
</div>
