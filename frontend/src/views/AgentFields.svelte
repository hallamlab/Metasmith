<script>
  import { app } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'

  // The one field set an agent has, shared by the new-agent view and the detail
  // view so that creating an agent and editing one are the same act rather than
  // two screens that drifted apart. `form` is bound by the parent.
  let { form = $bindable(), nameEditable = true } = $props()
</script>

<div class="grid">
  <Field label="name">
    <input bind:value={form.name} disabled={!nameEditable} />
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
      ? 'a path on that host — on a cluster, prefer scratch over a home quota'
      : 'a path on this machine'}
  >
    <input bind:value={form.path} placeholder="~/msm_home" />
  </Field>

  <Field label="container runtime">
    <select bind:value={form.runtime}>
      <option value="APPTAINER">apptainer</option>
      <option value="DOCKER">docker</option>
    </select>
  </Field>

  <Field label="container image" hint="leave empty for the image matching this version">
    <input class="mono" bind:value={form.container} />
  </Field>

  <Field
    label="setup commands"
    hint="run before anything else on that host — one per line, e.g. module load"
  >
    <textarea bind:value={form.setup} rows="3" spellcheck="false"></textarea>
  </Field>
</div>

<style>
  /* One field per row, in the order you fill them, the same as the ssh forms */
  .grid { display: flex; flex-direction: column; gap: 10px; }
  .grid :global(textarea) { font-family: var(--mono); }
</style>
