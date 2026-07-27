<script>
  import { app } from '../lib/state.svelte.js'
  import { defaultHome } from '../lib/agentform.js'
  import Field from '../components/Field.svelte'
  import ConfigEditor from '../components/ConfigEditor.svelte'

  // The one field set an agent has. `form` is bound by the parent; `runtimes`
  // comes from the server, which reads it off the Runtime enum, so a runtime
  // added there appears here with nothing to change. `realPath` is what the
  // agent last reported the home resolves to on its host, and is only worth a
  // line when it is not simply the path above it.
  let { form = $bindable(), runtimes = ['APPTAINER', 'DOCKER', 'MAMBA'], realPath = null } = $props()

  const RUNTIME_LABELS = {
    APPTAINER: 'apptainer',
    DOCKER: 'docker',
    MAMBA: 'mamba — no container; tools run in conda environments',
  }

  const WHERE = [
    { id: 'local', label: 'this machine' },
    { id: 'ssh', label: 'a remote host' },
  ]

  // The name is the heading (`EditableName` in AgentView), not a field here:
  // the page is already titled with it, and a labelled box repeating it is the
  // same word twice.
</script>

<div class="grid">
  <!-- Two tabs over one box, and the box holds exactly what the tabs decide:
       nothing for a local agent, the host for a remote one. A `where it lives`
       dropdown said the same thing while hiding what it controlled -- the host
       field appeared and disappeared somewhere below it. -->
  <!-- not a `Field`: that is a <label>, which belongs to exactly one control,
       and this row is a tablist over a box that sometimes holds another -->
  <div class="field">
    <span class="small muted">where it lives</span>
    <div class="tabs" role="tablist" aria-label="where this agent lives">
      {#each WHERE as w}
        <button
          class="tab"
          class:on={form.kind === w.id}
          role="tab"
          aria-selected={form.kind === w.id}
          onclick={() => (form.kind = w.id)}
        >{w.label}</button>
      {/each}
    </div>
    <div class="panel" role="tabpanel">
      {#if form.kind === 'ssh'}
        <select bind:value={form.host} aria-label="host">
          <option value="">choose a host…</option>
          {#each app.hosts as h}
            <option value={h.alias}>{h.alias}</option>
          {/each}
        </select>
        <p class="small muted">an alias from the SSH section</p>
      {:else}
        <p class="small muted">the agent runs here</p>
      {/if}
    </div>
  </div>

  <!-- Empty means the default, which is what the placeholder says it is. It is
       left empty rather than pre-filled: typing the default into the box makes
       a name change stop moving the home, since a path someone has touched is
       theirs from then on. -->
  <Field
    label="home directory"
    hint={realPath && realPath !== form.path
      ? `resolves to ${realPath}`
      : form.kind === 'ssh'
        ? 'a path on that host — on a cluster, prefer scratch over a home quota'
        : 'a path on this machine'}
  >
    <input class="mono" bind:value={form.path} placeholder={defaultHome(form.name)} />
  </Field>

  <Field label="runtime" hint="how a tool is provided on that host">
    <select bind:value={form.runtime}>
      {#each runtimes as r}
        <option value={r}>{RUNTIME_LABELS[r] ?? r.toLowerCase()}</option>
      {/each}
    </select>
  </Field>

  <Field
    label="setup commands"
    hint="run before anything else on that host — one per line, e.g. module load"
  >
    <ConfigEditor
      bind:value={form.setup}
      language="bash"
      rows={6}
      resizable={false}
      label="setup commands"
    />
  </Field>
</div>

<style>
  /* One field per row, in the order you fill them, the same as the ssh forms */
  .grid { display: flex; flex-direction: column; gap: 10px; }
  /* the same shape `Field` renders, for the one row that cannot be a label */
  .field { display: flex; flex-direction: column; gap: 3px; }

  .tabs { display: flex; gap: 2px; }
  .tab {
    background: none;
    border: 1px solid transparent;
    border-bottom: none;
    border-radius: var(--radius) var(--radius) 0 0;
    padding: 6px 14px;
    color: var(--muted);
    /* sits on the panel's top border, so the selected tab can erase it */
    margin-bottom: -1px;
  }
  .tab:hover { color: var(--text); }
  .tab.on {
    color: var(--text);
    background: var(--panel-2);
    border-color: var(--line);
    border-bottom: 1px solid var(--panel-2);
  }
  /* the box the tabs control: whatever is in here is what the choice decides */
  .panel {
    border: 1px solid var(--line);
    border-radius: 0 var(--radius) var(--radius) var(--radius);
    background: var(--panel-2);
    padding: 10px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .panel p { margin: 0; }
</style>
