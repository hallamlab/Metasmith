<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, refresh, select } from '../lib/state.svelte.js'
  import Modal from './Modal.svelte'

  // Taking one in. The payload is read on the server -- it is the side that
  // knows which libraries, types and hosts exist here -- and read *twice*: once
  // to say what would be created, and again to create it. The preview is the
  // point of the dialog. Names resolve best-effort, so an import can land with
  // parts missing, and finding that out afterwards is worse than being told.
  let { onclose } = $props()

  let payload = $state('')
  let preview = $state(null)
  let checking = $state(false)
  let importing = $state(false)
  let error = $state(null)

  // A pasted string is read as soon as it is there; nothing is written until
  // the import button is pressed.
  $effect(() => {
    const text = payload.trim()
    preview = null
    error = null
    if (!text) return
    checking = true
    api
      .post('/share/preview', { payload: text })
      .then((out) => (preview = out))
      .catch((e) => (error = e.message))
      .finally(() => (checking = false))
  })

  async function commit() {
    importing = true
    const out = await attempt(() => api.post('/share/import', { payload: payload.trim() }))
    importing = false
    if (!out) return
    const section = out.kind === 'ssh_host' ? 'ssh' : out.kind === 'agent' ? 'agents' : 'workflows'
    await refresh(section)
    select(section, out.name)
    onclose?.()
  }
</script>

<Modal title="import" subtitle="paste a string somebody shared with you" {onclose}>
  <label class="col small">
    <span class="muted">the string</span>
    <!-- svelte-ignore a11y_autofocus -->
    <textarea class="mono payload" rows="4" autofocus bind:value={payload}></textarea>
  </label>

  <div class="out">
    {#if error}
      <p class="small bad">{error}</p>
    {:else if checking}
      <p class="small muted">reading…</p>
    {:else if preview}
      <p class="small">
        <span class="tag">{preview.kind.replace('_', ' ')}</span>
        <strong>{preview.name}</strong>
      </p>
      <dl class="small">
        {#each Object.entries(preview.creates ?? {}) as [k, v] (k)}
          <div class="pair">
            <dt class="muted">{k.replace(/_/g, ' ')}</dt>
            <dd class="mono truncate">{Array.isArray(v) ? v.join(', ') || '—' : (v ?? '—')}</dd>
          </div>
        {/each}
      </dl>
      {#each preview.notes ?? [] as note (note)}
        <p class="small note" class:bad={preview.blocked}>{note}</p>
      {/each}
    {:else}
      <p class="small muted">nothing pasted yet</p>
    {/if}
  </div>

  {#snippet footer()}
    <button onclick={() => onclose?.()}>cancel</button>
    <button
      class="primary"
      disabled={!preview || preview.blocked || importing}
      onclick={commit}>{importing ? 'importing…' : 'import'}</button
    >
  {/snippet}
</Modal>

<style>
  .payload {
    word-break: break-all;
    resize: vertical;
  }
  .out {
    min-height: 140px;
    padding: 10px;
    background: var(--sunken);
    border: 1px solid var(--line);
    border-radius: var(--radius);
  }
  .pair {
    display: flex;
    gap: 8px;
  }
  dt {
    min-width: 130px;
  }
  dd {
    margin: 0;
    min-width: 0;
  }
  .note {
    margin: 6px 0 0;
    color: var(--warn, var(--muted));
  }
  .bad {
    color: var(--bad);
  }
</style>
