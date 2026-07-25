<script>
  import { api } from '../lib/api.js'
  import { attempt, loadSsh } from '../lib/state.svelte.js'
  import ConfigEditor from '../components/ConfigEditor.svelte'

  // The whole config file in two boxes. Both are editable -- refusing to edit
  // the user's half only sends them to another editor for a file this page is
  // already showing -- but they are kept apart because the consequences differ:
  // the managed half is rewritten by the host forms and anything you add by
  // hand there lives alongside generated entries, while the native half is
  // yours and metasmith only ever reads it.
  let cfg = $state({ path: '', managed: '', native: '', exists: false })
  let managed = $state('')
  let native = $state('')
  let saved = $state(false)
  let loaded = $state(false)

  let dirty = $derived(loaded && (managed !== cfg.managed || native !== cfg.native))

  function adopt(body) {
    cfg = body
    managed = body.managed
    native = body.native
    loaded = true
  }

  $effect(() => {
    attempt(async () => adopt(await api.get('/ssh/config')))
  })

  async function save() {
    await attempt(async () => {
      adopt(await api.put('/ssh/config', { managed, native }))
      saved = true
      setTimeout(() => (saved = false), 1500)
      await loadSsh()
    })
  }

  function revert() {
    managed = cfg.managed
    native = cfg.native
  }
</script>

<div class="col" style="gap:14px">
  <div class="spread">
    <h1>ssh config</h1>
    <div class="row">
      <span class="small muted mono truncate">{cfg.path}</span>
      {#if saved}<span class="tag ok">saved</span>{/if}
      <button class="small" onclick={revert} disabled={!dirty}>revert</button>
      <button class="primary" onclick={save} disabled={!dirty}>save</button>
    </div>
  </div>

  {#if !cfg.exists}
    <p class="small muted">This file does not exist yet; saving will create it.</p>
  {/if}

  <div class="col" style="gap:6px">
    <div class="spread">
      <h3>managed — metasmith</h3>
      <span class="small muted">rewritten by the host forms</span>
    </div>
    <ConfigEditor bind:value={managed} rows={10} resizable={false} label="the metasmith block" />
    <p class="small muted">
      This block is written first in the file on purpose. ssh takes the first
      value it finds for each keyword, so a <span class="mono">Host *</span> above it
      would quietly set the user or identity file for anything defined here — which is
      why the markers themselves are not yours to move.
    </p>
  </div>

  <div class="divider"></div>

  <div class="col" style="gap:6px">
    <div class="spread">
      <h3>native — yours</h3>
      <span class="small muted">everything outside the block</span>
    </div>
    <ConfigEditor bind:value={native} rows={18} label="the rest of your ssh config" />
  </div>
</div>

<style>
  .divider { border-top: 1px solid var(--line); margin: 2px 0; }
</style>
