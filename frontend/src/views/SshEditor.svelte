<script>
  import { api } from '../lib/api.js'
  import { attempt, loadSsh } from '../lib/state.svelte.js'

  // The whole config file, with metasmith's block editable and the rest shown
  // read-only. Ownership is legible from the layout, so it needs no legend.
  let cfg = $state({ path: '', before: '', managed: '', after: '', exists: false })
  let managed = $state('')
  let saved = $state(false)

  async function load() {
    const body = await api.get('/ssh/config')
    cfg = body
    managed = body.managed
  }

  $effect(() => {
    load()
  })

  async function save() {
    await attempt(async () => {
      const body = await api.put('/ssh/config', { managed })
      cfg = { ...cfg, ...body }
      managed = body.managed
      saved = true
      setTimeout(() => (saved = false), 1500)
      await loadSsh()
    })
  }
</script>

<div class="col" style="gap:12px">
  <div class="spread">
    <h1>ssh config</h1>
    <span class="small muted mono">{cfg.path}</span>
  </div>

  {#if !cfg.exists}
    <p class="small muted">This file does not exist yet; adding a host will create it.</p>
  {/if}

  {#if cfg.before.trim()}
    <div class="col">
      <h3>above the block — yours</h3>
      <pre class="log ro">{cfg.before}</pre>
    </div>
  {/if}

  <div class="col">
    <div class="spread">
      <h3>managed by metasmith</h3>
      <div class="row">
        {#if saved}<span class="tag ok">saved</span>{/if}
        <button class="primary" onclick={save}>save</button>
      </div>
    </div>
    <textarea bind:value={managed} rows="14" spellcheck="false"></textarea>
    <p class="small muted">
      This block is written first in the file on purpose. ssh takes the first
      value it finds for each keyword, so a <span class="mono">Host *</span> above it
      would quietly set the user or identity file for anything defined here.
    </p>
  </div>

  {#if cfg.after.trim()}
    <div class="col">
      <h3>below the block — yours</h3>
      <pre class="log ro">{cfg.after}</pre>
    </div>
  {/if}
</div>

<style>
  .ro { opacity: 0.7; max-height: 240px; }
</style>
