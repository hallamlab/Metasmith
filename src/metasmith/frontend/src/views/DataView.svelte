<script>
  // What one agent's store holds, read as data instances rather than as cache.
  //
  // Every action here is a call into `ops/`: the list, the grouping, the tags,
  // the import. Nothing is computed on this side -- the backend answers the
  // four questions a store can be asked (what category, which run, when it
  // arrived, what you called it) and this draws the answer.
  import { api } from '../lib/api.svelte.js'
  import { attempt } from '../lib/state.svelte.js'
  import Ago from '../components/Ago.svelte'
  import CopyButton from '../components/CopyButton.svelte'
  import Field from '../components/Field.svelte'

  let { name } = $props()

  const GROUPINGS = ['run', 'origin', 'dtype', 'tag', 'day']
  const SORTS = ['created_at', 'last_hit_at', 'size_bytes', 'hit_count', 'path']

  let groupBy = $state('run')
  let origin = $state('')
  let tag = $state('')
  let sortBy = $state('created_at')
  let store = $state(null)
  let error = $state(null)
  let loading = $state(false)

  let importing = $state(false)
  let form = $state({ path: '', dtype: '', name: '', tags: '' })
  let editing = $state(null)
  let tagDraft = $state('')

  async function load() {
    loading = true
    error = null
    const q = new URLSearchParams({ group_by: groupBy, sort_by: sortBy })
    if (origin) q.set('origin', origin)
    if (tag) q.set('tag', tag)
    try {
      store = await api.get(`/agents/${encodeURIComponent(name)}/store?${q}`)
    } catch (e) {
      store = null
      error = e.message
    } finally {
      loading = false
    }
  }

  $effect(() => {
    // re-reads whenever the question changes; `name` included, so switching
    // agents in the rail switches stores
    name; groupBy; origin; tag; sortBy
    load()
  })

  let groups = $derived(
    store?.groups
      ? Object.entries(store.groups).sort((a, b) => a[0].localeCompare(b[0]))
      : [],
  )

  let total = $derived(store?.entries?.length ?? 0)

  function human(n) {
    if (!n) return '0 B'
    const units = ['B', 'KB', 'MB', 'GB', 'TB']
    let i = 0
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1 }
    return `${n < 10 && i ? n.toFixed(1) : Math.round(n)} ${units[i]}`
  }

  async function submitImport() {
    const out = await attempt(() =>
      api.post(`/agents/${encodeURIComponent(name)}/store/import`, {
        path: form.path.trim(),
        dtype: form.dtype.trim(),
        name: form.name.trim() || null,
        tags: form.tags.split(',').map((s) => s.trim()).filter(Boolean),
      }),
    )
    if (out) {
      importing = false
      form = { path: '', dtype: '', name: '', tags: '' }
      await load()
    }
  }

  async function saveTags(row) {
    const out = await attempt(() =>
      api.post(
        `/agents/${encodeURIComponent(name)}/store/entries/${row.key}/tags`,
        { tags: tagDraft.split(',').map((s) => s.trim()).filter(Boolean), replace: true },
      ),
    )
    if (out) {
      editing = null
      await load()
    }
  }

  async function forget(row) {
    const out = await attempt(() =>
      api.del(`/agents/${encodeURIComponent(name)}/store/entries/${row.key}?delete=1`),
    )
    if (out) await load()
  }
</script>

<div class="pane">
  <header class="bar">
    <div class="grow">
      <h2>{name}</h2>
      <div class="small muted">
        {#if store}
          {total} instance(s) · <span class="mono">{store.cache_root}</span>
          <CopyButton text={store.cache_root} label="copy the store path" />
        {/if}
      </div>
    </div>
    <button class="small" onclick={() => (importing = !importing)}>
      {importing ? 'cancel' : '+ import'}
    </button>
  </header>

  {#if importing}
    <!-- The same three things the CLI asks for, because it is the same call.
         Nothing is copied or read: what is registered is the declaration. -->
    <form class="import" onsubmit={(e) => { e.preventDefault(); submitImport() }}>
      <Field label="path" hint="the file or folder, left where it is">
        <input bind:value={form.path} placeholder="/data/refs/kofam" required />
      </Field>
      <Field label="type" hint="what it is; nothing here opens the data to check">
        <input bind:value={form.dtype} placeholder="ns::type" required />
      </Field>
      <Field label="name" hint="what the identity is minted from (default: the path)">
        <input bind:value={form.name} placeholder="optional" />
      </Field>
      <Field label="tags" hint="comma separated">
        <input bind:value={form.tags} placeholder="refs, v2" />
      </Field>
      <button class="small" type="submit">import</button>
    </form>
  {/if}

  <div class="controls small">
    <label>group
      <select bind:value={groupBy}>
        {#each GROUPINGS as g}<option value={g}>{g}</option>{/each}
      </select>
    </label>
    <label>origin
      <select bind:value={origin}>
        <option value="">all</option>
        <option value="lineage">products</option>
        <option value="imported">imports</option>
      </select>
    </label>
    <label>tag <input bind:value={tag} placeholder="any" size="10" /></label>
    <label>sort
      <select bind:value={sortBy}>
        {#each SORTS as s}<option value={s}>{s}</option>{/each}
      </select>
    </label>
  </div>

  {#if error}
    <p class="muted">{error}</p>
  {:else if loading && !store}
    <p class="muted small">reading the store…</p>
  {:else if !total}
    <p class="muted small">
      nothing stored yet. A run's products land here, and so does anything you
      import.
    </p>
  {:else}
    {#each groups as [label, rows]}
      <section>
        <h3 class="small">
          <!-- A run key is case-sensitive and gets typed back into
               `cache list --run`, so the global h3 uppercase is wrong here. -->
          <span class="label mono">{label}</span>
          <span class="muted">
            {rows.length} · {human(rows.reduce((a, r) => a + r.size_bytes, 0))}
          </span>
        </h3>
        <table>
          <tbody>
            {#each rows as row}
              <tr>
                <td class="mono small type">{row.dtype || '—'}</td>
                <td class="path mono small truncate" title={row.path}>{row.path}</td>
                <td class="small muted num">{human(row.size_bytes)}</td>
                <td class="small muted when"><Ago iso={new Date(row.created_at * 1000).toISOString()} /></td>
                <td class="tags small">
                  {#if editing === row.key}
                    <input
                      bind:value={tagDraft}
                      onkeydown={(e) => e.key === 'Enter' && saveTags(row)}
                      size="16"
                    />
                    <button class="small" onclick={() => saveTags(row)}>save</button>
                  {:else}
                    {#each row.tags as t}<span class="tag">{t}</span>{/each}
                    <button
                      class="small"
                      title="edit tags"
                      onclick={() => { editing = row.key; tagDraft = row.tags.join(', ') }}
                    >
                      tag
                    </button>
                  {/if}
                </td>
                <td>
                  {#if row.origin === 'imported'}
                    <!-- forget drops the entry. The bytes were never the
                         store's, so there is nothing here that could remove
                         them. A product has no forget: `cache gc` reclaims it,
                         because it knows it can be re-derived. -->
                    <button class="small" onclick={() => forget(row)}>forget</button>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </section>
    {/each}
  {/if}
</div>

<style>
  .pane { padding: 14px 16px; overflow: auto; }
  .bar { display: flex; align-items: flex-start; gap: 12px; }
  .grow { flex: 1 1 auto; min-width: 0; }
  h2 { margin: 0; font-size: 15px; }
  h3 { margin: 14px 0 4px; display: flex; gap: 8px; align-items: baseline; }
  h3 .label { text-transform: none; letter-spacing: 0; color: var(--fg); }
  .controls { display: flex; gap: 14px; align-items: center; margin: 10px 0; flex-wrap: wrap; }
  .import { display: flex; gap: 10px; align-items: flex-end; flex-wrap: wrap; margin: 10px 0; }
  table { width: 100%; border-collapse: collapse; }
  td { padding: 2px 6px; border-bottom: 1px solid var(--line); vertical-align: middle; }
  .type { white-space: nowrap; }
  .path { max-width: 0; width: 100%; }
  .num { text-align: right; white-space: nowrap; }
  .when { white-space: nowrap; }
  .tags { white-space: nowrap; }
</style>
