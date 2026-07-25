<script>
  import { api } from '../lib/api.js'
  import { attempt } from '../lib/state.svelte.js'
  import DeleteControl from '../components/DeleteControl.svelte'
  import Field from '../components/Field.svelte'

  let { workflow, onchange } = $props()

  let lib = $state(null)
  let types = $state([])
  let form = $state({ mode: 'file', path: '', name: '', value: '', dtype: '', parents: [] })
  let adding = $state(false)

  async function load() {
    lib = await api.get(`/workflows/${workflow}/inputs`)
  }

  $effect(() => {
    const w = workflow
    lib = null
    attempt(async () => {
      types = await api.get('/project/types')
      await load()
    })
    void w
  })

  async function add() {
    adding = true
    const body =
      form.mode === 'file'
        ? { path: form.path, dtype: form.dtype, parents: form.parents }
        : { name: form.name, value: form.value, dtype: form.dtype, parents: form.parents }
    const ok = await attempt(() => api.post(`/workflows/${workflow}/inputs/items`, body))
    adding = false
    if (ok) {
      form = { ...form, path: '', name: '', value: '' }
      await load()
      onchange?.()
    }
  }

  async function remove(path) {
    await attempt(async () => {
      await api.del(`/workflows/${workflow}/inputs/items?path=${encodeURIComponent(path)}`)
      await load()
      onchange?.()
    })
  }

  function toggleParent(path) {
    form.parents = form.parents.includes(path)
      ? form.parents.filter((p) => p !== path)
      : [...form.parents, path]
  }
</script>

<div class="col" style="gap:10px">
  <div class="spread">
    <h3>inputs</h3>
    <span class="small muted">{lib?.items?.length ?? 0} item(s)</span>
  </div>

  {#if !lib}
    <p class="small muted">loading…</p>
  {:else}
    {#if lib.items.length === 0}
      <p class="small muted">
        Nothing here yet. Add the files you want to run over, and give each one a
        type — the planner works out the steps from the types alone.
      </p>
    {:else}
      <div class="scroll">
        <table class="small">
          <thead>
            <tr><th>item</th><th>type</th><th>from</th><th></th></tr>
          </thead>
          <tbody>
            {#each lib.items as item (item.path)}
              <tr>
                <td class="mono" title={item.path}>{item.path}</td>
                <td class="mono">{item.type_name}</td>
                <td class="mono muted small">
                  {item.parents.map((p) => p.path).join(', ') || '—'}
                </td>
                <td style="width:1%">
                  <DeleteControl title="remove from library" onconfirm={() => remove(item.path)} />
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
      <p class="small muted">
        Removing unregisters the item; the file itself is left alone.
      </p>
    {/if}

    <div class="card col" style="gap:8px">
      <div class="row">
        <label class="small row" style="gap:4px">
          <input type="radio" bind:group={form.mode} value="file" style="width:auto" /> a file
        </label>
        <label class="small row" style="gap:4px">
          <input type="radio" bind:group={form.mode} value="value" style="width:auto" /> a value
        </label>
      </div>

      {#if form.mode === 'file'}
        <Field label="path" hint="an absolute path on this machine; nothing is copied">
          <input bind:value={form.path} placeholder="/data/sample_01.fastq.gz" />
        </Field>
      {:else}
        <Field label="name">
          <input bind:value={form.name} placeholder="K12" />
        </Field>
        <Field label="value" hint="an accession, an identifier — anything a transform takes as text">
          <input bind:value={form.value} placeholder="GCF_000005845.2" />
        </Field>
      {/if}

      <Field label="type">
        <select bind:value={form.dtype}>
          <option value="">choose a type…</option>
          {#each types as t}
            {#if t.full_name}<option value={t.full_name}>{t.full_name}</option>{/if}
          {/each}
        </select>
      </Field>

      {#if lib.items.length}
        <Field
          label="derived from"
          hint="optional: which existing items this one belongs to, so results stay grouped by sample"
        >
          <div class="parents">
            {#each lib.items as item}
              <label class="small row" style="gap:4px">
                <input
                  type="checkbox"
                  style="width:auto"
                  checked={form.parents.includes(item.path)}
                  onchange={() => toggleParent(item.path)}
                />
                <span class="mono truncate">{item.path}</span>
              </label>
            {/each}
          </div>
        </Field>
      {/if}

      <div>
        <button
          onclick={add}
          disabled={adding || !form.dtype || (form.mode === 'file' ? !form.path : !form.name)}
        >add input</button>
      </div>
    </div>
  {/if}
</div>

<style>
  .scroll { overflow-x: auto; }
  .parents {
    max-height: 130px;
    overflow-y: auto;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 6px 8px;
  }
</style>
