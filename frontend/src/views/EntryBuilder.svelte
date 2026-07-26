<script>
  import Field from '../components/Field.svelte'
  import TypeSelect from '../components/TypeSelect.svelte'

  // One builder for both halves of the recipe. An output is an input with the
  // identity rows taken away and a different set of things it can descend from,
  // so the role is a radio rather than a second component.
  let {
    types = [],
    index = null,
    enabled = null,
    libraries = [],
    items = [],
    targets = [],
    prefill = null,
    onadd,
    onfocus,
    ontogglelibrary,
  } = $props()

  let role = $state('input')
  let mode = $state('file')
  let form = $state({ path: '', name: '', value: '', dtype: '' })
  let parents = $state([])
  let libsOpen = $state(false)
  let busy = $state(false)

  // a type picked anywhere else -- a row of the recipe, a chip in the panel, a
  // near-miss in the hints -- lands in the field here, which is what makes those
  // one click rather than one click and then retyping it
  $effect(() => {
    if (prefill) form.dtype = prefill
  })

  // the standard library's type files are the vocabulary, but a transform may
  // name a type that no type file exports; both are offerable
  let allTypes = $derived(
    [
      ...new Set([
        ...types.filter((t) => t.full_name).map((t) => t.full_name),
        ...Object.keys(index?.by_type ?? {}),
      ]),
    ].sort(),
  )
  let typeNames = $derived(new Set(allTypes))
  let known = $derived(!!form.dtype && typeNames.has(form.dtype))

  // entries are {i, as, match} -- a transform is on this list because its
  // *properties* fit, which is not the same as having named this type
  function matches(type, side) {
    const entries = index?.by_type?.[type]?.[side] ?? []
    if (!enabled) return entries
    return entries.filter((e) => enabled.has(index.transforms[e.i]?.library))
  }

  const count = (type, side) => matches(type, side).length

  let produced = $derived(known ? count(form.dtype, 'produced_by') : 0)
  let consumed = $derived(known ? count(form.dtype, 'consumed_by') : 0)

  // How many of those are there because the type system says so rather than
  // because a name lined up. Worth saying: it is the difference between "one
  // transform mentions this" and "one transform will accept this".
  const indirect = (type, side) => matches(type, side).filter((e) => e.match !== 'exact').length

  let producedVia = $derived(known ? indirect(form.dtype, 'produced_by') : 0)
  let consumedVia = $derived(known ? indirect(form.dtype, 'consumed_by') : 0)

  // the right-hand side of a row in the list
  function describe(type) {
    const p = count(type, 'produced_by')
    const c = count(type, 'consumed_by')
    return { note: `${p} produce · ${c} consume`, warn: p === 0 && c === 0 }
  }

  // the same request twice is refused by the planner, so it is refused here,
  // where the reason can still be shown next to the thing that caused it
  let duplicate = $derived(
    role === 'output' &&
      known &&
      targets.some(
        (t) =>
          t.type === form.dtype &&
          JSON.stringify([...(t.parents ?? [])].sort()) === JSON.stringify([...parents].sort()),
      ),
  )

  let ready = $derived(
    known &&
      !duplicate &&
      (role === 'output' || (mode === 'file' ? !!form.path.trim() : !!form.name.trim())),
  )

  // parents mean different things on either side, so switching role drops them
  function setRole(next) {
    role = next
    parents = []
  }

  function pickType(value) {
    form.dtype = value
    if (typeNames.has(value)) onfocus?.(value)
  }

  function toggleParent(key) {
    parents = parents.includes(key) ? parents.filter((p) => p !== key) : [...parents, key]
  }

  async function add() {
    busy = true
    const body =
      role === 'output'
        ? { role, type: form.dtype, parents: [...parents].sort((a, b) => a - b) }
        : {
            role,
            dtype: form.dtype,
            parents,
            ...(mode === 'file'
              ? { path: form.path.trim() }
              : { name: form.name.trim(), value: form.value }),
          }
    const ok = await onadd?.(body)
    busy = false
    // the type and the lineage stay: registering five files of one type is the
    // common case, and retyping it four times is not a feature
    if (ok) form = { ...form, path: '', name: '', value: '' }
  }

  let enabledCount = $derived(libraries.filter((l) => !enabled || enabled.has(l.path)).length)
</script>

<div class="col" style="gap:10px">
  <h3>add</h3>

  <div class="row">
    <label class="small row" style="gap:4px">
      <input
        type="radio"
        style="width:auto"
        checked={role === 'input'}
        onchange={() => setRole('input')}
      /> an input
    </label>
    <label class="small row" style="gap:4px">
      <input
        type="radio"
        style="width:auto"
        checked={role === 'output'}
        onchange={() => setRole('output')}
      /> an output
    </label>
  </div>

  {#if role === 'input'}
    <div class="row">
      <label class="small row" style="gap:4px">
        <input type="radio" bind:group={mode} value="file" style="width:auto" /> a file
      </label>
      <label class="small row" style="gap:4px">
        <input type="radio" bind:group={mode} value="value" style="width:auto" /> a value
      </label>
    </div>

    {#if mode === 'file'}
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
  {/if}

  <Field label="type" hint="pick one, or type to narrow the list">
    <TypeSelect
      value={form.dtype}
      options={allTypes}
      placeholder="namespace::type"
      {describe}
      onchange={pickType}
    />
  </Field>

  <div class="row wrap small">
    {#if form.dtype && !known}
      <span class="tag warn">not a type in this library</span>
    {:else if known}
      <span
        class="tag"
        class:bad={role === 'output' && produced === 0}
        title={producedVia
          ? `${producedVia} of them by a more specific type, which satisfies this one`
          : null}
      >
        {produced} produce it{producedVia ? ` (${producedVia} indirectly)` : ''}
      </span>
      <span
        class="tag"
        class:warn={role === 'input' && consumed === 0}
        title={consumedVia
          ? `${consumedVia} of them ask for a more general type, which this one satisfies`
          : null}
      >
        {consumed} consume it{consumedVia ? ` (${consumedVia} indirectly)` : ''}
      </span>
      {#if role === 'output' && produced === 0}
        <span class="muted">nothing can make this — the plan will not solve</span>
      {:else if role === 'input' && consumed === 0}
        <span class="muted">nothing takes this — it would sit unused</span>
      {/if}
    {/if}
  </div>

  {#if role === 'input' && items.length}
    <Field
      label="derived from"
      hint="optional: which registered items this one belongs to, so results stay grouped by sample"
    >
      <div class="picker">
        {#each items as item (item.path)}
          <label class="small row" style="gap:4px">
            <input
              type="checkbox"
              style="width:auto"
              checked={parents.includes(item.path)}
              onchange={() => toggleParent(item.path)}
            />
            <span class="mono truncate">{item.path}</span>
          </label>
        {/each}
      </div>
    </Field>
  {:else if role === 'output' && targets.length}
    <Field
      label="derived from"
      hint="optional: which earlier output this one comes off — how two outputs of the same type stay distinct"
    >
      <div class="picker">
        {#each targets as target, i (i)}
          <label class="small row" style="gap:4px">
            <input
              type="checkbox"
              style="width:auto"
              checked={parents.includes(i)}
              onchange={() => toggleParent(i)}
            />
            <span class="mono truncate">#{i + 1} {target.type}</span>
          </label>
        {/each}
      </div>
    </Field>
  {/if}

  <div class="row">
    <button onclick={add} disabled={busy || !ready}>
      add {role}
    </button>
    {#if duplicate}
      <span class="small muted">already wanted, with the same lineage</span>
    {/if}
  </div>

  <!-- Not part of adding a row: it is what the planner is allowed to reach for,
       and it narrows the counts above and the panel beside it as you touch it. -->
  <div class="libs">
    <button class="fold small" onclick={() => (libsOpen = !libsOpen)}>
      <span class="caret">{libsOpen ? '▾' : '▸'}</span> transform libraries
      <span class="muted">{enabledCount} of {libraries.length}</span>
    </button>
    {#if libsOpen}
      <div class="picker" style="margin-top:6px">
        {#each libraries as l (l.path)}
          <label class="small row" style="gap:4px" title={l.path}>
            <input
              type="checkbox"
              style="width:auto"
              checked={!enabled || enabled.has(l.path)}
              onchange={() => ontogglelibrary?.(l.path)}
            />
            <span class="mono truncate grow">{l.name}</span>
            {#if l.error}
              <span class="tag bad">unreadable</span>
            {:else}
              <span class="muted">{l.transform_count}</span>
            {/if}
          </label>
        {/each}
      </div>
      <p class="small muted" style="margin:6px 0 0">
        Narrowing these changes what a generate may use, and marks the result stale.
      </p>
    {/if}
  </div>
</div>

<style>
  .picker {
    max-height: 150px;
    overflow-y: auto;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 6px 8px;
  }
  .libs { border-top: 1px dashed var(--line); padding-top: 10px; }
  .fold {
    background: none;
    border: none;
    padding: 0;
    color: var(--text);
    display: flex;
    gap: 6px;
    align-items: baseline;
  }
  .fold:hover { border: none; color: var(--accent); }
  .caret { color: var(--muted); }
</style>
