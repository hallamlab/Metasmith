<script>
  import { isPlumbing } from '../lib/graphs.js'

  // What sits on either side of a type. This is the question the builder cannot
  // answer on its own -- you pick a type and immediately want to know what could
  // make it, what could take it, and what *else* those transforms want before
  // they will run. The last one is the payoff: it names the input you have not
  // registered yet, before a failed generate has to tell you.
  //
  // A card is also how you get a tool drawn: clicking one hands its index up, and
  // the graph above takes it over until the focus moves to another type.
  let {
    type = null,
    index = null,
    enabled = null,
    selected = null,
    onpick,
    onselect,
    onapply,
  } = $props()

  // An entry is {i, as, match}: which transform, the type it actually declared,
  // and how that relates to the one in focus. A transform is here because its
  // properties fit -- matching is the solver's `IsA`, never name equality -- so
  // the declared name is worth showing whenever it is not the name asked for.
  function resolve(entries) {
    if (!index) return []
    return (entries ?? [])
      .map((e) => ({ ...e, tr: index.transforms[e.i] }))
      .filter((e) => e.tr && (!enabled || enabled.has(e.tr.library)))
  }

  let produced = $derived(resolve(index?.by_type?.[type]?.produced_by))
  let consumed = $derived(resolve(index?.by_type?.[type]?.consumed_by))

  // said from the point of view of the type in focus, not the transform's
  const RELATION = {
    alias: { label: 'as', why: 'the same type under another name' },
    narrower: { label: 'makes', why: 'more specific than this, so it stands in for it' },
    broader: { label: 'takes', why: 'more general than this, so this satisfies it' },
  }

  let visible = $derived(
    (index?.transforms ?? []).filter((t) => !enabled || enabled.has(t.library)),
  )
  let libCount = $derived((index?.libraries ?? []).filter((l) => !enabled || enabled.has(l.path)).length)

  // Container images and bundled scripts are requirements, but never ones a
  // person registers -- the resource libraries supply them. Listing them here
  // would bury the requirement that *is* the user's to satisfy, which is the
  // whole reason this section exists. Same namespaces the DAG renderer hides,
  // and the same ones the graphs leave out -- one definition, in lib/graphs.js.
  const plumbing = isPlumbing

  // everything the transform wants that is not the type being looked at. `self`
  // is a list, not one name: the type in focus and the one the transform
  // declared for it can differ, and both are already shown above.
  const rest = (list, self) => (list ?? []).filter((t) => !self.includes(t))
  const others = (list, self) => rest(list, self).filter((t) => !plumbing(t))
  const hidden = (list, self) => rest(list, self).filter((t) => plumbing(t)).length
</script>

{#if !index}
  <p class="small muted">reading the libraries…</p>
{:else if !type}
  <div class="col" style="gap:10px">
    <p class="small muted">
      Pick a type — in the builder, or on any row of the recipe — and this shows
      which transforms could produce it and which could take it. Matching is by
      properties, so a transform asking for a more general type is listed too.
    </p>
    <table class="small">
      <tbody>
        <tr><td class="muted">transforms</td><td>{visible.length}</td></tr>
        <tr><td class="muted">types known</td><td>{Object.keys(index.by_type).length}</td></tr>
        <tr><td class="muted">libraries</td><td>{libCount} of {index.libraries.length}</td></tr>
      </tbody>
    </table>
    {#each index.libraries.filter((l) => l.error) as l}
      <p class="small"><span class="tag bad">unreadable</span>
        <span class="mono">{l.name}</span> — {l.error}</p>
    {/each}
  </div>
{:else}
  <div class="col" style="gap:14px">
    {#each [{ key: 'produced', label: 'produced by', list: produced, empty: 'Nothing in the enabled libraries produces this. As an output it is unreachable; as an input it has to come from you.' }, { key: 'consumed', label: 'consumed by', list: consumed, empty: 'Nothing in the enabled libraries takes this. As an input it would sit unused.' }] as section (section.key)}
      <div class="col" style="gap:6px">
        <div class="spread">
          <h3>{section.label}</h3>
          <span class="tag">{section.list.length}</span>
        </div>
        {#if section.list.length === 0}
          <p class="small muted">{section.empty}</p>
        {/if}
        {#each section.list as entry}
          {@const tr = entry.tr}
          {@const rel = RELATION[entry.match]}
          <!-- the type in focus and the name the transform declared for it are
               both shown above, so neither repeats in the lines below -->
          {@const seen = [type, entry.as]}
          <div class="tr col" class:sel={entry.i === selected}>
            <div class="spread">
              <button class="head grow spread" onclick={() => onselect?.(entry.i)} title="draw {tr.name}">
                <span class="mono truncate" title={tr.path}>{tr.name}</span>
                <span class="tag">{tr.library_name}</span>
              </button>
              {#if onapply}
                <!-- the card already says what this tool needs; this is that
                     list, put into the recipe as rows you can fill in -->
                <button
                  class="apply small"
                  onclick={() => onapply(entry.i)}
                  title="add a row to the recipe for each input {tr.name} needs"
                >apply</button>
              {/if}
            </div>
            {#if rel && entry.as}
              <!-- the match the type system made, spelled out: without this the
                   transform looks like it named this type and did not -->
              <div class="line small">
                <span class="muted lbl">{rel.label}</span>
                <span class="chips">
                  <button class="chip mono" onclick={() => onpick?.(entry.as)} title={rel.why}>
                    {entry.as}
                  </button>
                </span>
              </div>
            {/if}
            {#each [{ label: 'also needs', list: others(tr.inputs, seen), extra: hidden(tr.inputs, seen) }, { label: 'produces', list: others(tr.outputs, seen), extra: 0 }] as line}
              {#if line.list.length || line.extra}
                <div class="line small">
                  <span class="muted lbl">{line.label}</span>
                  <span class="chips">
                    {#each line.list as t}
                      <button class="chip mono" onclick={() => onpick?.(t)} title="look at {t}">
                        {t}
                      </button>
                    {/each}
                    {#if line.extra}
                      <span class="muted" title="container images and bundled scripts, supplied for you">
                        +{line.extra} supplied
                      </span>
                    {/if}
                  </span>
                </div>
              {/if}
            {/each}
          </div>
        {/each}
      </div>
    {/each}

    <p class="small muted">
      Counted across {libCount} of {index.libraries.length} libraries — the ones
      enabled in the builder.
    </p>
  </div>
{/if}

<style>
  .tr {
    gap: 4px;
    padding: 7px 8px;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    background: var(--bg);
  }
  .tr.sel { border-color: var(--accent); }
  /* the card's name is what selects it; it carries no chrome of its own so the
     card still reads as a card rather than as a button holding one */
  .head {
    width: 100%;
    background: none;
    border: none;
    border-radius: 0;
    padding: 0;
    text-align: left;
  }
  .head:hover { border: none; }
  .head:hover .mono { color: var(--accent); }
  /* an action on the card, not the card's purpose: quiet until it is reached for */
  .apply {
    flex: 0 0 auto;
    background: none;
    border-color: transparent;
    color: var(--muted);
    padding: 1px 6px;
  }
  .apply:hover { color: var(--text); border-color: var(--line); background: var(--panel-2); }
  .line { display: flex; gap: 6px; align-items: baseline; }
  .lbl { flex: 0 0 auto; }
  .chips { display: flex; flex-wrap: wrap; gap: 3px; min-width: 0; }
  .chip {
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    font-size: 12px;
    text-align: left;
    word-break: break-all;
  }
  .chip:hover { text-decoration: underline; border: none; }
</style>
