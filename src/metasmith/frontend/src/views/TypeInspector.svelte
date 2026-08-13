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

  // What goes in and what comes out, as a list of names -- including the type in
  // focus. Filtering it out used to make a card look like it named a type it did
  // not: the focused type is part of what the tool takes, and reading the card as
  // "these in, these out" only works if all of them are on it.
  const listed = (list) => [...new Set(list ?? [])].filter((t) => !plumbing(t))
  // ...counted over the whole requirement list, not what is left after a filter:
  // the focus is itself plumbing on the container cards, and excluding it there
  // left the count one short.
  const supplied = (list) => (list ?? []).filter(plumbing).length
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
            <!-- Two plain lists rather than a sentence in fragments: what goes
                 in, what comes out, one name per line. The prose form ("takes",
                 "also needs", "produces") with the types as chips wrapped across
                 a line read as a sentence, and the card is the narrowest column
                 on the page -- so a list it is. -->
            {#each [{ label: 'inputs', list: listed(tr.inputs), extra: supplied(tr.inputs) }, { label: 'outputs', list: listed(tr.outputs), extra: 0 }] as group}
              {#if group.list.length || group.extra}
                <div class="group small muted">{group.label}</div>
                {#each group.list as t}
                  <button
                    class="item mono small"
                    class:on={t === type || t === entry.as}
                    onclick={() => onpick?.(t)}
                    title="look at {t}"
                  >{t}</button>
                {/each}
                {#if group.extra}
                  <!-- its own line at the end of the list, not an aside on the
                       last name in it -->
                  <div class="supplied small muted" title="container images and bundled scripts, supplied for you">
                    +{group.extra} supplied
                  </div>
                {/if}
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
  /* the heading over a list, not a label beside one */
  .group {
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 10.5px;
    margin-top: 2px;
  }
  /* one type per line: the card is the narrowest column on the page, so a long
     namespace::type has to be allowed to break rather than widen it */
  .item {
    display: block;
    width: 100%;
    background: none;
    border: none;
    border-radius: 0;
    color: var(--accent);
    padding: 0 0 0 8px;
    font-size: 12px;
    text-align: left;
    word-break: break-all;
  }
  button.item:hover { text-decoration: underline; border: none; }
  /* the type you are looking at, in the list like any other -- just findable */
  .item.on { color: var(--text); }
  .supplied { padding-left: 8px; }
</style>
