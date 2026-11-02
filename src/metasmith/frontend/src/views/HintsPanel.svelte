<script>
  // The planner already produces structured diagnostics for an unsolvable
  // target -- the requirement chain it was following, the transforms that could
  // have produced the type, and the ones that nearly matched. Until now they
  // were only ever printed as text.
  let { result, onadd } = $props()

  // Every near-miss line leads with the type it is about -- a registered input
  // (`sequences::reads @ /path (overlap …)`), a retyping to make, or a parent
  // type nothing provides. The button acts on that type, so it reads it back
  // off the front of the line rather than passing the whole sentence as a type
  // name, which is what it used to do.
  const TYPE = /^[A-Za-z_][\w]*::[\w.-]+/
  const typeOf = (miss) => TYPE.exec(String(miss ?? ''))?.[0] ?? null
</script>

<div class="col" style="gap:12px">
  <!-- `.row`, not `.spread`: with three children `space-between` pushes the two
       chips apart and away from the heading, and these read as annotations on
       it rather than as a right-hand column -->
  <div class="row">
    <h3>why it did not solve</h3>
    <span class="tag bad">experimental</span>
    <span class="tag bad">no plan</span>
  </div>

  <p class="small">{result.message}</p>

  {#if result.dropped_targets?.length}
    <p class="small muted">
      unreachable:
      {#each result.dropped_targets as t}<span class="mono"> {t}</span>{/each}
    </p>
  {/if}

  <!-- What the *server* planned from, in its own words. The recipe above is the
       browser's account of the same thing, which is exactly the account you
       cannot check a failure against: nearly every one of these is a type that
       is not what it was thought to be. Shown on failure only -- a plan that
       solved has a drawing that says the same thing better. -->
  {#if result.given || result.targets}
    <div class="card col" style="gap:8px">
      <div class="spread">
        <h3>what it was given</h3>
        <span class="small muted">as the server read it, not as the page drew it</span>
      </div>
      {#if result.given?.length}
        <table class="small echo">
          <tbody>
            {#each result.given as g}
              <tr>
                <td class="mono truncate" title={g.path}>{g.path}</td>
                <td class="mono type">{g.type}</td>
                <td class="muted small">
                  {#if g.parents?.length}from {g.parents.join(', ')}{/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else}
        <p class="small muted">Nothing was registered — every row is still a draft.</p>
      {/if}
      <div class="row wrap small">
        <span class="muted">wanted:</span>
        {#each result.targets ?? [] as t}
          <span class="tag mono">{t.type || '(no type)'}</span>
        {/each}
        {#if result.sample_type}
          <span class="muted">· one run per sample of</span>
          <span class="tag mono">{result.sample_type}</span>
        {/if}
      </div>
    </div>
  {/if}

  {#each result.hints ?? [] as hint}
    <div class="card col" style="gap:8px">
      <div class="spread">
        <strong class="mono small">{hint.target}</strong>
        <!-- the one hint kind that is usually the whole answer, so it is the
             one that does not read like another line of diagnostics -->
        <span class="tag" class:warn={hint.kind === 'too_general'}>
          {hint.kind === 'too_general' ? 'too general' : hint.kind}
        </span>
      </div>
      <p class="small">{hint.message}</p>

      {#if hint.chain?.length}
        <div>
          <h3>what it was trying to build</h3>
          <div class="chain small mono">
            {#each hint.chain as step, i}
              {#if i > 0}<span class="muted"> → </span>{/if}<span>{step}</span>
            {/each}
          </div>
        </div>
      {/if}

      {#if hint.candidate_transforms?.length}
        <div>
          <h3>transforms that produce this</h3>
          <div class="row wrap">
            {#each hint.candidate_transforms as c}<span class="tag mono">{c}</span>{/each}
          </div>
        </div>
      {/if}

      {#if hint.near_misses?.length}
        <div class="col" style="gap:4px">
          <h3>nearly matched</h3>
          {#each hint.near_misses as miss}
            {@const type = typeOf(miss)}
            <div class="miss small">
              <span class="mono">{miss}</span>
              {#if onadd && type}
                <!-- makes the row rather than registering anything: a near miss
                     still needs a path or a value before it is an input -->
                <button class="small" onclick={() => onadd(type)}>add a row for this</button>
              {/if}
            </div>
          {/each}
          <p class="small muted">
            {#if hint.kind === 'too_general'}
              The first of these are labels for the row you already have —
              click its type in the recipe above and pick one. The lines naming
              a parent are inputs nothing here provides at all, so those do want
              a row of their own.
            {:else}
              These are one requirement short. Usually the missing piece is an input
              you have not registered, or one registered under a different type.
            {/if}
          </p>
        </div>
      {/if}
    </div>
  {/each}
</div>

<style>
  .chain { line-height: 1.8; word-break: break-word; }
  /* fixed layout so a long path truncates instead of pushing the type -- which
     is the column being read -- off the card */
  .echo { width: 100%; table-layout: fixed; }
  .echo td { padding: 1px 6px 1px 0; }
  .echo .type { color: var(--accent); width: 40%; }
  .miss { display: flex; justify-content: space-between; gap: 8px; align-items: center; }
</style>
