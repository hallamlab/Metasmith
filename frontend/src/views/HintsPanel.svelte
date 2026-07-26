<script>
  // The planner already produces structured diagnostics for an unsolvable
  // target -- the requirement chain it was following, the transforms that could
  // have produced the type, and the ones that nearly matched. Until now they
  // were only ever printed as text.
  let { result, onadd } = $props()
</script>

<div class="col" style="gap:12px">
  <div class="spread">
    <h3>why it did not solve</h3>
    <span class="tag bad">no plan</span>
  </div>

  <p class="small">{result.message}</p>

  {#if result.dropped_targets?.length}
    <p class="small muted">
      unreachable:
      {#each result.dropped_targets as t}<span class="mono"> {t}</span>{/each}
    </p>
  {/if}

  {#each result.hints ?? [] as hint}
    <div class="card col" style="gap:8px">
      <div class="spread">
        <strong class="mono small">{hint.target}</strong>
        <span class="tag">{hint.kind}</span>
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
            <div class="miss small">
              <span class="mono">{miss}</span>
              {#if onadd}
                <!-- fills the builder's type field rather than registering
                     anything: a near miss still needs a path or a value -->
                <button class="small" onclick={() => onadd(miss)}>use this type</button>
              {/if}
            </div>
          {/each}
          <p class="small muted">
            These are one requirement short. Usually the missing piece is an input
            you have not registered, or one registered under a different type.
          </p>
        </div>
      {/if}
    </div>
  {/each}
</div>

<style>
  .chain { line-height: 1.8; word-break: break-word; }
  .miss { display: flex; justify-content: space-between; gap: 8px; align-items: center; }
</style>
