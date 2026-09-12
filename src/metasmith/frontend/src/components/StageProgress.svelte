<script>
  // A progress bar rather than a row of dots: consecutive named stages, so
  // the thing that reads them is a filled track. `stageStates` carries the
  // whole meaning -- one entry per stage in `stages`, each 'idle' | 'running'
  // | 'done' | 'failed'. The caller derives that array (usually: everything
  // before the current stage is 'done', everything after is 'idle', and the
  // current stage alone takes its color from whatever is actually happening)
  // rather than this component guessing at status semantics it doesn't own.
  let { stages, stageStates } = $props()
</script>

<div class="progress">
  {#each stages as label, i}
    <div class="seg {stageStates[i]}">
      <span class="bar"></span>
      <span class="label">{label}</span>
    </div>
  {/each}
</div>

<style>
  .progress { display: flex; gap: 4px; }
  .seg { flex: 1; display: flex; flex-direction: column; gap: 5px; min-width: 0; }
  .seg .bar { height: 6px; border-radius: 3px; background: var(--line); }
  .seg .label {
    font-size: 12px;
    color: var(--muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .seg.running .bar { background: var(--accent); }
  .seg.running .label { color: var(--accent); }
  .seg.done .bar { background: var(--ok); }
  .seg.done .label { color: var(--ok); }
  .seg.failed .bar { background: var(--bad); }
  .seg.failed .label { color: var(--bad); }
</style>
