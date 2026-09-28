<script>
  import { TUTORIALS } from '../lib/tutorials/index.js'
  import { setMinimized, startTour, tour } from '../lib/tour.svelte.js'

  let open = $state(false)
  let box = $state(null)

  function pick(id) {
    open = false
    if (tour.id === id) setMinimized(false)
    else startTour(id)
  }

  $effect(() => {
    if (!open) return
    const away = (e) => {
      if (box && !box.contains(e.target)) open = false
    }
    window.addEventListener('pointerdown', away)
    return () => window.removeEventListener('pointerdown', away)
  })
</script>

<div class="menu" bind:this={box}>
  <button
    class="small"
    class:on={!!tour.id}
    onclick={() => (open = !open)}
    title="step-by-step walkthroughs that point at the controls as you use them"
    data-tour="tutorials"
  >tutorials</button>
  {#if open}
    <div class="pop card col">
      {#each TUTORIALS as t}
        <button class="item" onclick={() => pick(t.id)}>
          <div class="spread">
            <strong>{t.title}</strong>
            {#if tour.id === t.id}<span class="tag live">step {tour.step + 1}</span>{/if}
          </div>
          <div class="small muted">{t.summary}</div>
          <div class="small muted">{t.steps.length} steps · {t.duration}</div>
        </button>
        {#if tour.id === t.id}
          <button class="small" onclick={() => { open = false; startTour(t.id) }}>start over</button>
        {/if}
      {/each}
    </div>
  {/if}
</div>

<style>
  .menu { position: relative; }
  .on { border-color: var(--accent); color: var(--accent); }
  .pop {
    position: absolute;
    right: 0;
    top: calc(100% + 6px);
    width: 320px;
    z-index: 40;
    box-shadow: 0 10px 30px var(--shadow);
    padding: 8px;
  }
  .item {
    display: flex;
    flex-direction: column;
    gap: 2px;
    text-align: left;
    background: none;
  }
  .item:hover { background: var(--panel-2); }
</style>
