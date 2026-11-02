<script>
  import { selectSection } from '../lib/state.svelte.js'
  import { setHidden, tour, tutorial } from '../lib/tour.svelte.js'

  // One press shows or hides the tutorial in progress, navigator and all. With
  // none in progress it opens the Tutorials tab, which is where one starts.
  let t = $derived(tutorial())

  function click() {
    if (!t) selectSection('tutorials')
    else setHidden(!tour.hidden)
  }
</script>

<button
  class="tourbtn small"
  class:showing={!!t && !tour.hidden}
  class:paused={!!t && tour.hidden}
  onclick={click}
  data-tour="tutorials"
  title={!t
    ? 'step-by-step walkthroughs that point at the controls as you use them'
    : tour.hidden
      ? `bring back “${t.title}” where you left it`
      : 'hide the tutorial — the page is yours until you press this again'}
>
  <span class="glyph" aria-hidden="true">?</span>
  <span>{!t ? 'tutorials' : tour.hidden ? 'show tutorial' : 'hide tutorial'}</span>
  {#if t}<span class="where mono">{tour.step + 1}/{t.steps.length}</span>{/if}
</button>

<style>
  .tourbtn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    border-color: var(--accent);
    color: var(--accent);
    font-weight: 600;
  }
  .tourbtn:hover:not(:disabled) { background: color-mix(in srgb, var(--accent) 12%, transparent); }
  .glyph {
    display: inline-grid;
    place-items: center;
    width: 16px;
    height: 16px;
    border-radius: 50%;
    background: var(--accent);
    color: var(--panel);
    font-size: 11px;
    font-weight: 700;
  }
  .showing { background: var(--accent); color: var(--panel); }
  .showing:hover:not(:disabled) { background: color-mix(in srgb, var(--accent) 85%, black); }
  .showing .glyph { background: var(--panel); color: var(--accent); }
  .paused .glyph { animation: nudge 1.6s ease-in-out infinite; }
  @keyframes nudge {
    0%, 100% { box-shadow: 0 0 0 0 color-mix(in srgb, var(--accent) 60%, transparent); }
    50% { box-shadow: 0 0 0 5px transparent; }
  }
  .where { font-weight: 400; opacity: 0.8; font-size: 11px; }
</style>
