<script>
  import { TUTORIALS } from '../lib/tutorials/index.js'
  import { select } from '../lib/state.svelte.js'
  import { linksOf, resumeTour, startTour, stopTour, tour } from '../lib/tour.svelte.js'

  let { id } = $props()

  let t = $derived(TUTORIALS.find((x) => x.id === id) ?? null)
  let active = $derived(tour.id === id)
  let finished = $derived(tour.finished.includes(id))
  let links = $derived(linksOf(id))
  const MADE = [
    { key: 'agent', label: 'agent', section: 'agents' },
    { key: 'workflow', label: 'workflow', section: 'workflows' },
    { key: 'run', label: 'run', section: 'runs' },
  ]
  let made = $derived(MADE.filter((m) => links[m.key]))

  const plain = (text) => String(text).replaceAll('`', '')
</script>

{#if t}
  <div class="col page" style="gap:14px">
    <div class="col" style="gap:4px">
      <h2 style="margin:0">{t.title}</h2>
      <p class="muted" style="margin:0">{t.summary}</p>
      <p class="small muted" style="margin:0">{t.steps.length} steps · {t.duration}</p>
    </div>

    <div class="card col" style="gap:10px">
      {#if active}
        <div class="row wrap" style="gap:8px">
          <span class="tag live">in progress</span>
          <span>step {tour.step + 1} of {t.steps.length}: {plain(t.steps[tour.step].title)}</span>
        </div>
        <div class="row wrap" style="gap:8px">
          <button class="primary" onclick={() => resumeTour(id)}>continue here ›</button>
          <button onclick={() => startTour(id)}>start over</button>
          <button onclick={stopTour}>stop</button>
        </div>
      {:else}
        {#if finished}
          <div class="row" style="gap:8px"><span class="tag ok">finished</span></div>
        {:else if tour.id}
          <p class="small muted" style="margin:0">Another tutorial is in progress; starting this one puts it aside.</p>
        {/if}
        <div class="row wrap" style="gap:8px">
          <button class="primary" onclick={() => startTour(id)}>{finished ? 'start again' : 'start'}</button>
        </div>
      {/if}
      <p class="small muted" style="margin:0">
        Something gone wrong half way? Pick up from any step below: the tutorial
        opens the page that step happens on and carries on from there with what
        it made so far.
      </p>
    </div>

    {#if made.length}
      <div class="card col" style="gap:8px">
        <h3 style="margin:0">what this tutorial made</h3>
        <p class="small muted" style="margin:0">
          Later steps pick these up. If one was deleted or went wrong, restart from
          the step that makes it and the new one takes its place.
        </p>
        {#each made as m}
          <div class="row" style="gap:8px">
            <span class="small muted label">{m.label}</span>
            <button class="link mono" onclick={() => select(m.section, links[m.key])}>{links[m.key]}</button>
          </div>
        {/each}
      </div>
    {/if}

    <div class="card col" style="gap:6px">
      <ol class="steps">
        {#each t.steps as s, i}
          {#if i === 0 || t.steps[i - 1].chapter !== s.chapter}
            <li class="chap">{s.chapter}</li>
          {/if}
          <li>
            <button
              class:on={active && i === tour.step}
              class:past={active && i < tour.step}
              onclick={() => resumeTour(id, i)}
              title={active || made.length ? 'restart from here' : 'start from here'}
            >
              <span class="num">{i + 1}</span>
              <span class="grow truncate">{plain(s.title)}</span>
              {#if active && i === tour.step}<span class="tag live">here</span>{/if}
            </button>
          </li>
        {/each}
      </ol>
    </div>
  </div>
{/if}

<style>
  .page { padding: 18px; max-width: 760px; }
  .label { width: 70px; }
  .link {
    background: none;
    border: none;
    padding: 0;
    color: var(--accent);
    text-decoration: underline;
    cursor: pointer;
  }
  .steps { list-style: none; margin: 0; padding: 0; }
  .steps button {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    text-align: left;
    background: none;
    border-color: transparent;
    padding: 3px 6px;
  }
  .steps button:hover { background: var(--panel-2); }
  .steps button.past { color: var(--muted); }
  .steps button.on { border-color: var(--accent); background: var(--panel-2); }
  .chap {
    padding: 8px 6px 2px;
    color: var(--accent);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .chap:first-child { padding-top: 0; }
  .num { width: 20px; flex: 0 0 auto; text-align: right; color: var(--muted); }
</style>
