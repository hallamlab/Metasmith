<script>
  import { api } from '../lib/api.svelte.js'

  // The one thing on the page that reads the api service's own state rather
  // than any data it fetched. Nothing is wired to it: `api.status` is `$state`,
  // so this re-renders when a request starts, lands, or fails to land.

  const STATES = [
    { id: 'connected', title: 'the metasmith server is answering' },
    { id: 'saving', title: 'sending changes to the server' },
    { id: 'offline', title: 'no answer from the metasmith server' },
  ]

  let status = $derived(api.status)
  let title = $derived(STATES.find((s) => s.id === status)?.title ?? '')
</script>

<!-- Every label is rendered and all but one hidden, so the box is as wide as
     the widest word and stays that width whatever the state is. Sizing to the
     live text instead would shove the path and the links sideways each time a
     save started, which is a page that twitches every time you type. -->
<div class="link {status}" {title} role="status">
  <span class="dot"></span>
  <span class="label small">
    {#each STATES as s}
      <span class:on={s.id === status}>{s.id}</span>
    {/each}
  </span>
</div>

<style>
  .link {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    gap: 6px;
    /* the colour is set once, here, and both halves take it from currentColor */
    color: var(--muted);
  }
  .link.connected { color: var(--ok); }
  .link.saving { color: var(--warn); }
  .link.offline { color: var(--bad); }

  .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: currentColor;
    box-shadow: 0 0 5px currentColor;
  }

  /* one cell, three labels stacked in it */
  .label {
    display: grid;
    justify-items: start;
    white-space: nowrap;
  }
  .label > span {
    grid-area: 1 / 1;
    /* `visibility` rather than `display`, so the hidden ones still take part in
       sizing -- and, unlike opacity, are left out of the accessibility tree */
    visibility: hidden;
  }
  .label > span.on { visibility: visible; }
</style>
