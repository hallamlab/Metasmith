<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, createWorkflow, ui } from '../lib/state.svelte.js'
  import JobLog from './JobLog.svelte'

  // What `+ workflow` opens. A template is a workflow you start from -- a spec
  // whose input paths are deferred -- so choosing one here is not a merge and
  // never touches an existing recipe: this modal is only ever on *new*.
  //
  // Blank is the default and costs nothing. Nothing is solved until a template
  // is picked, and then only that one.
  let { onclose } = $props()

  let templates = $state([])
  let picked = $state('')
  let jobId = $state(null)
  let drawing = $state(false)
  let error = $state(null)
  // bumped when a drawing lands: the url is otherwise identical to the one that
  // 409'd a moment ago, and the browser is entitled to remember that
  let stamp = $state(0)
  let creating = $state(false)

  let chosen = $derived(templates.find((t) => t.name === picked) ?? null)

  $effect(() => {
    const theme = ui.theme
    attempt(async () => {
      templates = await api.get(`/templates?theme=${theme}`)
    })
  })

  // Both inputs, because a drawing is cached per theme: switching theme with a
  // template open has to draw the other one rather than ask for a file that is
  // not there.
  $effect(() => {
    draw(picked, ui.theme)
  })

  async function draw(name, theme) {
    jobId = null
    error = null
    if (!name) return
    drawing = true
    try {
      const out = await api.post(`/templates/${name}/dag?theme=${theme}`)
      if (out?.cached) {
        ready(name)
      } else {
        jobId = out.id
      }
    } catch (e) {
      error = e.message
      drawing = false
    }
  }

  function ready(name) {
    templates = templates.map((t) => (t.name === name ? { ...t, dag_ready: true } : t))
    stamp += 1
    drawing = false
  }

  function drawn(summary) {
    if (summary?.status === 'done') {
      ready(picked)
      jobId = null
    } else {
      // the log stays up on a failure: a template that no longer solves says so
      // there, and that is the only place the reason exists
      error = summary?.error ?? 'the drawing failed'
      drawing = false
    }
  }

  async function create() {
    creating = true
    const out = await createWorkflow(picked ? { template: picked } : {})
    creating = false
    if (out) onclose?.()
  }

  function onkey(e) {
    if (e.key === 'Escape') onclose?.()
  }
</script>

<svelte:window on:keydown={onkey} />

<!-- The backdrop closes on click, which is why it carries a role and a key
     handler; the card stops the click so a press inside it never closes. -->
<!-- svelte-ignore a11y_click_events_have_key_events -->
<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="scrim" onclick={() => onclose?.()}>
  <div
    class="sheet card col"
    role="dialog"
    tabindex="-1"
    aria-modal="true"
    aria-label="new workflow"
    onclick={(e) => e.stopPropagation()}
  >
    <div class="spread">
      <h2>new workflow</h2>
      <span class="small muted">start from a template, or from nothing</span>
    </div>

    <div class="stage">
      {#if error}
        <p class="small bad pad">{error}</p>
      {:else if !picked}
        <p class="small muted pad">
          an empty workflow — you name the inputs and the targets yourself
        </p>
      {:else if chosen?.dag_ready}
        <img
          class="dag"
          src={`/api/templates/${picked}/dag?theme=${ui.theme}&v=${stamp}`}
          alt={`what ${picked} builds`}
        />
      {:else}
        <p class="small muted pad">solving {picked}…</p>
      {/if}
    </div>

    {#if jobId}
      <JobLog {jobId} onend={drawn} />
    {/if}

    <label class="col small">
      <span class="muted">template</span>
      <select bind:value={picked}>
        <option value="">blank</option>
        {#each templates as t (t.name)}
          <option value={t.name}>{t.name}</option>
        {/each}
      </select>
    </label>
    <p class="small muted desc">
      {chosen?.description || (picked ? '' : 'nothing is planned until you generate')}
    </p>

    <div class="row end">
      <button onclick={() => onclose?.()}>cancel</button>
      <button class="primary" disabled={creating || drawing} onclick={create}>create</button>
    </div>
  </div>
</div>

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: color-mix(in srgb, var(--bg) 70%, transparent);
    backdrop-filter: blur(2px);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 20;
    padding: 24px;
  }
  .sheet {
    width: min(720px, 100%);
    max-height: 100%;
    overflow: auto;
  }
  /* the drawing area keeps its height whatever is in it, so picking a template
     does not make the buttons jump out from under the cursor */
  .stage {
    height: min(380px, 46vh);
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--sunken);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    overflow: auto;
    padding: 8px;
  }
  /* Drawn at its own size and scrolled, not shrunk to fit: a 26-step plan
     squeezed into 320px is a grey smear, and the point of showing it is to read
     what the template builds. Only an over-wide one is scaled. */
  .dag { max-width: 100%; margin: auto; }
  .pad { padding: 12px; text-align: center; }
  .bad { color: var(--bad); }
  .desc { min-height: 18px; margin: 0; }
  .end { justify-content: flex-end; }
</style>
