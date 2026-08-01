<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, createWorkflow, ui } from '../lib/state.svelte.js'
  import DeleteControl from './DeleteControl.svelte'
  import JobLog from './JobLog.svelte'
  import Modal from './Modal.svelte'

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

  // -- pan and zoom over the preview ------------------------------------------
  //
  // `scale`/`tx`/`ty` place the image's own top-left corner in the stage's own
  // pixels -- `transform-origin: 0 0`, so nothing here fights the browser's
  // default (centre) origin, which is what made cursor-anchored zoom simple to
  // get wrong.
  let scale = $state(1)
  let tx = $state(0)
  let ty = $state(0)
  let stageEl = $state(null)
  let imgEl = $state(null)
  let drag = $state(null)

  const MIN_SCALE = 0.2
  const MAX_SCALE = 8

  // Centred at its own size, not fit to the box: this is the same choice the
  // scrollable version made (a 26-step plan squeezed to fit is a grey smear),
  // panning is just what replaced the scrollbar that used to reach the rest.
  function center() {
    if (!stageEl || !imgEl?.naturalWidth) return
    scale = 1
    tx = (stageEl.clientWidth - imgEl.naturalWidth) / 2
    ty = (stageEl.clientHeight - imgEl.naturalHeight) / 2
  }

  // Attached by hand rather than as `onwheel={…}`. Svelte 5 registers a
  // declarative wheel handler as a *passive* listener, so `preventDefault()` in
  // it is a no-op and the browser scrolls the page instead of just zooming.
  $effect(() => {
    if (!stageEl) return
    const el = stageEl
    el.addEventListener('wheel', onWheel, { passive: false })
    return () => el.removeEventListener('wheel', onWheel)
  })

  function onWheel(e) {
    if (!imgEl) return
    e.preventDefault()
    // shift turns the wheel into a scrollbar: some browsers already swap
    // deltaX/deltaY for us when shift is held, so take whichever axis carries
    // the motion rather than assuming deltaY
    if (e.shiftKey) {
      ty -= e.deltaY || e.deltaX
      return
    }
    const rect = stageEl.getBoundingClientRect()
    const cx = e.clientX - rect.left
    const cy = e.clientY - rect.top
    // the point under the cursor stays under it: solve for the image-space
    // point it currently names, then place the new scale so that point still
    // lands at (cx, cy)
    const ix = (cx - tx) / scale
    const iy = (cy - ty) / scale
    const next = Math.min(MAX_SCALE, Math.max(MIN_SCALE, scale * Math.exp(-e.deltaY * 0.0015)))
    tx = cx - ix * next
    ty = cy - iy * next
    scale = next
  }

  function onPointerDown(e) {
    if (e.button !== 0 || !imgEl) return
    drag = { x: e.clientX - tx, y: e.clientY - ty, id: e.pointerId }
    stageEl.setPointerCapture(e.pointerId)
  }

  function onPointerMove(e) {
    if (!drag || drag.id !== e.pointerId) return
    tx = e.clientX - drag.x
    ty = e.clientY - drag.y
  }

  function endDrag(e) {
    if (drag?.id === e.pointerId) drag = null
  }

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

  async function deleteTemplate() {
    const gone = picked
    const out = await attempt(() => api.del(`/templates/${gone}`))
    if (out) {
      templates = templates.filter((t) => t.name !== gone)
      if (picked === gone) picked = ''
    }
  }
</script>

<Modal title="new workflow" subtitle="start from a template, or from nothing" {onclose}>
  <div
    class="stage"
    class:panning={!!drag}
    role="application"
    aria-label="template diagram — scroll to zoom, drag to pan"
    bind:this={stageEl}
    onpointerdown={onPointerDown}
    onpointermove={onPointerMove}
    onpointerup={endDrag}
    onpointercancel={endDrag}
  >
    {#if error}
      <p class="small bad pad">{error}</p>
    {:else if !picked}
      <p class="small muted pad">
        an empty workflow — you name the inputs and the targets yourself
      </p>
    {:else if chosen?.dag_ready}
      <img
        class="dag"
        bind:this={imgEl}
        onload={center}
        draggable="false"
        style={`transform: translate(${tx}px, ${ty}px) scale(${scale})`}
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
    <div class="row">
      <select bind:value={picked}>
        <option value="">blank</option>
        {#each templates as t (t.name)}
          <option value={t.name}>{t.name}</option>
        {/each}
      </select>
      {#if chosen?.source === 'user'}
        <DeleteControl title="delete template" archived onconfirm={deleteTemplate} />
      {/if}
    </div>
  </label>
  <p class="small muted desc">
    {chosen?.description || (picked ? '' : 'nothing is planned until you generate')}
  </p>

  {#snippet footer()}
    <button onclick={() => onclose?.()}>cancel</button>
    <button class="primary" disabled={creating || drawing} onclick={create}>create</button>
  {/snippet}
</Modal>

<style>
  /* the drawing area keeps its height whatever is in it, so picking a template
     does not make the buttons jump out from under the cursor. `hidden` and
     `relative`, not `auto` and static: the image no longer scrolls, it is
     panned by hand, and `.dag`'s `position: absolute` places it against this
     box's own corner. */
  .stage {
    position: relative;
    height: min(380px, 46vh);
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--sunken);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    overflow: hidden;
    padding: 8px;
    touch-action: none;
    cursor: grab;
  }
  .stage.panning { cursor: grabbing; }
  /* Drawn at its own size, panned and zoomed rather than shrunk to fit: a
     26-step plan squeezed into 320px is a grey smear, and the point of showing
     it is to read what the template builds. `transform-origin: 0 0` is what
     keeps the cursor-anchored zoom math in the script simple -- `tx`/`ty` are
     then exactly the image's own top-left corner, in the stage's pixels, and
     nothing here has to account for the browser's default centre origin. */
  .dag {
    position: absolute;
    top: 0;
    left: 0;
    transform-origin: 0 0;
    max-width: none;
  }
  .pad { padding: 12px; text-align: center; }
  .bad { color: var(--bad); }
  .desc { min-height: 18px; margin: 0; }
</style>
