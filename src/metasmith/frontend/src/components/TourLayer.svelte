<script>
  import { untrack } from 'svelte'
  import { app, selectSection } from '../lib/state.svelte.js'
  import {
    addLinks,
    clearEdits,
    currentStep,
    editCount,
    exportEdits,
    goWhere,
    gotoStep,
    linksOf,
    nextStep,
    prevStep,
    resolveTarget,
    setHidden,
    setNavPosition,
    setStepText,
    settleStep,
    stepText,
    tour,
    tourCtx,
    tourEdit,
    tutorial,
    whereOf,
  } from '../lib/tour.svelte.js'
  import CopyButton from './CopyButton.svelte'
  import Icon from './Icon.svelte'

  const PAD = 6
  const GAP = 14
  const EDGE = 10
  const ADVANCE_MS = 900
  const FLASH_MS = 4000
  const FLASH_MAX = 3

  let t = $derived(tutorial())
  let step = $derived(tour.id ? currentStep() : null)
  let stepKey = $derived(t ? `${t.id}:${tour.step}` : null)

  let rect = $state(null)
  let done = $state(false)
  let watchRects = $state([])
  let flashes = $state([])
  const flashIds = new Set()
  let calloutW = $state(0)
  let calloutH = $state(0)
  let listOpen = $state(false)
  let vw = $state(window.innerWidth)
  let vh = $state(window.innerHeight)

  const box = (r) => ({ left: r.left, top: r.top, width: r.width, height: r.height })
  const same = (a, b) =>
    !!a && !!b && a.left === b.left && a.top === b.top && a.width === b.width && a.height === b.height

  // -- following the target -------------------------------------------------
  //
  // Polled every frame rather than observed: the target can appear late (a
  // modal opening, a solve finishing), move without resizing (the page
  // scrolling under it) or be replaced outright by a keyed remount, and no one
  // observer sees all three. It runs while the tutorial is hidden too, at a
  // slower beat, so a step done with the tutorial tucked away still counts and
  // still records what it made.
  const HIDDEN_POLL_MS = 500
  let armed = false
  let advanceTimer = null
  let scrolledFor = null

  $effect(() => {
    const key = stepKey
    clearTimeout(advanceTimer)
    advanceTimer = null
    armed = tour.arrived === 'forward'
    done = false
    rect = null
    watchRects = []
    listOpen = false
    let linked = false
    if (!key) return
    let raf = 0
    let slow = 0
    const later = () => {
      if (tour.hidden) slow = setTimeout(tick, HIDDEN_POLL_MS)
      else raf = requestAnimationFrame(tick)
    }
    const tick = () => {
      const s = currentStep()
      if (!s) return
      const el = resolveTarget(s.target)
      if (el) {
        const r = el.getBoundingClientRect()
        if (!same(rect, r)) rect = box(r)
        if (scrolledFor !== key && !tour.hidden) {
          scrolledFor = key
          const vh = window.innerHeight
          if (r.top < 0 || r.bottom > vh) {
            // a target taller than the window is read from its top
            el.scrollIntoView({ block: r.height > vh * 0.8 ? 'start' : 'center', inline: 'nearest', behavior: 'smooth' })
          }
        }
      } else if (rect) {
        rect = null
      }

      const watched = []
      for (const w of s.watch ?? []) {
        const wel = resolveTarget(w.target)
        if (wel) watched.push({ ...box(wel.getBoundingClientRect()), label: w.label })
      }
      if (watched.length !== watchRects.length || watched.some((w, i) => !same(w, watchRects[i]))) {
        watchRects = watched
      }

      let d = false
      try {
        d = settleStep(s) && (!!s.done?.(tourCtx, app, linksOf(tour.id)) || (!!s.touch && !!tourCtx.touched))
      } catch {
        d = false
      }
      if (d && !linked && s.link) {
        linked = true
        try {
          const made = s.link(tourCtx, app, linksOf(tour.id))
          addLinks(made)
          // the rail row of what was just made; it may only appear with the
          // section the step takes you to, so the diff below would miss it
          for (const id of Object.values(made ?? {})) if (id) flashIds.add(id)
        } catch {
          /* a link that cannot be read is simply not recorded */
        }
      }
      // Seeing a step undone is what licenses its `done` to carry you on, so a
      // step revisited after it was finished waits for Next like any other.
      // Editing text never moves on: the span being typed into would go with it.
      if (!d) armed = true
      if (d && !done && armed && !tourEdit && s.advance !== false && !advanceTimer) {
        advanceTimer = setTimeout(() => {
          advanceTimer = null
          if (stepKey === key && done) nextStep()
        }, ADVANCE_MS)
      }
      done = d
      later()
    }
    raf = requestAnimationFrame(tick)
    return () => {
      cancelAnimationFrame(raf)
      clearTimeout(slow)
      clearTimeout(advanceTimer)
      advanceTimer = null
    }
  })

  // A step with `touch` is done once the pointer has been over, pressed or
  // scrolled on something matching it -- "hover a step", "click one".
  $effect(() => {
    if (!stepKey) return
    const onEvent = (e) => {
      const sel = currentStep()?.touch
      if (sel && e.target instanceof Element && e.target.closest(sel)) tourCtx.touched = true
    }
    const opts = { capture: true, passive: true }
    for (const type of ['pointerover', 'pointerdown', 'wheel']) document.addEventListener(type, onEvent, opts)
    return () => {
      for (const type of ['pointerover', 'pointerdown', 'wheel']) document.removeEventListener(type, onEvent, opts)
    }
  })

  // -- what changed in the rails ---------------------------------------------
  //
  // A row that appears in a side rail while a tutorial runs -- the agent just
  // made, the run just launched -- is outlined for a few seconds, because the
  // page changes out of the corner of the eye while the card is being read.
  // Switching sections is not a change: the new rail is taken as it is. Nor is
  // a list arriving -- an empty rail filling, or the archive toggle adding a
  // batch -- nor a list reshuffling, as a rename or a project switch does, so
  // only a few rows turning up in a rail that kept all its rows count. What a
  // step just made is flashed by id wherever its row turns up, since that row
  // often arrives with the section change the diff ignores.
  $effect(() => {
    if (!tour.id) return
    let seen = null
    let seenSection = null
    const poll = () => {
      const rows = [...document.querySelectorAll('[data-rail-id]')]
      const ids = new Set(rows.map((r) => r.dataset.railId))
      const fresh = new Set([...flashIds].filter((id) => ids.has(id)))
      for (const id of fresh) flashIds.delete(id)
      if (seen && seenSection === app.section && seen.size && [...seen].every((id) => ids.has(id))) {
        const added = [...ids].filter((id) => !seen.has(id))
        if (added.length <= FLASH_MAX) for (const id of added) fresh.add(id)
      }
      if (fresh.size && !tour.hidden) {
        const now = Date.now()
        flashes = [
          ...flashes.filter((f) => !fresh.has(f.id)),
          ...[...fresh].map((id) => ({ id, until: now + FLASH_MS, rect: null })),
        ]
      }
      seen = ids
      seenSection = app.section
      const now = Date.now()
      const live = flashes
        .filter((f) => f.until > now)
        .map((f) => {
          const el = document.querySelector(`[data-rail-id="${CSS.escape(f.id)}"]`)
          return el ? { ...f, rect: box(el.getBoundingClientRect()) } : null
        })
        .filter(Boolean)
      if (live.length !== flashes.length || live.some((f, i) => !same(f.rect, flashes[i].rect))) flashes = live
    }
    const timer = setInterval(poll, 250)
    untrack(poll)
    return () => clearInterval(timer)
  })

  $effect(() => {
    const onresize = () => {
      vw = window.innerWidth
      vh = window.innerHeight
    }
    window.addEventListener('resize', onresize)
    return () => window.removeEventListener('resize', onresize)
  })

  // -- how much of the page goes dark --------------------------------------
  //
  // Only an intro card and a control still waiting to be pressed dim the page.
  // A step that waits on something -- a deploy, a solve, a run -- or points at
  // something to read leaves the page lit, since what happens next happens out
  // there and the dark would hide it.
  let dim = $derived(!!step && !!rect && !!step.do && !step.wait && !step.watch?.length && step.dim !== false && !done)

  // -- where the callout goes ------------------------------------------------

  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v))

  const NAV_W = 300
  let navH = $state(0)
  let dragging = $state(false)
  let prefX = $derived(clamp(tour.nav?.x ?? vw - NAV_W - 20, EDGE, vw - NAV_W - EDGE))
  let prefY = $derived(clamp(tour.nav?.y ?? vh - (navH || 150) - 20, EDGE, vh - (navH || 40) - EDGE))

  let headerBottom = $derived.by(() => {
    void vh, vw
    return document.querySelector('[data-tour="header"]')?.getBoundingClientRect().bottom ?? 0
  })

  // A callout with nothing to point at goes bottom right, and steps out of the
  // navigator's way only when the two would actually overlap: above it if
  // there is room, else below.
  function clearOfNav(x, y, w, h) {
    const top = headerBottom + EDGE
    const n = { l: prefX - GAP, t: prefY - GAP, r: prefX + NAV_W + GAP, b: prefY + (navH || 150) + GAP }
    y = clamp(y, top, vh - h - EDGE)
    if (x < n.r && x + w > n.l && y < n.b && y + h > n.t) {
      if (n.t - h >= top) y = n.t - h
      else if (n.b + h <= vh - EDGE) y = n.b
    }
    return { x, y }
  }

  let placed = $derived.by(() => {
    const w = calloutW || 340
    const h = calloutH || 160
    if (!rect) {
      // Nothing to point at: an intro is centred, a step still waiting for its
      // target sits above where the navigator would like to be
      if (!step?.target) return { x: (vw - w) / 2, y: Math.max(EDGE, vh * 0.28 - h / 2), side: null }
      return { ...clearOfNav(vw - w - 20, vh - h - 20, w, h), side: null }
    }
    const r = rect
    const cx = r.left + r.width / 2
    const cy = r.top + r.height / 2
    // a card over the tab bar would hide the next step's control
    const ceiling = r.top >= headerBottom ? headerBottom + EDGE : EDGE
    const at = {
      right: () => ({ x: r.left + r.width + PAD + GAP, y: clamp(cy - h / 2, ceiling, vh - h - EDGE) }),
      left: () => ({ x: r.left - PAD - GAP - w, y: clamp(cy - h / 2, ceiling, vh - h - EDGE) }),
      bottom: () => ({ x: clamp(cx - w / 2, EDGE, vw - w - EDGE), y: r.top + r.height + PAD + GAP }),
      top: () => ({ x: clamp(cx - w / 2, EDGE, vw - w - EDGE), y: r.top - PAD - GAP - h }),
    }
    const fits = (p) => p.x >= EDGE && p.y >= ceiling && p.x + w <= vw - EDGE && p.y + h <= vh - EDGE
    for (const side of [step?.placement, 'right', 'bottom', 'left', 'top']) {
      if (!side || !at[side]) continue
      const p = at[side]()
      if (fits(p)) return { ...p, side }
    }
    // a target bigger than the room around it: sit over its upper right
    return { x: vw - w - EDGE, y: clamp(r.top + 12, EDGE, vh - h - EDGE), side: null }
  })

  let arrow = $derived.by(() => {
    if (!rect || !placed.side) return null
    const w = calloutW || 340
    const h = calloutH || 160
    const cx = rect.left + rect.width / 2 - placed.x
    const cy = rect.top + rect.height / 2 - placed.y
    if (placed.side === 'right') return { left: -7, top: clamp(cy, 16, h - 16) - 6 }
    if (placed.side === 'left') return { left: w - 7, top: clamp(cy, 16, h - 16) - 6 }
    if (placed.side === 'bottom') return { left: clamp(cx, 16, w - 16) - 6, top: -7 }
    return { left: clamp(cx, 16, w - 16) - 6, top: h - 7 }
  })

  // -- the navigator ---------------------------------------------------------
  //
  // It stands where you put it (bottom right until you move it) unless that
  // covers the ring, a watched spot or the callout; then it takes the first
  // corner that covers none of them, or the one that covers least.
  let navPos = $derived.by(() => {
    const h = navH || 150
    if (dragging || !step) return { x: prefX, y: prefY }
    const top = headerBottom + EDGE
    const cands = [
      { x: prefX, y: prefY },
      { x: vw - NAV_W - 20, y: vh - h - 20 },
      { x: 20, y: vh - h - 20 },
      { x: vw - NAV_W - 20, y: top },
      { x: 20, y: top },
    ].map((p) => ({ x: clamp(p.x, EDGE, vw - NAV_W - EDGE), y: clamp(p.y, EDGE, vh - h - EDGE) }))
    const obstacles = []
    if (rect) obstacles.push({ l: rect.left - 12, t: rect.top - 12, r: rect.left + rect.width + 12, b: rect.top + rect.height + 12 })
    for (const w of watchRects) obstacles.push({ l: w.left - 8, t: w.top - 8, r: w.left + w.width + 8, b: w.top + w.height + 8 })
    obstacles.push({ l: placed.x, t: placed.y, r: placed.x + (calloutW || 340), b: placed.y + (calloutH || 160) })
    const overlap = (p) =>
      obstacles.reduce((sum, o) => {
        const w = Math.min(p.x + NAV_W, o.r) - Math.max(p.x, o.l)
        const hh = Math.min(p.y + h, o.b) - Math.max(p.y, o.t)
        return sum + (w > 0 && hh > 0 ? w * hh : 0)
      }, 0)
    let best = cands[0]
    let bestArea = overlap(best)
    for (const c of cands.slice(1)) {
      if (bestArea === 0) break
      const a = overlap(c)
      if (a < bestArea) {
        best = c
        bestArea = a
      }
    }
    return best
  })

  let drag = null

  function dragStart(e) {
    if (e.button !== 0 || e.target.closest('button')) return
    // grabbed where it stands, which is not where it was put if it dodged
    setNavPosition({ x: navPos.x, y: navPos.y }, false)
    drag = { dx: e.clientX - navPos.x, dy: e.clientY - navPos.y, id: e.pointerId }
    dragging = true
    e.currentTarget.setPointerCapture(e.pointerId)
  }
  function dragMove(e) {
    if (!drag || drag.id !== e.pointerId) return
    setNavPosition({ x: e.clientX - drag.dx, y: e.clientY - drag.dy }, false)
  }
  function dragEnd(e) {
    if (drag?.id !== e.pointerId) return
    drag = null
    dragging = false
    setNavPosition(tour.nav)
  }

  // the step list, folded to the steps either side of this one, with a
  // chapter's name over the first of its steps shown
  let rows = $derived.by(() => {
    if (!t) return []
    const shown = listOpen
      ? t.steps.map((_s, i) => i)
      : [tour.step - 1, tour.step, tour.step + 1].filter((i) => i >= 0 && i < t.steps.length)
    const out = []
    shown.forEach((i, k) => {
      const chapter = t.steps[i].chapter
      if (k === 0 || t.steps[shown[k - 1]].chapter !== chapter) out.push({ key: `c${i}`, chapter })
      out.push({ key: `s${i}`, i })
    })
    return out
  })
  let listEl = $state(null)
  $effect(() => {
    if (listOpen && listEl) listEl.querySelector('button.on')?.scrollIntoView({ block: 'center' })
  })

  let there = $derived.by(() => {
    if (!step?.target || rect) return null
    const w = whereOf(step)
    if (!w) return null
    const here = app.section === w.section && (!w.id || app.selected[w.section] === w.id)
    return here ? null : w
  })

  // backticks in a step's prose are code, and nothing else is markup
  const parts = (text) => String(text).split('`').map((s, i) => ({ code: i % 2 === 1, s }))
  const plain = (text) => String(text).replaceAll('`', '')
  const txt = (s, field, i = null) => stepText(t.id, s, field, i)
  // bound to the step the span was drawn for, which by blur may not be current
  const save = (s, field, i = null) => (e) =>
    setStepText(t.id, s, field, e.currentTarget.innerText.replace(/\n+$/, ''), i)
  // typing replaces the text node Svelte holds, so a reset has to redraw
  let resets = $state(0)
</script>

{#snippet prose(field, i = null)}
  {#if tourEdit}
    <span
      class="editable"
      contenteditable="plaintext-only"
      spellcheck="true"
      data-ph={field === 'doneText' ? 'done — shown once the step is done' : field}
      onblur={save(step, field, i)}
    >{txt(step, field, i)}</span>
  {:else}
    {#each parts(txt(step, field, i)) as p}{#if p.code}<code>{p.s}</code>{:else}{p.s}{/if}{/each}
  {/if}
{/snippet}

{#if t && step && !tour.hidden}
  {#if rect}
    <div
      class="ring"
      class:done
      class:dim
      aria-hidden="true"
      style={`left:${rect.left - PAD}px; top:${rect.top - PAD}px; width:${rect.width + PAD * 2}px; height:${rect.height + PAD * 2}px`}
    ></div>
  {:else if !step.target}
    <div class="scrim" aria-hidden="true"></div>
  {/if}

  {#each watchRects as w (w.label)}
    <div
      class="watch"
      aria-hidden="true"
      style={`left:${w.left - 4}px; top:${w.top - 4}px; width:${w.width + 8}px; height:${w.height + 8}px`}
    >
      <span class="wlabel small">{w.label}</span>
    </div>
  {/each}
  {#each flashes as f (f.id)}
    {#if f.rect}
      <div
        class="flash"
        aria-hidden="true"
        style={`left:${f.rect.left - 2}px; top:${f.rect.top - 2}px; width:${f.rect.width + 4}px; height:${f.rect.height + 4}px`}
      >
        <span class="wlabel small">new</span>
      </div>
    {/if}
  {/each}

  {#key `${stepKey}:${resets}`}
    <div
      class="callout"
      class:done
      class:editing={tourEdit}
      role="dialog"
      aria-label={plain(txt(step, 'title'))}
      bind:clientWidth={calloutW}
      bind:clientHeight={calloutH}
      style={`left:${placed.x}px; top:${placed.y}px`}
    >
      {#if arrow}
        <span class="arrow {placed.side}" style={`left:${arrow.left}px; top:${arrow.top}px`}></span>
      {/if}
      <div class="kicker small">{step.chapter}</div>
      <h2>{@render prose('title')}</h2>
      {#each step.body ?? [] as _para, i}
        <p>{@render prose('body', i)}</p>
      {/each}
      {#if step.snippet}
        <div class="snippet">
          <pre class="mono">{step.snippet.text}</pre>
          <CopyButton text={step.snippet.text} label={step.snippet.label ?? 'copy'} />
        </div>
      {/if}
      {#if step.do}
        <div class="do" class:done>
          {#if done && !tourEdit}
            <Icon name="check" size={12} />
            <span>{txt(step, 'doneText') || 'done'}</span>
          {:else}
            <span class="pip" aria-hidden="true"></span>
            <span>{@render prose('do')}</span>
          {/if}
        </div>
        {#if tourEdit}
          <div class="do done small">
            <Icon name="check" size={12} />
            <span>{@render prose('doneText')}</span>
          </div>
        {/if}
      {/if}
      {#if step.target && (tourEdit ? step.waiting : !rect && step.waiting)}
        <p class="small muted waiting">{@render prose('waiting')}</p>
      {/if}
      {#if there}
        <button class="small there" onclick={() => goWhere(step)}>take me there ›</button>
      {/if}
    </div>
  {/key}

  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="nav"
    class:dragging
    bind:clientHeight={navH}
    style={`left:${navPos.x}px; top:${navPos.y}px; width:${NAV_W}px`}
  >
    <div class="navhead" onpointerdown={dragStart} onpointermove={dragMove} onpointerup={dragEnd} onpointercancel={dragEnd}>
      <span class="grip" aria-hidden="true">⠿</span>
      <span class="grow truncate small" title={t.title}>{t.title}</span>
      <span class="count small mono">{tour.step + 1}/{t.steps.length}</span>
      <button
        class="icon"
        title="every tutorial, and picking this one up at any step"
        onclick={() => selectSection('tutorials')}
      >☰</button>
      <button
        class="icon"
        title="hide the tutorial — the tutorial button at the top brings it back where you left it"
        onclick={() => setHidden(true)}
      >×</button>
    </div>

    <ol class="steps small" class:open={listOpen} bind:this={listEl}>
      {#each rows as row (row.key)}
        {#if row.chapter}
          <li class="chap">{row.chapter}</li>
        {:else}
        {@const i = row.i}
        {@const s = t.steps[i]}
        <li>
          {#if i === tour.step}
            <button
              class="on"
              onclick={() => (listOpen = !listOpen)}
              title={listOpen ? 'fold the list' : 'every step'}
            >
              <span class="num">{i + 1}</span>
              <span class="grow truncate">{plain(txt(s, 'title'))}</span>
              {#if done}<span class="ok"><Icon name="check" size={11} /></span>{/if}
              <span class="chev" aria-hidden="true">{listOpen ? '▴' : '▾'}</span>
            </button>
          {:else}
            <button class:past={i < tour.step} class="peek" onclick={() => gotoStep(i)}>
              <span class="num">{i + 1}</span>
              <span class="grow truncate">{plain(txt(s, 'title'))}</span>
            </button>
          {/if}
        </li>
        {/if}
      {/each}
    </ol>

    <div class="navbtns">
      <button onclick={prevStep} disabled={tour.step === 0}>‹ prev</button>
      <button class="primary" onclick={nextStep}>
        {tour.step === t.steps.length - 1 ? 'finish' : done || !step.do ? 'next ›' : 'skip ›'}
      </button>
    </div>

    {#if tourEdit}
      <div class="editbar small">
        <span class="grow muted">editing · {editCount(t.id)} step(s) changed</span>
        <CopyButton text={exportEdits(t.id)} label="copy the edits as JSON" />
        <button class="small" disabled={!editCount(t.id)} onclick={() => (clearEdits(t.id), resets++)}>reset</button>
      </div>
    {/if}
  </div>
{/if}

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: var(--tour-scrim);
    z-index: 1000;
    pointer-events: none;
  }
  /* The dim, when there is one, is the ring's own shadow, so the hole in it is
     exactly the ring and the page under both stays clickable. */
  .ring {
    position: fixed;
    z-index: 1000;
    pointer-events: none;
    border: 2px solid var(--accent);
    border-radius: 8px;
    box-shadow: 0 0 0 3px color-mix(in srgb, var(--accent) 25%, transparent);
    transition: left 0.18s, top 0.18s, width 0.18s, height 0.18s, border-color 0.2s, box-shadow 0.3s;
  }
  .ring.dim { box-shadow: 0 0 0 200vmax var(--tour-scrim); }
  .ring::after {
    content: '';
    position: absolute;
    inset: -2px;
    border-radius: 10px;
    border: 2px solid var(--accent);
    animation: pulse 1.6s ease-out infinite;
  }
  .ring.done, .ring.done::after { border-color: var(--ok); }
  @keyframes pulse {
    from { inset: -2px; opacity: 0.9; }
    to { inset: -16px; opacity: 0; }
  }

  /* what to keep an eye on while the step waits: dashed, labelled, never dim */
  .watch, .flash {
    position: fixed;
    z-index: 1001;
    pointer-events: none;
    border: 2px dashed var(--accent);
    border-radius: 8px;
    animation: breathe 1.8s ease-in-out infinite;
  }
  .flash { border-style: solid; border-color: var(--ok); animation: fade 4s ease-out forwards; }
  .wlabel {
    position: absolute;
    top: -9px;
    right: 8px;
    padding: 0 6px;
    border-radius: 8px;
    background: var(--accent);
    color: var(--panel);
    font-size: 10px;
    line-height: 16px;
    white-space: nowrap;
  }
  .flash .wlabel { background: var(--ok); }
  @keyframes breathe { 50% { border-color: color-mix(in srgb, var(--accent) 35%, transparent); } }
  @keyframes fade { 0%, 70% { opacity: 1; } 100% { opacity: 0; } }

  .callout {
    position: fixed;
    z-index: 1002;
    width: min(340px, calc(100vw - 20px));
    background: var(--panel);
    border: 1px solid var(--accent);
    border-radius: 8px;
    padding: 12px 14px;
    box-shadow: 0 10px 30px var(--shadow);
    animation: rise 0.18s ease-out;
  }
  .callout.done { border-color: var(--ok); }
  @keyframes rise {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: none; }
  }
  .arrow {
    position: absolute;
    width: 12px;
    height: 12px;
    background: var(--panel);
    border-left: 1px solid var(--accent);
    border-top: 1px solid var(--accent);
  }
  /* one square with two borders drawn: its bordered corner is turned to face
     the target from whichever side of it the callout landed on */
  .arrow.right { transform: rotate(-45deg); }
  .arrow.left { transform: rotate(135deg); }
  .arrow.bottom { transform: rotate(45deg); }
  .arrow.top { transform: rotate(-135deg); }
  .callout.done .arrow { border-color: var(--ok); }
  .kicker {
    color: var(--accent);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 11px;
  }
  .callout h2 { margin: 2px 0 6px; }
  .callout p { margin: 0 0 8px; line-height: 1.45; }
  code {
    font-family: var(--mono);
    font-size: 12px;
    background: var(--panel-2);
    border-radius: 3px;
    padding: 0 4px;
  }
  .editable {
    display: inline-block;
    min-width: 2em;
    outline: 1px dashed var(--muted);
    outline-offset: 2px;
    border-radius: 2px;
    white-space: pre-wrap;
  }
  .editable:focus { outline-color: var(--accent); background: var(--sunken); }
  .editable:empty::before { content: attr(data-ph); color: var(--muted); font-style: italic; }
  .snippet {
    display: flex;
    align-items: flex-start;
    gap: 6px;
    margin-bottom: 8px;
  }
  .snippet pre {
    flex: 1;
    margin: 0;
    padding: 6px 8px;
    background: var(--sunken);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    white-space: pre;
    overflow-x: auto;
  }
  .do {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 8px;
    border-radius: var(--radius);
    background: color-mix(in srgb, var(--accent) 14%, transparent);
    font-size: 13px;
  }
  .do + .do { margin-top: 6px; }
  .do.done { background: color-mix(in srgb, var(--ok) 16%, transparent); color: var(--ok); }
  .pip {
    flex: 0 0 auto;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--accent);
    animation: blink 1.2s ease-in-out infinite;
  }
  @keyframes blink { 50% { opacity: 0.25; } }
  .waiting { margin: 8px 0 0; }
  .there { margin-top: 8px; }

  .nav {
    position: fixed;
    z-index: 1003;
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 8px;
    box-shadow: 0 10px 30px var(--shadow);
    padding: 8px 10px 10px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    transition: left 0.25s ease, top 0.25s ease;
  }
  .nav.dragging { transition: none; }
  .navhead {
    display: flex;
    align-items: center;
    gap: 6px;
    cursor: grab;
    user-select: none;
    touch-action: none;
  }
  .grip { color: var(--muted); }
  .count {
    flex: 0 0 auto;
    padding: 0 6px;
    border-radius: 8px;
    background: var(--panel-2);
    color: var(--muted);
  }
  .icon {
    padding: 0 7px;
    background: none;
    border-color: transparent;
    color: var(--muted);
  }
  .icon:hover:not(:disabled) { color: var(--text); background: var(--panel-2); }
  .chap {
    padding: 4px 6px 1px;
    color: var(--accent);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .ok { color: var(--ok); display: flex; }
  .steps {
    list-style: none;
    margin: 0;
    padding: 2px;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    background: var(--sunken);
  }
  .steps.open { max-height: 260px; overflow-y: auto; }
  .steps button {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    text-align: left;
    background: none;
    border-color: transparent;
    padding: 2px 6px;
    color: var(--muted);
  }
  .steps button.peek { opacity: 0.75; }
  .steps button.past { color: var(--text); }
  .steps button.on { background: var(--panel-2); color: var(--text); border-color: var(--accent); }
  .chev { color: var(--accent); flex: 0 0 auto; }
  .num { width: 18px; flex: 0 0 auto; text-align: right; color: var(--muted); }
  .navbtns { display: flex; justify-content: space-between; gap: 8px; }
  .navbtns button { flex: 1; }
  .editbar {
    display: flex;
    align-items: center;
    gap: 6px;
    padding-top: 6px;
    border-top: 1px dashed var(--line);
  }
</style>
