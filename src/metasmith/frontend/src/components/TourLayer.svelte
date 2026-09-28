<script>
  import { app } from '../lib/state.svelte.js'
  import {
    currentStep,
    nextStep,
    prevStep,
    gotoStep,
    resolveTarget,
    setMinimized,
    setNavPosition,
    stopTour,
    tour,
    tourCtx,
    tutorial,
  } from '../lib/tour.svelte.js'
  import CopyButton from './CopyButton.svelte'
  import Icon from './Icon.svelte'

  const PAD = 6
  const GAP = 14
  const EDGE = 10
  const ADVANCE_MS = 900

  let t = $derived(tutorial())
  let step = $derived(tour.id ? currentStep() : null)
  let stepKey = $derived(t ? `${t.id}:${tour.step}` : null)
  let chapters = $derived.by(() => {
    if (!t) return []
    const out = []
    t.steps.forEach((s, i) => {
      const last = out[out.length - 1]
      if (!last || last.name !== s.chapter) out.push({ name: s.chapter, from: i, to: i })
      else last.to = i
    })
    return out
  })

  let rect = $state(null)
  let done = $state(false)
  let calloutW = $state(0)
  let calloutH = $state(0)
  let listOpen = $state(false)
  let vw = $state(window.innerWidth)
  let vh = $state(window.innerHeight)

  // -- following the target -------------------------------------------------
  //
  // Polled every frame rather than observed: the target can appear late (a
  // modal opening, a solve finishing), move without resizing (the page
  // scrolling under it) or be replaced outright by a keyed remount, and no one
  // observer sees all three.
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
    if (!key) return
    let raf = 0
    const tick = () => {
      const s = currentStep()
      if (!s) return
      const el = resolveTarget(s.target)
      if (el) {
        const r = el.getBoundingClientRect()
        if (!rect || r.left !== rect.left || r.top !== rect.top || r.width !== rect.width || r.height !== rect.height) {
          rect = { left: r.left, top: r.top, width: r.width, height: r.height }
        }
        if (scrolledFor !== key) {
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
      let d = false
      try {
        d = !!s.done?.(tourCtx, app)
      } catch {
        d = false
      }
      // Seeing a step undone is what licenses its `done` to carry you on, so a
      // step revisited after it was finished waits for Next like any other.
      if (!d) armed = true
      if (d && !done && armed && s.advance !== false && !advanceTimer) {
        advanceTimer = setTimeout(() => {
          advanceTimer = null
          if (stepKey === key) nextStep()
        }, ADVANCE_MS)
      }
      done = d
      raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => {
      cancelAnimationFrame(raf)
      clearTimeout(advanceTimer)
      advanceTimer = null
    }
  })

  $effect(() => {
    const onresize = () => {
      vw = window.innerWidth
      vh = window.innerHeight
    }
    window.addEventListener('resize', onresize)
    return () => window.removeEventListener('resize', onresize)
  })

  // -- where the callout goes ------------------------------------------------

  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v))

  let placed = $derived.by(() => {
    const w = calloutW || 340
    const h = calloutH || 160
    if (!rect) {
      // Nothing to point at: an intro is centred, a step still waiting for its
      // target sits above the navigator so it is read alongside it.
      if (!step?.target) return { x: (vw - w) / 2, y: Math.max(EDGE, vh * 0.28 - h / 2), side: null }
      return { x: vw - w - 20, y: Math.max(EDGE, navY - h - 12), side: null }
    }
    const r = rect
    const cx = r.left + r.width / 2
    const cy = r.top + r.height / 2
    const at = {
      right: () => ({ x: r.left + r.width + PAD + GAP, y: clamp(cy - h / 2, EDGE, vh - h - EDGE) }),
      left: () => ({ x: r.left - PAD - GAP - w, y: clamp(cy - h / 2, EDGE, vh - h - EDGE) }),
      bottom: () => ({ x: clamp(cx - w / 2, EDGE, vw - w - EDGE), y: r.top + r.height + PAD + GAP }),
      top: () => ({ x: clamp(cx - w / 2, EDGE, vw - w - EDGE), y: r.top - PAD - GAP - h }),
    }
    const fits = (p) => p.x >= EDGE && p.y >= EDGE && p.x + w <= vw - EDGE && p.y + h <= vh - EDGE
    for (const side of [step?.placement, 'right', 'bottom', 'left', 'top']) {
      if (!side || !at[side]) continue
      const p = at[side]()
      if (fits(p)) return { ...p, side }
    }
    // a target bigger than the room around it: sit over its upper right,
    // clear of the navigator's default corner
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

  const NAV_W = 300
  let navH = $state(0)
  let navX = $derived(clamp(tour.nav?.x ?? vw - NAV_W - 20, EDGE, vw - NAV_W - EDGE))
  let navY = $derived(clamp(tour.nav?.y ?? vh - (navH || 150) - 20, EDGE, vh - (navH || 40) - EDGE))
  let drag = null

  function dragStart(e) {
    if (e.button !== 0 || e.target.closest('button')) return
    drag = { dx: e.clientX - navX, dy: e.clientY - navY, id: e.pointerId }
    e.currentTarget.setPointerCapture(e.pointerId)
  }
  function dragMove(e) {
    if (!drag || drag.id !== e.pointerId) return
    setNavPosition({ x: e.clientX - drag.dx, y: e.clientY - drag.dy })
  }
  function dragEnd(e) {
    if (drag?.id === e.pointerId) drag = null
  }

  // backticks in a step's prose are code, and nothing else is markup
  const parts = (text) => String(text).split('`').map((s, i) => ({ code: i % 2 === 1, s }))
  const plain = (text) => String(text).replaceAll('`', '')
</script>

{#if t && step}
  {#if !tour.minimized}
    {#if rect}
      <div
        class="ring"
        class:done
        aria-hidden="true"
        style={`left:${rect.left - PAD}px; top:${rect.top - PAD}px; width:${rect.width + PAD * 2}px; height:${rect.height + PAD * 2}px`}
      ></div>
    {:else if !step.target}
      <div class="scrim" aria-hidden="true"></div>
    {/if}

    {#key stepKey}
      <div
        class="callout"
        class:done
        role="dialog"
        aria-label={step.title}
        bind:clientWidth={calloutW}
        bind:clientHeight={calloutH}
        style={`left:${placed.x}px; top:${placed.y}px`}
      >
        {#if arrow}
          <span class="arrow {placed.side}" style={`left:${arrow.left}px; top:${arrow.top}px`}></span>
        {/if}
        <div class="kicker small">
          <span>{step.chapter}</span>
          <span class="muted">{tour.step + 1} / {t.steps.length}</span>
        </div>
        <h2>{#each parts(step.title) as p}{#if p.code}<code>{p.s}</code>{:else}{p.s}{/if}{/each}</h2>
        {#each step.body ?? [] as para}
          <p>{#each parts(para) as p}{#if p.code}<code>{p.s}</code>{:else}{p.s}{/if}{/each}</p>
        {/each}
        {#if step.snippet}
          <div class="snippet">
            <pre class="mono">{step.snippet.text}</pre>
            <CopyButton text={step.snippet.text} label={step.snippet.label ?? 'copy'} />
          </div>
        {/if}
        {#if step.do}
          <div class="do" class:done>
            {#if done}
              <Icon name="check" size={12} />
              <span>{step.doneText ?? 'done'}</span>
            {:else}
              <span class="pip" aria-hidden="true"></span>
              <span>{#each parts(step.do) as p}{#if p.code}<code>{p.s}</code>{:else}{p.s}{/if}{/each}</span>
            {/if}
          </div>
        {/if}
        {#if step.target && !rect && step.waiting}
          <p class="small muted waiting">{step.waiting}</p>
        {/if}
      </div>
    {/key}
  {/if}

  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="nav"
    class:mini={tour.minimized}
    bind:clientHeight={navH}
    style={`left:${navX}px; top:${navY}px; width:${NAV_W}px`}
  >
    <div class="navhead" onpointerdown={dragStart} onpointermove={dragMove} onpointerup={dragEnd} onpointercancel={dragEnd}>
      <span class="grip" aria-hidden="true">⠿</span>
      <span class="grow truncate small">{t.title}</span>
      <button
        class="icon"
        title={tour.minimized ? 'show the tutorial' : 'tuck the tutorial away — the page is yours'}
        onclick={() => setMinimized(!tour.minimized)}
      >{tour.minimized ? '▴' : '▾'}</button>
      <button class="icon" title="end the tutorial" onclick={stopTour}>×</button>
    </div>

    {#if !tour.minimized}
      <div class="bars" aria-hidden="true">
        {#each chapters as c}
          <div class="bar" style={`flex:${c.to - c.from + 1} 1 0`}>
            <div
              class="fill"
              style={`width:${tour.step > c.to ? 100 : tour.step < c.from ? 0 : ((tour.step - c.from + 1) / (c.to - c.from + 1)) * 100}%`}
            ></div>
          </div>
        {/each}
      </div>
      <div class="chapterline small">
        {#each chapters as c}
          <button
            class="chap"
            class:on={tour.step >= c.from && tour.step <= c.to}
            style={`flex:${c.to - c.from + 1} 1 0`}
            onclick={() => gotoStep(c.from)}
            title={`jump to ${c.name}`}
          >{c.name}</button>
        {/each}
      </div>

      <button class="steptoggle small" onclick={() => (listOpen = !listOpen)}>
        <span class="muted">{listOpen ? '▾' : '▸'}</span>
        <span class="grow truncate">{tour.step + 1}. {plain(step.title)}</span>
        {#if done}<span class="ok"><Icon name="check" size={11} /></span>{/if}
      </button>
      {#if listOpen}
        <ol class="steps small">
          {#each t.steps as s, i}
            <li>
              <button class:on={i === tour.step} class:past={i < tour.step} onclick={() => gotoStep(i)}>
                <span class="num">{i + 1}</span>
                <span class="truncate">{plain(s.title)}</span>
              </button>
            </li>
          {/each}
        </ol>
      {/if}

      <div class="navbtns">
        <button onclick={prevStep} disabled={tour.step === 0}>‹ prev</button>
        <button class="primary" onclick={nextStep}>
          {tour.step === t.steps.length - 1 ? 'finish' : done || !step.do ? 'next ›' : 'skip ›'}
        </button>
      </div>
    {:else}
      <button class="steptoggle small" onclick={() => setMinimized(false)}>
        <span class="grow truncate">{tour.step + 1}/{t.steps.length} · {plain(step.title)}</span>
      </button>
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
  /* The dim is the ring's own shadow, so the hole in it is exactly the ring and
     the page under both stays clickable. */
  .ring {
    position: fixed;
    z-index: 1000;
    pointer-events: none;
    border: 2px solid var(--accent);
    border-radius: 8px;
    box-shadow: 0 0 0 200vmax var(--tour-scrim);
    transition: left 0.18s, top 0.18s, width 0.18s, height 0.18s, border-color 0.2s;
  }
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
    display: flex;
    justify-content: space-between;
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
  }
  .nav.mini { padding-bottom: 8px; }
  .navhead {
    display: flex;
    align-items: center;
    gap: 6px;
    cursor: grab;
    user-select: none;
    touch-action: none;
  }
  .grip { color: var(--muted); }
  .icon {
    padding: 0 7px;
    background: none;
    border-color: transparent;
    color: var(--muted);
  }
  .icon:hover:not(:disabled) { color: var(--text); background: var(--panel-2); }
  .bars { display: flex; gap: 3px; }
  .bar { min-width: 34px; height: 4px; background: var(--panel-2); border-radius: 2px; overflow: hidden; }
  .fill { height: 100%; background: var(--accent); transition: width 0.25s; }
  .chapterline { display: flex; gap: 3px; margin-top: -4px; }
  .chap {
    padding: 0;
    background: none;
    border: none;
    color: var(--muted);
    font-size: 11px;
    text-align: left;
    min-width: 34px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .chap.on { color: var(--text); }
  .steptoggle {
    display: flex;
    align-items: center;
    gap: 6px;
    text-align: left;
    background: var(--panel-2);
  }
  .ok { color: var(--ok); display: flex; }
  .steps {
    list-style: none;
    margin: 0;
    padding: 0;
    max-height: 240px;
    overflow-y: auto;
  }
  .steps button {
    display: flex;
    gap: 8px;
    width: 100%;
    text-align: left;
    background: none;
    border-color: transparent;
    padding: 2px 6px;
    color: var(--muted);
  }
  .steps button.past { color: var(--text); }
  .steps button.on { background: var(--panel-2); color: var(--text); border-color: var(--accent); }
  .num { width: 18px; flex: 0 0 auto; text-align: right; color: var(--muted); }
  .navbtns { display: flex; justify-content: space-between; gap: 8px; }
  .navbtns button { flex: 1; }
</style>
