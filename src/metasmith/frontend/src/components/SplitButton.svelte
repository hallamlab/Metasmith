<script>
  // One primary action, and beside it the variants worth a second click --
  // forcing past a "that is already done" judgement, usually. The variants live
  // behind a chevron rather than as buttons of their own because they are the
  // rarer answer to the same question the primary button asks.
  let { label, disabled = false, onclick, options = [], title = null } = $props()

  let open = $state(false)
  let root = $state(null)

  // outside click closes the menu, the same gesture ParentPicker uses
  $effect(() => {
    if (!open) return
    const away = (e) => {
      if (!root?.contains(e.target)) open = false
    }
    window.addEventListener('pointerdown', away, true)
    return () => window.removeEventListener('pointerdown', away, true)
  })
</script>

<div class="split" bind:this={root}>
  <button class="primary" {onclick} {disabled} {title}>{label}</button>
  <button
    class="primary chevron"
    aria-expanded={open}
    aria-label="more options"
    title="more options"
    {disabled}
    onclick={() => (open = !open)}
  >
    <svg viewBox="0 0 10 6" width="10" height="6" aria-hidden="true" class:up={open}>
      <path d="M1 1L5 5L9 1" fill="none" stroke="currentColor" stroke-width="1.6"
            stroke-linecap="round" stroke-linejoin="round" />
    </svg>
  </button>
  {#if open}
    <div class="menu">
      {#each options as opt}
        <button
          class="opt"
          title={opt.title}
          onclick={() => { open = false; opt.onclick() }}
        >{opt.label}</button>
      {/each}
    </div>
  {/if}
</div>

<style>
  .split {
    position: relative;
    display: flex;
  }
  .split .primary:first-child {
    border-right: none;
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
  }
  .split .chevron {
    padding: 0 6px;
    border-top-left-radius: 0;
    border-bottom-left-radius: 0;
  }
  .split .chevron svg { transition: transform 0.12s; }
  .split .chevron svg.up { transform: rotate(180deg); }
  .split .menu {
    position: absolute;
    z-index: 30;
    top: 100%;
    right: 0;
    margin-top: 4px;
    min-width: 150px;
    background: var(--panel);
    border: 1px solid var(--accent);
    border-radius: var(--radius);
    box-shadow: 0 10px 24px var(--shadow);
    overflow: hidden;
  }
  .split .opt {
    display: block;
    width: 100%;
    background: none;
    border: none;
    border-radius: 0;
    padding: 6px 10px;
    text-align: left;
  }
  .split .opt:hover { background: var(--panel-2); border-color: transparent; }
</style>
