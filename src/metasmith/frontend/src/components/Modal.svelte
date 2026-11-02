<script>
  // The dialog shell: a scrim, a card, a heading, and the two ways out that a
  // dialog has to have. Lifted out of the new-workflow modal when sharing
  // needed the same thing twice more -- the parts that differ between the three
  // are the body and the buttons, and everything else was going to be copied.
  let { title, subtitle = '', wide = false, onclose, children, footer } = $props()

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
    class:wide
    role="dialog"
    tabindex="-1"
    aria-modal="true"
    aria-label={title}
    onclick={(e) => e.stopPropagation()}
  >
    <div class="spread">
      <h2>{title}</h2>
      {#if subtitle}<span class="small muted">{subtitle}</span>{/if}
    </div>

    {@render children?.()}

    <div class="row end">{@render footer?.()}</div>
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
  .sheet.wide {
    width: min(900px, 100%);
  }
  .end {
    justify-content: flex-end;
  }
</style>
