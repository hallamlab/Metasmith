<script>
  // The icon-swap clipboard button used beside a path someone will want to
  // paste elsewhere -- the project root, a run's staged directory, its
  // results, an agent's resolved home. One copy of the gesture rather than
  // one per page: `navigator.clipboard` first, a throwaway `<textarea>` +
  // `execCommand('copy')` when permission is refused (it can be, even on
  // localhost), and a check mark that reverts on its own.
  import Icon from './Icon.svelte'

  let { text, label = 'copy' } = $props()

  let copied = $state(false)

  async function copy() {
    if (!text) return
    try {
      await navigator.clipboard.writeText(text)
    } catch {
      const ta = document.createElement('textarea')
      ta.value = text
      ta.style.cssText = 'position:fixed;opacity:0'
      document.body.appendChild(ta)
      ta.select()
      document.execCommand('copy')
      ta.remove()
    }
    copied = true
    setTimeout(() => (copied = false), 1200)
  }
</script>

<button
  class="copy"
  onclick={copy}
  title={copied ? 'copied' : label}
  aria-label={label}
>
  <Icon name={copied ? 'check' : 'copy'} size={13} />
</button>

<style>
  .copy {
    flex: 0 0 auto;
    display: flex;
    padding: 5px;
    background: none;
    border-color: transparent;
    color: var(--muted);
  }
  .copy:hover { color: var(--text); background: var(--panel-2); }
</style>
