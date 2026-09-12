<script>
  // Every time on the page, printed the one way. The delta is what is read and
  // the exact stamp is one hover away -- a tooltip rather than a second column,
  // because these sit in rail rows and table cells that are already one line.
  import { ago, clock, stamp } from '../lib/time.svelte.js'

  let { iso = null } = $props()

  // reading the shared clock is the subscription: it moves once every half
  // minute and every mounted <Ago> re-renders, so "just now" becomes
  // "1 minute ago" without anyone reloading the page
  let text = $derived(ago(iso, clock.now))
</script>

<span title={stamp(iso)}>{text}</span>
