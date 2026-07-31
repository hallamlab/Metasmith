<script>
  import { api } from '../lib/api.svelte.js'

  // Follows a background job over server-sent events. The stream replays what
  // has already happened before following, so opening this late still shows the
  // whole job rather than only what comes next.
  //
  // `header` is off for a caller that draws its own heading around this --
  // the workflow page's plan card puts it behind a `<details>` and reads
  // `status` back (bindable) to paint its own summary's tag, rather than
  // showing "log" twice.
  let { jobId = null, onend, header = true, status = $bindable(null) } = $props()
  let lines = $state([])
  let box = $state(null)

  $effect(() => {
    const id = jobId
    if (!id) return
    lines = []
    status = 'running'
    const stop = api.stream(
      id,
      (line) => {
        lines = [...lines.slice(-2000), line]
        queueMicrotask(() => box && (box.scrollTop = box.scrollHeight))
      },
      (summary) => {
        status = summary?.status ?? 'done'
        onend?.(summary)
      },
    )
    return stop
  })
</script>

{#if jobId}
  <div class="col">
    {#if header}
      <div class="spread">
        <h3>log</h3>
        <span class="tag" class:ok={status === 'done'} class:bad={status === 'failed'}>
          {status ?? ''}
        </span>
      </div>
    {/if}
    <pre class="log" bind:this={box}>{lines.join('\n') || 'waiting…'}</pre>
  </div>
{/if}
