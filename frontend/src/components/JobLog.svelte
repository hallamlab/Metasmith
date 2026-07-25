<script>
  import { streamJob } from '../lib/api.js'

  // Follows a background job over server-sent events. The stream replays what
  // has already happened before following, so opening this late still shows the
  // whole job rather than only what comes next.
  let { jobId = null, onend } = $props()
  let lines = $state([])
  let status = $state(null)
  let box = $state(null)

  $effect(() => {
    const id = jobId
    if (!id) return
    lines = []
    status = 'running'
    const stop = streamJob(
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
    <div class="spread">
      <h3>log</h3>
      <span class="tag" class:ok={status === 'done'} class:bad={status === 'failed'}>
        {status ?? ''}
      </span>
    </div>
    <pre class="log" bind:this={box}>{lines.join('\n') || 'waiting…'}</pre>
  </div>
{/if}
