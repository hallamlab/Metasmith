<script>
  import { api } from '../lib/api.js'
  import { app, attempt, loadSsh } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'

  let { alias } = $props()

  let host = $derived(app.hosts.find((h) => h.alias === alias) ?? null)
  let draft = $state({ hostname: '', user: '', port: '', proxy_jump: '' })
  let lastLoaded = $state(null)

  $effect(() => {
    if (host && lastLoaded !== host.alias) {
      lastLoaded = host.alias
      draft = {
        hostname: host.hostname ?? '',
        user: host.user ?? '',
        port: host.port ?? '',
        proxy_jump: host.proxy_jump ?? '',
      }
    }
  })

  async function save() {
    await attempt(async () => {
      await api.patch(`/ssh/hosts/${alias}`, draft)
      await loadSsh()
    })
  }
</script>

{#if !host}
  <p class="muted">no host named [{alias}]</p>
{:else}
  <div class="col" style="gap:14px; max-width:640px">
    <div class="spread">
      <h1>{host.alias}</h1>
      <span class="tag" class:ok={host.managed}>
        {host.managed ? 'managed by metasmith' : 'yours'}
      </span>
    </div>
    <p class="small muted mono">{host.source}:{host.line}</p>

    {#if !host.managed}
      <div class="card col">
        <p class="small">
          This host is defined outside the metasmith block, so it is shown but not
          edited here — metasmith never rewrites an entry it does not own. Change
          it in the config editor, or in your editor of choice.
        </p>
        <table class="mono small">
          <tbody>
            {#each Object.entries(host.keywords) as [k, v]}
              <tr><td class="muted">{k}</td><td>{v}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else}
      <div class="card col" style="gap:10px">
        <Field label="hostname">
          <input bind:value={draft.hostname} placeholder="host.example.org" />
        </Field>
        <Field label="user">
          <input bind:value={draft.user} placeholder="(your local username)" />
        </Field>
        <Field label="port">
          <input bind:value={draft.port} placeholder="22" />
        </Field>
        <Field label="jump host" hint="an alias to bounce through, if this host is not reachable directly">
          <input bind:value={draft.proxy_jump} placeholder="(none)" />
        </Field>
        <div class="row">
          <button class="primary" onclick={save}>save</button>
          <span class="small muted">anything beyond these four fields belongs in the config editor</span>
        </div>
      </div>
    {/if}
  </div>
{/if}
