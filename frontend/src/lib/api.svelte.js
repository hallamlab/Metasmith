// Every call to the server goes through the one service below, and that is what
// makes the header's dot possible: a request cannot reach the backend without
// passing through a place that can say so. A refusal arrives as a message
// rather than a stack trace -- the backend distinguishes "refused" (a rule the
// user hit) from "invalid" (a bad form) from "error" (a bug), and callers can
// branch on `kind` when it matters -- and a request that never arrives at all
// is a fourth kind, "offline", which no route can report because no route ran.
//
// The module is `.svelte.js` because the service holds `$state`: components
// read `api.status` directly and re-render when it moves. Nothing subscribes by
// hand, and nothing else in the app owns a copy of it.

export class ApiError extends Error {
  constructor(message, kind, status) {
    super(message)
    this.kind = kind
    this.status = status
  }
}

// what counts as sending information rather than asking for it
const MUTATIONS = new Set(['POST', 'PUT', 'PATCH', 'DELETE'])

// A save that returns instantly -- most of them do -- would otherwise light the
// dot for one frame, which is a light nobody sees.
const MIN_SAVING_MS = 200

// how often the heartbeat asks whether the server is still there
const PULSE_MS = 5000

export class ApiService {
  // Optimistic on first paint: the page's own first load answers within a
  // moment, and starting red would mean every reload flashes an alarm that is
  // almost always wrong.
  online = $state(true)
  saving = $state(false)

  /** Called when the server starts answering again after having gone silent.
   *
   * The dot recovers by itself -- the heartbeat sets `online` back -- but the
   * *message* a failed request left on screen does not, so the page could sit
   * there reading `connected` beside a banner saying the server was not
   * answering. Whoever owns the notice registers here and clears its own. */
  onrecover = null

  #pending = 0
  #startedAt = 0
  #timer = null

  /** connected | saving | offline -- green, yellow, red on the dot. */
  get status() {
    return this.saving ? 'saving' : this.online ? 'connected' : 'offline'
  }

  // -- the calls -------------------------------------------------------------

  get = (p) => this.#request('GET', p)
  post = (p, b) => this.#request('POST', p, b ?? {})
  put = (p, b) => this.#request('PUT', p, b ?? {})
  patch = (p, b) => this.#request('PATCH', p, b ?? {})
  del = (p) => this.#request('DELETE', p)

  /** POST a file. The one route that takes bytes rather than a json object;
   * the browser sets the multipart content type itself, boundary and all. */
  upload = (p, form) => this.#request('POST', p, form)

  async #request(method, path, body) {
    const opts = { method, headers: {} }
    if (body instanceof FormData) {
      opts.body = body
    } else if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json'
      opts.body = JSON.stringify(body)
    }
    const sending = MUTATIONS.has(method)
    if (sending) this.#beginSend()
    let res
    let text
    try {
      res = await fetch(`/api${path}`, opts)
      text = await res.text()
    } catch {
      // fetch only rejects when the request never completed: the server is
      // gone, or the connection died mid-body. An HTTP error is not this.
      this.online = false
      throw new ApiError('the metasmith server is not answering', 'offline', 0)
    } finally {
      if (sending) this.#endSend()
    }
    // a reply of any status is proof the server answered -- a 409 refusal and a
    // 500 are both the server talking, not silence
    this.#markOnline()

    let data = null
    if (text) {
      try {
        data = JSON.parse(text)
      } catch {
        data = { error: text }
      }
    }
    if (!res.ok) {
      throw new ApiError(data?.error ?? res.statusText, data?.kind ?? 'error', res.status)
    }
    return data
  }

  /** Follow a background job's log. Returns a stop function. */
  stream(jobId, onLine, onEnd) {
    const source = new EventSource(`/api/jobs/${jobId}/stream`)
    source.onmessage = (e) => {
      try {
        onLine(JSON.parse(e.data).line)
      } catch {
        /* a malformed frame is not worth breaking the view over */
      }
    }
    let ended = false
    source.addEventListener('end', (e) => {
      ended = true
      source.close()
      try {
        onEnd?.(JSON.parse(e.data))
      } catch {
        onEnd?.(null)
      }
    })
    // A stream that ends *after* its job has is not evidence about anything:
    // that is how every job ends. One that breaks before it is -- and it used
    // to be swallowed, which left the log stuck on `running` for a job that was
    // no longer being followed. The reconnect EventSource does by itself is not
    // enough here, since the job may have finished while we were away.
    source.onerror = () => {
      source.close()
      if (ended) return
      this.pulse()
      onEnd?.({ status: 'unknown', error: 'lost the connection to the log' })
    }
    return () => {
      ended = true
      source.close()
    }
  }

  // -- the dot's two inputs --------------------------------------------------

  // Overlapping saves share one window: the dot goes back to green when the
  // last of them has landed and the newest has had its 200ms.
  #beginSend() {
    this.#pending += 1
    this.#startedAt = Date.now()
    if (this.#timer) {
      clearTimeout(this.#timer)
      this.#timer = null
    }
    this.saving = true
  }

  #endSend() {
    this.#pending -= 1
    if (this.#pending > 0) return
    const wait = Math.max(0, MIN_SAVING_MS - (Date.now() - this.#startedAt))
    if (this.#timer) clearTimeout(this.#timer)
    this.#timer = setTimeout(() => {
      this.#timer = null
      // another save may have started while this one was cooling off
      if (this.#pending === 0) this.saving = false
    }, wait)
  }

  #markOnline() {
    const wasOffline = !this.online
    this.online = true
    if (wasOffline) this.onrecover?.()
  }

  /** Ask whether the server is still there. A GET, so it never turns the dot
   * yellow, and the cheapest route in the backend so a timer can hold it. */
  async pulse() {
    try {
      await fetch('/api/health', { cache: 'no-store' })
      this.#markOnline()
    } catch {
      this.online = false
    }
  }

  /** Poll `pulse` for as long as the page is open. Returns a stop function.
   *
   * Without it the dot would only ever be as fresh as the last thing the user
   * clicked: a server killed in its terminal would keep reading green until
   * someone tried to save into it.
   */
  watch(interval = PULSE_MS) {
    const tick = () => {
      // a backgrounded tab is not worth a request; the visibility handler
      // catches up the moment it is looked at again
      if (document.visibilityState === 'visible') this.pulse()
    }
    tick()
    const id = setInterval(tick, interval)
    document.addEventListener('visibilitychange', tick)
    return () => {
      clearInterval(id)
      document.removeEventListener('visibilitychange', tick)
    }
  }
}

// one service for the whole page
export const api = new ApiService()
