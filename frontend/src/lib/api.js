// Every call goes through here so a refusal from the backend arrives as a
// message rather than a stack trace. The backend distinguishes "refused"
// (a rule the user hit) from "invalid" (a bad form) from "error" (a bug);
// callers can branch on `kind` when it matters.

export class ApiError extends Error {
  constructor(message, kind, status) {
    super(message)
    this.kind = kind
    this.status = status
  }
}

async function request(method, path, body) {
  const opts = { method, headers: {} }
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json'
    opts.body = JSON.stringify(body)
  }
  const res = await fetch(`/api${path}`, opts)
  const text = await res.text()
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

export const api = {
  get: (p) => request('GET', p),
  post: (p, b) => request('POST', p, b ?? {}),
  put: (p, b) => request('PUT', p, b ?? {}),
  patch: (p, b) => request('PATCH', p, b ?? {}),
  del: (p) => request('DELETE', p),
}

// Follow a background job's log. Returns a stop function.
export function streamJob(jobId, onLine, onEnd) {
  const source = new EventSource(`/api/jobs/${jobId}/stream`)
  source.onmessage = (e) => {
    try {
      onLine(JSON.parse(e.data).line)
    } catch {
      /* a malformed frame is not worth breaking the view over */
    }
  }
  source.addEventListener('end', (e) => {
    source.close()
    try {
      onEnd?.(JSON.parse(e.data))
    } catch {
      onEnd?.(null)
    }
  })
  source.onerror = () => source.close()
  return () => source.close()
}
