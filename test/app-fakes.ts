// A fuller in-memory R2 bucket and Worker env, enough to drive src/index.ts's
// routes through app.fetch without a Workers runtime.
import { UploadLedger } from '../src/index'

type Stored = {
  bytes: Uint8Array
  contentType?: string
  customMetadata?: Record<string, string>
  uploaded: Date
}

function objectFor(key: string, s: Stored) {
  return {
    key,
    size: s.bytes.byteLength,
    uploaded: s.uploaded,
    customMetadata: s.customMetadata,
    httpEtag: `"${key}"`,
    writeHttpMetadata(headers: Headers) {
      if (s.contentType) headers.set('content-type', s.contentType)
    },
  }
}

export class MemoryR2 {
  store = new Map<string, Stored>()

  async put(
    key: string,
    value: ArrayBuffer | string,
    opts?: { httpMetadata?: { contentType?: string }; customMetadata?: Record<string, string> },
  ) {
    const bytes = typeof value === 'string' ? new TextEncoder().encode(value) : new Uint8Array(value)
    this.store.set(key, {
      bytes,
      contentType: opts?.httpMetadata?.contentType,
      customMetadata: opts?.customMetadata,
      uploaded: new Date(),
    })
  }

  async head(key: string) {
    const s = this.store.get(key)
    return s ? objectFor(key, s) : null
  }

  async get(key: string) {
    const s = this.store.get(key)
    if (!s) return null
    const text = new TextDecoder().decode(s.bytes)
    return {
      ...objectFor(key, s),
      body: s.bytes,
      text: async () => text,
      json: async () => JSON.parse(text),
      arrayBuffer: async () => s.bytes.buffer.slice(s.bytes.byteOffset, s.bytes.byteOffset + s.bytes.byteLength),
    }
  }

  async delete(key: string) {
    this.store.delete(key)
  }

  async list(opts?: { prefix?: string; cursor?: string; limit?: number }) {
    const limit = opts?.limit ?? 1000
    const keys = [...this.store.keys()].filter((k) => !opts?.prefix || k.startsWith(opts.prefix)).sort()
    const start = opts?.cursor ? Number(opts.cursor) : 0
    const page = keys.slice(start, start + limit)
    const next = start + page.length
    const truncated = next < keys.length
    return {
      objects: page.map((k) => objectFor(k, this.store.get(k)!)),
      truncated,
      cursor: truncated ? String(next) : undefined,
    }
  }
}

// The namespace drives the real ledger queue and storage adapter. Only the
// Cloudflare storage transport is substituted, not the admission logic.
class MemoryDurableStorage {
  store = new Map<string, unknown>()

  async get<T>(key: string): Promise<T | undefined> {
    return this.store.get(key) as T | undefined
  }

  async put(key: string, value: unknown): Promise<void> {
    this.store.set(key, structuredClone(value))
  }

  async list<T>(opts: { prefix?: string; limit?: number; startAfter?: string } = {}): Promise<Map<string, T>> {
    return new Map([...this.store].sort(([a], [b]) => a.localeCompare(b))
      .filter(([key]) => (!opts.prefix || key.startsWith(opts.prefix)) && (!opts.startAfter || key > opts.startAfter))
      .slice(0, opts.limit ?? 1000) as Array<[string, T]>)
  }

  async delete(keys: string[]): Promise<number> {
    let deleted = 0
    for (const key of keys) if (this.store.delete(key)) deleted++
    return deleted
  }
}

const allow = { limit: async () => ({ success: true }) }

export function makeEnv(overrides: Record<string, string> = {}) {
  const env = {
    BUCKET: new MemoryR2(),
    LEDGER: {} as DurableObjectNamespace,
    UPLOAD_LIMITER: allow,
    DOC_LIMITER: allow,
    OCR_LIMITER: allow,
    GEMINI_API_KEY: '',
    GEMINI_MODEL: 'test',
    PUBLIC_ORIGIN: 'https://docs.example',
    TTL_HOURS: '24',
    MAX_UPLOAD_BYTES: '15728640',
    MAX_DOC_BYTES: '419430400',
    MAX_ADMIN_DOC_BYTES: '1572864000',
    DOC_DAILY_COUNT: '3',
    DOC_DAILY_BYTES: '1000000',
    MAX_TOTAL_BYTES: '1000000',
    R2_ACCOUNT_ID: 'acct',
    R2_BUCKET_NAME: 'bucket',
    R2_ACCESS_KEY_ID: 'key',
    R2_SECRET_ACCESS_KEY: 'secret',
    ADMIN_KEY: 'admin-secret',
    ...overrides,
  }
  const storage = new MemoryDurableStorage()
  let ready = Promise.resolve()
  const state = {
    storage,
    blockConcurrencyWhile(fn: () => Promise<void>) { ready = fn(); return ready },
  }
  const ledger = new UploadLedger(state as unknown as DurableObjectState, env as never)
  env.LEDGER = {
    idFromName: () => 'uploads',
    get: () => ({
      async fetch(input: Request | string) {
        await ready
        return ledger.fetch(typeof input === 'string' ? new Request(input) : input)
      },
    }),
  } as unknown as DurableObjectNamespace
  return env
}
