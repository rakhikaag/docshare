// Counters are strongly consistent inside the single UploadLedger object.
// Its request queue owns the complete check/write/refund operation, including R2.
export type CounterStore = {
  get(key: string): Promise<string | null>
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>
}

type Entry = { value: string; expiresAt?: number }

export function durableCounters(storage: DurableObjectStorage): CounterStore {
  return {
    async get(key) {
      const entry = await storage.get<Entry>(key)
      if (!entry || (entry.expiresAt !== undefined && entry.expiresAt <= Date.now())) return null
      return entry.value
    },
    async put(key, value, options) {
      await storage.put(key, {
        value,
        ...(options?.expirationTtl ? { expiresAt: Date.now() + options.expirationTtl * 1000 } : {}),
      })
    },
  }
}

export async function pruneCounters(storage: DurableObjectStorage): Promise<void> {
  let startAfter: string | undefined
  do {
    const entries = await storage.list<Entry>({ prefix: 'quota:', limit: 1000, startAfter })
    const expired = [...entries].filter(([, entry]) => entry.expiresAt !== undefined && entry.expiresAt <= Date.now())
    if (expired.length) await storage.delete(expired.map(([key]) => key))
    startAfter = entries.size === 1000 ? [...entries.keys()].at(-1) : undefined
  } while (startAfter)
}
