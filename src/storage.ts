// Global storage accounting. UploadLedger serializes checks, writes, deletes,
// and reconciliation so concurrent uploads cannot lose counter updates.
// Direct-to-R2 staging objects are not published until finalize accepts them.
import type { CounterStore } from './ledger'

const STORAGE_KEY = 'storage:used'

// Don't scan unbounded numbers of objects in one cron tick. Per-IP daily count
// caps bound how many objects can be created, so a healthy bucket stays well
// under this; if we ever blow past it we keep the incremental counter rather
// than writing a truncated (too-low) sum.
const MAX_RECONCILE_PAGES = 1000

export async function getStorageUsed(kv: CounterStore): Promise<number> {
  const raw = await kv.get(STORAGE_KEY)
  const n = raw ? Number(raw) : 0
  return Number.isFinite(n) && n > 0 ? n : 0
}

// Called only from the ledger's serialized request queue.
export async function addStorageUsed(kv: CounterStore, delta: number): Promise<void> {
  if (!Number.isFinite(delta) || delta === 0) return
  const cur = await getStorageUsed(kv)
  const next = Math.max(0, cur + delta)
  await kv.put(STORAGE_KEY, String(next))
}

// True when storing `addBytes` more would stay within the cap.
export async function withinStorageCap(
  kv: CounterStore,
  addBytes: number,
  cap: number,
): Promise<boolean> {
  if (!Number.isFinite(cap) || cap <= 0) return true // cap disabled
  const used = await getStorageUsed(kv)
  return used + addBytes <= cap
}

// Re-sum the whole bucket and overwrite the counter. Called from the cron.
export async function reconcileStorage(kv: CounterStore, bucket: R2Bucket): Promise<void> {
  let total = 0
  let cursor: string | undefined
  let pages = 0
  do {
    const listed = await bucket.list({ cursor, limit: 1000 })
    for (const obj of listed.objects) {
      if (obj.key.startsWith('doc/') || (obj.key.startsWith('img/') && (obj.key.endsWith('.webp') || obj.key.endsWith('.png')))) {
        total += obj.size
      }
    }
    cursor = listed.truncated ? listed.cursor : undefined
    pages++
    if (pages >= MAX_RECONCILE_PAGES && cursor) {
      // Bucket is larger than we're willing to scan — keep the incremental
      // counter (which over-counts, the safe direction) rather than writing a
      // partial sum that would under-count and let abuse through.
      console.warn(`reconcileStorage: bucket exceeds ${MAX_RECONCILE_PAGES} pages, keeping incremental counter`)
      return
    }
  } while (cursor)
  await kv.put(STORAGE_KEY, String(total))
}
