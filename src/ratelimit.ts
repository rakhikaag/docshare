// Per-IP daily document caps. All calls run in UploadLedger's serialized queue.
import type { CounterStore } from './ledger'

type DailyQuota = { count: number; bytes: number }

const DAY_TTL_SECONDS = 60 * 60 * 48 // keep a day's counter ~2 days, then expire

function utcDay(d = new Date()): string {
  return d.toISOString().slice(0, 10) // UTC YYYY-MM-DD
}

function quotaKey(tag: string, day = utcDay()): string {
  return `quota:${day}:${tag}`
}

// Stable, non-reversible-ish tag for the uploader's IP. Stored in metadata so a
// later delete can verify "same person who uploaded" before refunding quota,
// without persisting the raw IP. (SHA-256 of an IP isn't strong anonymisation —
// the input space is small — but the tag is never returned to clients.)
export async function hashIp(ip: string): Promise<string> {
  const data = new TextEncoder().encode(`docshare:${ip}`)
  const digest = await crypto.subtle.digest('SHA-256', data)
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('')
}

async function quotaKeyForIp(ip: string): Promise<string> {
  return quotaKey(await hashIp(ip))
}

// Give back one upload's worth of daily quota when a file uploaded *today* is
// deleted by its uploader. "Uploader" means the same IP, or the holder of the
// owner token the upload was made with (callerOwns) — so an agent that moved
// networks can still reclaim what it used. The refund always goes to the
// counter that was charged: the uploader tag is that counter's key. Refusing
// cross-day and non-owner refunds stops quota farming by deleting old or other
// people's files. Best-effort.
export async function refundDaily(
  kv: CounterStore,
  ip: string,
  size: number,
  uploadedAt: number,
  uploaderTag: string | undefined,
  callerOwns = false,
): Promise<void> {
  if (size <= 0 || !Number.isFinite(uploadedAt) || uploadedAt <= 0) return
  if (utcDay(new Date(uploadedAt)) !== utcDay()) return // only today's counter is live
  if (!uploaderTag) return
  if (!callerOwns && (await hashIp(ip)) !== uploaderTag) return // only the original uploader

  await refundKey(kv, quotaKey(uploaderTag), size)
}

// True when deleting an upload made at `uploadedAt` by `uploaderTag` would give
// quota back to the caller's own current counter (callerIpTag = hashIp(ip)).
export function refundsCallerToday(
  callerIpTag: string,
  uploadedAt: number,
  uploaderTag: string | undefined,
): boolean {
  if (!uploaderTag || !Number.isFinite(uploadedAt) || uploadedAt <= 0) return false
  return utcDay(new Date(uploadedAt)) === utcDay() && uploaderTag === callerIpTag
}

// When the daily counters roll over (UTC midnight), as an ISO timestamp.
export function dailyResetAt(now = new Date()): string {
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1))
  return next.toISOString()
}

// Undo a quota charge from the same request/IP after a later upload step fails.
export async function refundChargedDaily(
  kv: CounterStore,
  ip: string,
  size: number,
): Promise<void> {
  await refundKey(kv, await quotaKeyForIp(ip), size)
}

async function refundKey(kv: CounterStore, key: string, size: number): Promise<void> {
  const cur = await readKey(kv, key)
  const next: DailyQuota = {
    count: Math.max(0, cur.count - 1),
    bytes: Math.max(0, cur.bytes - nonNegativeNumber(size)),
  }
  await kv.put(key, JSON.stringify(next), { expirationTtl: DAY_TTL_SECONDS })
}

async function readQuota(kv: CounterStore, ip: string): Promise<DailyQuota> {
  return readKey(kv, await quotaKeyForIp(ip))
}

async function readKey(kv: CounterStore, key: string): Promise<DailyQuota> {
  const raw = await kv.get(key)
  if (!raw) return { count: 0, bytes: 0 }
  try {
    const parsed = JSON.parse(raw) as DailyQuota
    return {
      count: nonNegativeNumber(parsed.count),
      bytes: nonNegativeNumber(parsed.bytes),
    }
  } catch {
    return { count: 0, bytes: 0 }
  }
}

function nonNegativeNumber(value: unknown): number {
  const n = Number(value)
  return Number.isFinite(n) && n > 0 ? n : 0
}

// Read the requester's current daily usage without charging it (for /api/usage).
export async function readDailyUsage(kv: CounterStore, ip: string): Promise<DailyQuota> {
  return readQuota(kv, ip)
}

export type QuotaCheck =
  | { ok: true }
  | { ok: false; reason: 'daily_count' | 'daily_bytes'; limit: number; used: number }

export async function checkDaily(
  kv: CounterStore,
  uploaderTag: string,
  addBytes: number,
  maxCount: number,
  maxBytes: number,
): Promise<QuotaCheck> {
  const cur = await readKey(kv, quotaKey(uploaderTag))
  if (cur.count >= maxCount) {
    return { ok: false, reason: 'daily_count', limit: maxCount, used: cur.count }
  }
  if (cur.bytes + nonNegativeNumber(addBytes) > maxBytes) {
    return { ok: false, reason: 'daily_bytes', limit: maxBytes, used: cur.bytes }
  }
  return { ok: true }
}

export async function chargeDailyTag(
  kv: CounterStore,
  uploaderTag: string,
  addBytes: number,
  maxCount: number,
  maxBytes: number,
): Promise<QuotaCheck> {
  const checked = await checkDaily(kv, uploaderTag, addBytes, maxCount, maxBytes)
  if (!checked.ok) return checked
  const key = quotaKey(uploaderTag)
  const cur = await readKey(kv, key)
  await kv.put(key, JSON.stringify({ count: cur.count + 1, bytes: cur.bytes + nonNegativeNumber(addBytes) }), {
    expirationTtl: DAY_TTL_SECONDS,
  })
  return { ok: true }
}

export async function checkAndChargeDaily(
  kv: CounterStore,
  ip: string,
  addBytes: number,
  maxCount: number,
  maxBytes: number,
): Promise<QuotaCheck> {
  return chargeDailyTag(kv, await hashIp(ip), addBytes, maxCount, maxBytes)
}
