/**
 * The boot-time history import must be bounded and cancellable.
 *
 * The old backfill called `readSession` once per missing session; each call
 * re-listed the ENTIRE artifact tree, and its per-read race timed out without
 * aborting, so a large home pegged a core for tens of minutes with work
 * piling up behind the deadline. The replacement lists the tree once, then
 * borrows each session's own artifact through `observeSession` with a bounded
 * worker pool, and aborts the whole pass as a unit when the
 * `backfillTimeoutMs` budget runs out.
 *
 * Run: node tests/backfill.test.js
 */
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const HOME = path.join(os.tmpdir(), 'dsh-bill-backfill-test')
fs.rmSync(HOME, { recursive: true, force: true })
process.env.DSH_HOME = HOME
const RECORDS_FILE = path.join(HOME, 'dsh-bill', 'records.jsonl')

const { default: plugin, Config } = await import('../lib/index.js')

let failed = 0
function assert(cond, msg) {
  if (cond) console.log('  ok -', msg)
  else { failed++; console.error('  FAIL -', msg) }
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
async function waitFor(fn, ms = 10000) {
  const start = Date.now()
  for (;;) {
    const value = fn()
    if (value) return value
    if (Date.now() - start > ms) throw new Error('timed out waiting for a condition')
    await sleep(25)
  }
}

// ── config schema ────────────────────────────────────────────────────────────
console.log('config validation')
{
  const def = Config['~standard'].validate(undefined)
  assert(def.value?.maxRecords === 20000, 'maxRecords default preserved')
  assert(def.value?.backfillTimeoutMs === 60000, 'backfillTimeoutMs defaults to 60000')
  const ok = Config['~standard'].validate({ backfillTimeoutMs: 12345, maxRecords: 50000 })
  assert(!ok.issues && ok.value.backfillTimeoutMs === 12345 && ok.value.maxRecords === 50000, 'backfillTimeoutMs and maxRecords accepted in range')
  const zero = Config['~standard'].validate({ backfillTimeoutMs: 0 })
  assert(!zero.issues && zero.value.backfillTimeoutMs === 0, 'backfillTimeoutMs: 0 disables the import')
  for (const bad of [-1, 1.5, 'soon', NaN]) {
    const r = Config['~standard'].validate({ backfillTimeoutMs: bad })
    assert(
      r.issues?.length === 1 && r.issues[0].path?.[0] === 'backfillTimeoutMs',
      `backfillTimeoutMs rejected: ${String(bad)}`,
    )
  }
  for (const bad of [0, 9, 1000001, 1.5, 'soon', NaN]) {
    const r = Config['~standard'].validate({ maxRecords: bad })
    assert(
      r.issues?.length === 1 && r.issues[0].path?.[0] === 'maxRecords',
      `maxRecords rejected outside 10..1000000: ${String(bad)}`,
    )
  }
}

// ── harness ──────────────────────────────────────────────────────────────────
/**
 * One assistant step with usage, so exactly one sample per session.
 * request/header sets a fallback model/provider; the message source overrides
 * them (the same "later report wins" logic the live path uses).
 */
const USAGE_EVENTS = [
  { type: 'request/header', seq: 0, time: 1000, data: { header: { config: { model: 'm-fallback', provider: 'p-fallback' } } } },
  {
    type: 'assistant/message', seq: 1, time: 1000,
    data: {
      turn: 0, step: 0,
      usage: { inputTokens: 10, outputTokens: 20, cacheReadTokens: 2, cacheWriteTokens: 1, reasoningTokens: 3 },
      message: { source: { provider: 'p-src', model: 'm-src' } },
    },
  },
]

/**
 * Minimal session-query with the two methods the new backfill uses.
 * `listings` counts listing calls so a test can prove the pass ran. With
 * `hang`, `observeSession` never settles on its own — it rejects only when
 * the AbortSignal fires, mirroring the real service's observation lease.
 */
function fakeSessionQuery(sessions, { hang = false, rejectIds = [] } = {}) {
  const listings = { count: 0 }
  return {
    listings,
    async listSessions(signal) {
      signal?.throwIfAborted()
      listings.count++
      return sessions.map((s) => ({ header: { id: s.id } }))
    },
    async observeSession(id, { signal } = {}) {
      signal?.throwIfAborted()
      if (hang) {
        return new Promise((_, reject) => {
          signal?.addEventListener('abort', () => reject(new Error('aborted', { cause: 'abort' })), { once: true })
        })
      }
      if (rejectIds.includes(id)) throw new Error('corrupt')
      const session = sessions.find((s) => s.id === id)
      return {
        events: session?.events ?? [],
        inheritedEventCount: session?.inheritedEventCount,
        [Symbol.dispose]() {},
      }
    },
  }
}

/** Boot one plugin instance against the shared temp HOME. */
function boot(config, sessionQuery) {
  const services = { sessionQuery, webServer: { register(route) { this.handler = route && route.handler } } }
  const ctx = {
    ...services,
    effect: (fn) => fn(),
    get: (name) => services[name],
    on: () => {},
    inject: (names, apply) => { if (names.every((n) => services[n])) apply(ctx) },
  }
  plugin.apply(ctx, config)
  return ctx
}

/** Invoke the plugin HTTP dispatch in-process (no real socket in the test). */
function mockReq(payload) {
  const text = JSON.stringify(payload)
  return {
    on(ev, cb) { if (ev === 'data') cb(text); else if (ev === 'end') cb() },
    destroy() {},
  }
}
async function callApi(ctx, payload) {
  const web = ctx.get('webServer')
  let result
  const res = { writeHead() {}, end(text) { result = JSON.parse(text) } }
  await web.handler(mockReq(payload), res)
  return result
}

const readRecords = () => {
  if (!fs.existsSync(RECORDS_FILE)) return []
  return fs.readFileSync(RECORDS_FILE, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line))
}

// ── import: one pass, one listing, one record per usage sample ──────────────
console.log('history import')
{
  const sessions = [
    { id: 'sess-a', events: USAGE_EVENTS },
    { id: 'sess-b', events: USAGE_EVENTS },
    { id: 'sess-empty', events: [] },
  ]
  const sq = fakeSessionQuery(sessions)
  boot({ maxRecords: 10 }, sq)
  const records = await waitFor(() => {
    const r = readRecords()
    return r.filter((x) => x.sessionId === 'sess-a').length === 1 && r.filter((x) => x.sessionId === 'sess-b').length === 1 ? r : null
  })
  assert(sq.listings.count === 1, 'the whole pass needed exactly one artifact listing')
  const a = records.find((r) => r.sessionId === 'sess-a')
  assert(a.source === 'log', 'imported record is marked source:log')
  assert(a.model === 'm-src' && a.provider === 'p-src', 'message source wins over the header fallback')
  assert(a.inputTokens === 10 && a.outputTokens === 20, 'input/output token counts imported')
  assert(a.cacheReadTokens === 2 && a.cacheWriteTokens === 1 && a.reasoningTokens === 3, 'cache and reasoning counts imported')
  assert(!records.some((r) => r.sessionId === 'sess-empty'), 'a session with no usage imports nothing')

  // Idempotency: a second boot on the same home imports nothing new, because
  // bySession is rebuilt from the records file before backfill runs.
  const before = readRecords().length
  const sq2 = fakeSessionQuery([{ id: 'sess-a', events: USAGE_EVENTS }])
  boot({ maxRecords: 10 }, sq2)
  await waitFor(() => (sq2.listings.count >= 1 ? true : null))
  await sleep(400)
  assert(readRecords().length === before, 'second boot imports nothing (bySession covers the session)')
}

// ── per-session rejection: corrupt sessions are skipped, not fatal ───────────
console.log('per-session rejection')
{
  const sq = fakeSessionQuery(
    [{ id: 'sess-ok', events: USAGE_EVENTS }, { id: 'sess-bad', events: USAGE_EVENTS }],
    { rejectIds: ['sess-bad'] },
  )
  boot({ maxRecords: 10 }, sq)
  const records = await waitFor(() => {
    const r = readRecords()
    return r.some((x) => x.sessionId === 'sess-ok') ? r : null
  })
  assert(!records.some((r) => r.sessionId === 'sess-bad'), 'a rejected session is skipped without failing the pass')
}

// ── budget exhaustion: the pass aborts as a unit, imports nothing ────────────
console.log('budget exhaustion')
{
  const warns = []
  const realWarn = console.warn
  console.warn = (...args) => warns.push(args.join(' '))
  let sq
  try {
    sq = fakeSessionQuery([{ id: 'sess-hang', events: USAGE_EVENTS }], { hang: true })
    boot({ maxRecords: 10, backfillTimeoutMs: 150 }, sq)
    await waitFor(() => (sq.listings.count >= 1 ? true : null))
    await sleep(700)
  } finally {
    console.warn = realWarn
  }
  assert(warns.some((w) => w.includes('backfill') && w.includes('budget')), 'budget exhaustion is reported')
  assert(!readRecords().some((r) => r.sessionId === 'sess-hang'), 'nothing is imported after an abort')
}

// ── missing observeSession: older hosts skip the import gracefully ──────────
console.log('missing observeSession')
{
  const sq = fakeSessionQuery([{ id: 'sess-a', events: USAGE_EVENTS }])
  delete sq.observeSession
  const before = readRecords().length
  boot({ maxRecords: 10 }, sq)
  // The guard returns before listing, so this must neither hang nor list.
  await sleep(500)
  assert(sq.listings.count === 0, 'a session-query without observeSession is never listed')
  assert(readRecords().length === before, 'a session-query without observeSession skips the import')
}

// ── disabled via 0: the import is off entirely, not just bounded ────────────
console.log('disabled via 0')
{
  const sq = fakeSessionQuery([{ id: 'sess-a', events: USAGE_EVENTS }])
  const before = readRecords().length
  boot({ backfillTimeoutMs: 0 }, sq)
  await sleep(500)
  assert(sq.listings.count === 0, 'a zero budget never lists the artifact tree')
  assert(readRecords().length === before, 'a zero budget imports nothing')
}

// ── prefs drive the knobs: a settings-page value overrides the config ───────
console.log('prefs drive the knobs')
{
  const before = readRecords().length
  fs.writeFileSync(path.join(HOME, 'dsh-bill', 'prefs.json'), JSON.stringify({ backfillTimeoutMs: 0 }))
  const sq = fakeSessionQuery([{ id: 'sess-pref', events: USAGE_EVENTS }])
  boot({ backfillTimeoutMs: 60000 }, sq)
  await sleep(500)
  assert(sq.listings.count === 0, 'prefs backfillTimeoutMs: 0 disables the import even with a positive config')
  assert(readRecords().length === before, 'a prefs-disabled pass imports nothing')
}

// ── eviction safety: a fully-evicted session is not re-imported ──────────────
console.log('evicted session not re-imported')
{
  // `bySession` indexes only the live ring, so once a session's every record
  // is evicted into the rollup it disappears from the guard. Without the
  // knownSessions marker the next boot's backfill would pull the same calls
  // in again and fold them a second time — doubling the rollup's totals.
  // sess-evict has the earliest time, so with maxRecords:5 and 8 later
  // sessions it is the first of four records folded into the rollup.
  const HOME3 = path.join(os.tmpdir(), 'dsh-bill-backfill-test-evict')
  fs.rmSync(HOME3, { recursive: true, force: true })
  process.env.DSH_HOME = HOME3
  const rollupPath = () => path.join(HOME3, 'dsh-bill', 'rollup.json')
  const readRollup = () => fs.existsSync(rollupPath()) ? JSON.parse(fs.readFileSync(rollupPath(), 'utf8')) : {}

  const early = [
    { type: 'request/header', seq: 0, time: 100, data: { header: { config: { model: 'm', provider: 'p' } } } },
    { type: 'assistant/message', seq: 1, time: 100, data: { turn: 0, step: 0, usage: { inputTokens: 10, outputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0, reasoningTokens: 0 }, message: { source: { provider: 'p', model: 'm' } } } },
  ]
  const later = () => [
    { type: 'request/header', seq: 0, time: 200, data: { header: { config: { model: 'm', provider: 'p' } } } },
    { type: 'assistant/message', seq: 1, time: 200, data: { turn: 0, step: 0, usage: { inputTokens: 10, outputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0, reasoningTokens: 0 }, message: { source: { provider: 'p', model: 'm' } } } },
  ]
  const sessions = [{ id: 'sess-evict', events: early }]
  for (let i = 0; i < 8; i++) sessions.push({ id: `sess-fill-${i}`, events: later() })

  const sq = fakeSessionQuery(sessions)
  boot({ maxRecords: 5 }, sq)
  await waitFor(() => (sq.listings.count >= 1 ? true : null))
  await waitFor(() => (readRollup().calls === 4 ? true : null), 5000)
  assert(readRollup().calls === 4, 'four calls folded into the rollup on the first boot')
  assert(Array.isArray(readRollup().knownSessions) && readRollup().knownSessions.includes('sess-evict'),
    'the evicted session is recorded in rollup.knownSessions')
  const foldedBefore = readRollup().calls

  // Reboot: bySession holds only the five surviving fill sessions, so
  // sess-evict (and the three evicted fill sessions) are invisible to the
  // `bySession.has(id)` guard. Only knownSessions can stop a re-import.
  const sq2 = fakeSessionQuery(sessions)
  boot({ maxRecords: 5 }, sq2)
  await waitFor(() => (sq2.listings.count >= 1 ? true : null))
  await sleep(600)
  assert(readRollup().calls === foldedBefore,
    `the rollup did not double-count after reboot (${foldedBefore} → ${readRollup().calls})`)
}

// ── fork: the inherited prefix is not re-billed under the child ─────────────
console.log('forked session inherits the parent prefix without double-billing')
{
  // A forked (seeded) session opens with the parent's events as an inherited
  // prefix. The parent's backfill already bills those calls; importing them
  // again here would double-count. Only the child's OWN events (after the
  // inheritedEventCount cut) should produce records.
  const HOME4 = path.join(os.tmpdir(), 'dsh-bill-backfill-test-fork')
  fs.rmSync(HOME4, { recursive: true, force: true })
  process.env.DSH_HOME = HOME4
  const forkFile = path.join(HOME4, 'dsh-bill', 'records.jsonl')
  const readFork = () => fs.existsSync(forkFile)
    ? fs.readFileSync(forkFile, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l))
    : []

  const parentUsage = { inputTokens: 111, outputTokens: 11, cacheReadTokens: 0, cacheWriteTokens: 0, reasoningTokens: 0 }
  const childUsage = { inputTokens: 222, outputTokens: 22, cacheReadTokens: 0, cacheWriteTokens: 0, reasoningTokens: 0 }
  const header = (time) => ({ type: 'request/header', seq: 0, time, data: { header: { config: { model: 'm', provider: 'p' } } } })
  const message = (time, turn, usage) => ({ type: 'assistant/message', seq: 1, time, data: { turn, step: 0, usage, message: { source: { provider: 'p', model: 'm' } } } })
  const sessions = [{
    id: 'sess-fork',
    // The first two events are the parent's inherited prefix (turn 5); the
    // child's own events follow (turn 6). The cut is at index 2.
    inheritedEventCount: 2,
    events: [header(100), message(100, 5, parentUsage), header(200), message(200, 6, childUsage)],
  }]
  const sq = fakeSessionQuery(sessions)
  boot({ maxRecords: 10 }, sq)
  const records = await waitFor(() => {
    const r = readFork().filter((x) => x.sessionId === 'sess-fork')
    return r.length === 1 ? r : null
  })
  const fork = records[0]
  assert(fork.inputTokens === childUsage.inputTokens && fork.outputTokens === childUsage.outputTokens,
    'only the child OWN call is imported (inherited parent call dropped)')
  assert(!readFork().some((r) => r.sessionId === 'sess-fork' && r.inputTokens === parentUsage.inputTokens),
    'the inherited parent call is not re-billed under the child')
}

// ── rebuild: wipe + re-import from session logs ──────────────────────────────
console.log('rebuild from logs wipes and re-imports cleanly')
{
  const HOME5 = path.join(os.tmpdir(), 'dsh-bill-backfill-test-rebuild')
  fs.rmSync(HOME5, { recursive: true, force: true })
  process.env.DSH_HOME = HOME5
  const rbFile = path.join(HOME5, 'dsh-bill', 'records.jsonl')
  const readRb = () => fs.existsSync(rbFile)
    ? fs.readFileSync(rbFile, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l))
    : []
  const sessions = [
    { id: 'sess-r1', events: USAGE_EVENTS },
    { id: 'sess-r2', events: USAGE_EVENTS },
  ]
  const sq = fakeSessionQuery(sessions)
  const ctx = boot({ maxRecords: 10 }, sq)
  // Initial import: two sessions, two records.
  await waitFor(() => readRb().length === 2 ? readRb() : null)
  // Pollute the file to mimic the inherited-prefix double-count the rebuild
  // exists to clean: append a bogus duplicate straight to the file, bypassing
  // the plugin so its in-memory ring never learns of it.
  fs.appendFileSync(rbFile, JSON.stringify({
    sessionId: 'sess-r1', time: 1000, model: 'm-src', provider: 'p-src',
    inputTokens: 10, outputTokens: 20, cacheReadTokens: 2, cacheWriteTokens: 1,
    reasoningTokens: 3, purpose: 'agent', source: 'log', usd: null, seq: 999,
  }) + '\n')
  assert(readRb().length === 3, 'pollution: a bogus duplicate was appended')
  // Rebuild: wipes the ring + file, re-imports only what the logs hold.
  // The modal's timeout + concurrency flow through as body params.
  const res = await callApi(ctx, { action: 'rebuild', timeoutMs: 60000, concurrency: 2 })
  assert(res && res.ok, 'rebuild action with timeout/concurrency params returned ok')
  await waitFor(() => readRb().length === 2 ? readRb() : null)
  const after = readRb()
  assert(after.length === 2, 'rebuild re-imported exactly the two sessions (bogus record gone)')
  assert(!after.some((r) => r.seq === 999), 'the bogus duplicate did not survive the rebuild')
  const dir = path.join(HOME5, 'dsh-bill')
  const baks = fs.readdirSync(dir).filter((f) => f.includes('.bak-'))
  assert(baks.some((f) => f.startsWith('records.jsonl.bak-')), 'records.jsonl was backed up before wiping')
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`)
  process.exit(1)
}
console.log('\nall backfill tests passed')
