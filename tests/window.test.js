/**
 * Two kinds of report window, and why they are not one.
 *
 * `rangeDays` counts back from the moment of the request: ROLLING, with no end.
 * `day` names a whole calendar day on the host's clock, which is the only one
 * of the two with an END — yesterday must not include the calls made this
 * morning, and that is exactly what a count of days back cannot express. This
 * suite pins both bounds of each kind, the local day keys the chart is bucketed
 * by, and the fact that the two answers differ, which is the distinction the
 * report's grouped picker exists to expose.
 *
 * Run: node tests/window.test.js
 */
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const HOME = path.join(os.tmpdir(), 'dsh-bill-window-test')
fs.rmSync(HOME, { recursive: true, force: true })
process.env.DSH_HOME = HOME

const { default: plugin } = await import('../lib/index.js')

let failed = 0
function assert(cond, msg) {
  if (cond) console.log('  ok -', msg)
  else { failed++; console.error('  FAIL -', msg) }
}
const near = (a, b, eps = 1e-6) => Math.abs(a - b) < eps

/** Same cordis stub as the other suites: one webServer, one llm/stream hook. */
const instance = {}
const services = { webServer: { register: (r) => { instance.api = r.handler } } }
const ctx = {
  ...services,
  effect: (fn) => fn(),
  get: (name) => services[name],
  on: (name, fn) => { if (name === 'llm/stream') instance.stream = fn },
  inject: (names, apply) => { if (names.every((n) => services[n])) apply(ctx) },
}
plugin.apply(ctx, { maxRecords: 200 })
await new Promise((r) => setTimeout(r, 250))

/** Record one call stamped `when`, priced by its token count. */
async function call(when, inputTokens) {
  const real = Date.now
  Date.now = () => when
  async function* source() {
    yield { type: 'usage', usage: { inputTokens, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 } }
  }
  const options = { provider: 'deepseek-official', model: 'deepseek-v4-pro', sessionId: 's1', messages: [] }
  for await (const _ of instance.stream(options, () => source())) { /* drain */ }
  Date.now = real
}
async function ask(body) {
  const req = { on: (e, fn) => { if (e === 'data') fn(JSON.stringify(body)); if (e === 'end') fn() } }
  let out = null
  await instance.api(req, { writeHead() {}, end: (text) => { out = JSON.parse(text) } })
  return out
}
/** The host's own calendar arithmetic, restated so the test does not ask it. */
const pad2 = (n) => String(n).padStart(2, '0')
const localDay = (t) => {
  const d = new Date(t)
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
}

// Seeds, relative to the host clock so no assertion depends on when the suite
// runs: one call late yesterday, one early today, one stamped tomorrow, and one
// far outside every window here.
const now = new Date()
const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime()
const yesterdayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1).getTime()
const tomorrowStart = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1).getTime()
const hour = 3600_000

const old = Date.now() - 40 * 24 * hour
const lateYesterday = yesterdayStart + 23.5 * hour
const earlyToday = todayStart + 0.5 * hour

await call(old, 50_000)
await call(lateYesterday, 100_000)
await call(earlyToday, 200_000)
await call(tomorrowStart, 300_000)

// Every seed is priced by its own token count, so a window's total says WHICH
// call it counted, not merely how many. Nothing here is derived from a rate:
// the windows are disjoint, so their totals are the per-call costs, and the
// rolling window can be checked against their sum.
console.log('a calendar day has a start and an end')
const today = await ask({ action: 'dashboard', day: 'today' })
assert(today.calls === 1, 'today holds only the call made today (got ' + today.calls + ')')
assert(today.totalUsd > 0, 'and its money (got ' + today.totalUsd + ')')
assert(today.day === localDay(todayStart), 'the answer names the day it resolved to (got ' + today.day + ')')
assert(today.timelineDays.length === 1 && today.timelineDays[0].day === localDay(todayStart),
  'a calendar day is one bar, bucketed on the host calendar')
assert(today.rangeDays === 0, 'a named day reports no day count to confuse it with a rolling window')

console.log('the end of the window is what a rolling count cannot express')
const yesterday = await ask({ action: 'dashboard', day: 'yesterday' })
assert(yesterday.calls === 1, 'yesterday holds its own call (got ' + yesterday.calls + ')')
assert(yesterday.totalUsd > 0 && yesterday.totalUsd !== today.totalUsd,
  'and it is the other call, not this morning\'s')
assert(yesterday.day === localDay(yesterdayStart), 'and names yesterday\'s date (got ' + yesterday.day + ')')

console.log('an explicit date names the same kind of window')
const tomorrow = await ask({ action: 'dashboard', day: localDay(tomorrowStart) })
assert(tomorrow.calls === 1 && tomorrow.totalUsd !== today.totalUsd, 'a date selects exactly its own day')
assert(tomorrow.timelineDays.length === 1 && tomorrow.timelineDays[0].day === localDay(tomorrowStart), 'one bar, its own day')

console.log('the same question, asked both ways, does not get one answer')
const rolling = await ask({ action: 'dashboard', rangeDays: 1 })
// Late yesterday is inside the last 24 hours only while the clock is before
// 23:30; the expectation is derived from the clock so this asserts the bound
// rather than the hour the suite ran.
const lateInRolling = Date.now() - lateYesterday < 24 * hour
assert(rolling.calls === 2 + (lateInRolling ? 1 : 0),
  'the rolling day counts this morning and tomorrow\'s stamp, because it never ends (got ' + rolling.calls + ')')
assert(near(rolling.totalUsd,
  today.totalUsd + tomorrow.totalUsd + (lateInRolling ? yesterday.totalUsd : 0), 1e-3),
  'rolling money is exactly those calls, and not yesterday\'s morning')
assert(rolling.calls > yesterday.calls, 'so rolling and yesterday are different answers to different questions')
// The window's days come back dense, so the quiet ones are dropped here: the
// days that CARRY calls are what show which records the bound let in.
const rollingDays = rolling.timelineDays.filter((d) => d.calls > 0).map((d) => d.day).sort().join(',')
const expectedDays = [localDay(todayStart), localDay(tomorrowStart)]
  .concat(lateInRolling ? [localDay(yesterdayStart)] : []).sort().join(',')
assert(rollingDays === expectedDays, 'the rolling window is bucketed by local day too (' + rollingDays + ')')

console.log('an unreadable day falls back instead of answering about another')
const junk = await ask({ action: 'dashboard', day: 'nonsense' })
assert(junk.day === null && junk.rangeDays === 30, 'a junk day gets the default 30-day window (got ' + junk.rangeDays + ')')
assert(junk.calls === 3, 'and that window is the real one, over every record inside 30 days (got ' + junk.calls + ')')
const rolled = await ask({ action: 'dashboard', day: '2026-02-31' })
assert(rolled.day === null && rolled.rangeDays === 30,
  'a date that does not exist is refused, not rolled into March (got ' + rolled.day + ')')

console.log('a day or less comes back by the hour, dense, on the host clock')
const hourOf = (t) => `${localDay(t)}T${pad2(new Date(t).getHours())}`
// Yesterday is a whole day: every one of its hours, the quiet ones as zeros.
// (A DST changeover day has 23 or 25 local hours; JST has none.)
const yHours = yesterday.timelineHours.map((h) => h.hour)
assert(yHours.length >= 23 && yHours.length <= 25, 'yesterday has an hour slot for each hour (got ' + yHours.length + ')')
assert(yHours[0] === localDay(yesterdayStart) + 'T00', 'and they start at its local midnight (got ' + yHours[0] + ')')
const busy = yesterday.timelineHours.filter((h) => h.calls > 0)
assert(busy.length === 1 && busy[0].hour === hourOf(lateYesterday),
  'its one call sits in its own local hour (got ' + busy.map((h) => h.hour).join(',') + ')')
// Today runs up to the hour now falls in, never past it.
const tHours = today.timelineHours.map((h) => h.hour)
assert(tHours[0] === localDay(todayStart) + 'T00' && tHours[tHours.length - 1] === hourOf(Date.now()),
  'today runs from its midnight to the current hour (' + tHours[0] + ' … ' + tHours[tHours.length - 1] + ')')
// The rolling 24 hours starts at the top of the hour 24 hours ago.
assert(rolling.timelineHours.some((h) => h.hour === hourOf(Date.now() - 23 * hour)),
  'the rolling day carries the hours back to 24 hours ago')
// A long window keeps its hours sparse: thirty days of zeros is not a chart.
assert(junk.timelineHours.every((h) => h.calls > 0), 'a 30-day window does not pad its hours')

console.log('the heatmap reads the same clock as the bars')
const cellAt = (r, t) => r.heatmap.find((c) => c.weekday === new Date(t).getDay() && c.hour === new Date(t).getHours())
const yCell = cellAt(yesterday, lateYesterday)
assert(yCell && yCell.calls === 1, 'yesterday\'s call lands on its local weekday and hour')
assert(yesterday.heatmap.reduce((n, c) => n + c.calls, 0) === 1, 'and nowhere else')

console.log(failed === 0 ? '\nALL PASSED' : `\n${failed} FAILED`)
process.exit(failed === 0 ? 0 : 1)
