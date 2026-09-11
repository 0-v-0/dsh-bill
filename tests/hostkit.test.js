/**
 * A write into a `$DSH_HOME/dsh-bill` that does not exist yet must land.
 *
 * This is the first write of a fresh install, and it used to fail silently:
 * `withFileLock` delegated to the borrowed `@deepseek-ai/dsh-atomic-write`
 * before creating the parent, and that module's `withFileLock` — unlike its
 * `writeFileAtomic` — requires the parent to exist. The lock is taken first, so
 * the first lock of a fresh home raised ENOENT on `<file>.lock` and no byte was
 * ever written: no records, no budget, no preferences. The failure surfaced as
 * an error in the client and nothing on disk, which is why it read as "the
 * setting does not stick" rather than "the write never happened".
 *
 * Run: node tests/hostkit.test.js
 */
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const { withFileLock, hostkitReady } = await import('../lib/hostkit.js')
await hostkitReady

let failed = 0
function assert(cond, msg) {
  if (cond) console.log('  ok -', msg)
  else { failed++; console.error('  FAIL -', msg) }
}

// Which implementation is in charge decides whether this test reaches the bug
// at all: the delegating path is the one that lacked the mkdir. Say so out
// loud, and let `npm test` in a harness checkout cover it.
const borrowed = await import('@deepseek-ai/dsh-atomic-write').then(() => true, () => false)
console.log(borrowed
  ? '  note: @deepseek-ai/dsh-atomic-write is resolvable — the delegating path is covered'
  : '  note: @deepseek-ai/dsh-atomic-write is not resolvable here — only the fallback is covered')

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-bill-hostkit-'))
const file = path.join(root, 'fresh', 'nested', 'prefs.json')
assert(!fs.existsSync(path.dirname(file)), 'the parent directory starts out missing')

let ran = false
await withFileLock(file, async () => {
  ran = true
  fs.writeFileSync(file, '{"currency":"EUR"}')
})

assert(ran, 'the locked operation ran')
assert(fs.existsSync(path.dirname(file)), 'the parent directory was created')
assert(fs.existsSync(file), 'the file was written')
assert(
  fs.existsSync(file) && JSON.parse(fs.readFileSync(file, 'utf8')).currency === 'EUR',
  'the bytes survived the round trip',
)

// Read-modify-write under the same lock, the shape the rollup actually uses.
await withFileLock(file, async () => {
  const current = JSON.parse(fs.readFileSync(file, 'utf8'))
  fs.writeFileSync(file, JSON.stringify({ ...current, budgetAmount: 25 }))
})
const after = JSON.parse(fs.readFileSync(file, 'utf8'))
assert(after.currency === 'EUR' && after.budgetAmount === 25, 'a second locked write merges instead of replacing')

fs.rmSync(root, { recursive: true, force: true })

if (failed) { console.error(`\n${failed} check(s) failed`); process.exit(1) }
console.log('\nall checks passed')
