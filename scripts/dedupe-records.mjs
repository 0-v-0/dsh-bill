#!/usr/bin/env node
/**
 * Dedupe dsh-bill records.jsonl — remove the fork/subagent "inherited prefix"
 * over-counts that backfill created before the inheritedEventCount fix.
 *
 * One real model call, inherited by N forked/delegated sessions, was written
 * once per session (all marked source:log). This script collapses every
 * duplicate call-identity to a single record, restoring accurate counts/usd
 * for the live ring.
 *
 * A call's identity = (time, purpose, model, inputTokens, outputTokens,
 *   cacheReadTokens, cacheWriteTokens, reasoningTokens). Two records sharing
 * all eight are the same physical call (a coincidence across ms-time + seven
 * token fields + model + purpose is astronomically unlikely).
 *
 * USAGE
 *   node scripts/dedupe-records.mjs                 # dry-run: report only
 *   node scripts/dedupe-records.mjs --apply         # backup + dedupe the live ring
 *   node scripts/dedupe-records.mjs --apply --clean-slate  # also wipe rollup.json
 *
 * --apply (default mode) is SAFE and preserves history:
 *   - The dead prefix (rollup.fileSkip leading lines, already folded into the
 *     rollup) is left VERBATIM, so the fileSkip invariant and the rollup stay
 *     valid. A live record duplicating a dead-prefix record is dropped (that
 *     call is already counted via the rollup).
 *   - The rollup's compacted-away totals (records evicted long ago, no longer
 *     in the file) CANNOT be un-inflated from aggregated data — that residual
 *     stays. Only a full rebuild from session logs (if retained) would clean it.
 *
 * --clean-slate trades history for a clean restart:
 *   - Dedupes the WHOLE file (dead prefix + live) and DELETES rollup.json.
 *   - On the next dsh start, loadPersisted reads the deduped file with
 *     fileSkip=0 and rebuilds a fresh rollup from it. The compacted-away
 *     records (gone from the file) are lost — undercount, but un-inflated.
 *
 * Run with dsh web STOPPED (the plugin appends to records.jsonl live; a
 * concurrent write would race the rewrite).
 */
import fs from 'node:fs'
import path from 'node:path'
import os from 'node:os'

const home = process.env.DSH_HOME || path.join(os.homedir(), '.dsh')
const dir = path.join(home, 'dsh-bill')
const recordsPath = path.join(dir, 'records.jsonl')
const rollupPath = path.join(dir, 'rollup.json')
const legacyPath = path.join(home, 'dsh-cost-money', 'records.jsonl')

const args = new Set(process.argv.slice(2))
const APPLY = args.has('--apply')
const CLEAN_SLATE = args.has('--clean-slate')
if (CLEAN_SLATE && !APPLY) {
  console.error('--clean-slate requires --apply')
  process.exit(2)
}

const fileExists = (p) => { try { fs.accessSync(p); return true } catch { return false } }

function loadRollup() {
  if (!fileExists(rollupPath)) return { fileSkip: 0 }
  try { return JSON.parse(fs.readFileSync(rollupPath, 'utf8')) } catch (e) {
    console.error('rollup.json unreadable (' + e.message + '); assuming fileSkip=0')
    return { fileSkip: 0 }
  }
}

function readRecords(p) {
  if (!fileExists(p)) return []
  const out = []
  let bad = 0
  for (const line of fs.readFileSync(p, 'utf8').split('\n')) {
    if (!line.trim()) continue
    try { out.push(JSON.parse(line)) } catch { bad++ }
  }
  if (bad) console.error(`warning: skipped ${bad} unparseable line(s)`)
  return out
}

// Eight-field call identity. Fields are coerced to string so `null` and `0`
// collide the same way the plugin serializes them.
function identity(r) {
  return [r.time, r.purpose ?? '', r.model ?? '', r.inputTokens ?? 0,
    r.outputTokens ?? 0, r.cacheReadTokens ?? 0, r.cacheWriteTokens ?? 0,
    r.reasoningTokens ?? 0].join('|')
}

function usd(r) { const v = r.usd; return typeof v === 'number' ? v : 0 }

function fmt(n) { return Number.isFinite(n) ? n.toFixed(4) : String(n) }

// ── load ────────────────────────────────────────────────────────────────────
const recPath = fileExists(recordsPath) ? recordsPath : (fileExists(legacyPath) ? legacyPath : null)
if (!recPath) { console.error('no records.jsonl found under ' + dir); process.exit(1) }
const rollup = loadRollup()
const fileSkip = CLEAN_SLATE ? 0 : (Number.isInteger(rollup.fileSkip) ? rollup.fileSkip : 0)
const records = readRecords(recPath)

const deadPrefix = records.slice(0, fileSkip)
const live = records.slice(fileSkip)

// ── dedupe ───────────────────────────────────────────────────────────────────
// Identities already counted via the dead prefix (folded into the rollup) — a
// live record matching one of these is a duplicate the rollup already bills.
const seen = new Set(deadPrefix.map(identity))
const kept = []
const dropped = []
for (const r of live) {
  const k = identity(r)
  if (seen.has(k)) { dropped.push(r); continue }
  seen.add(k)
  kept.push(r)
}

const overcountUsd = dropped.reduce((s, r) => s + usd(r), 0)
const liveUsd = live.reduce((s, r) => s + usd(r), 0)
const keptUsd = kept.reduce((s, r) => s + usd(r), 0)
const totalUsd = records.reduce((s, r) => s + usd(r), 0)

// ── report ───────────────────────────────────────────────────────────────────
console.log('records.jsonl : ' + recPath)
console.log('total records : ' + records.length + '  (live ring ' + live.length + ' + dead prefix ' + deadPrefix.length + ')')
console.log('rollup        : calls=' + (rollup.calls ?? '?') + '  usd=' + fmt(rollup.usd) + '  fileSkip=' + fileSkip)
console.log('live dups     : ' + dropped.length + '  over-counted usd=' + fmt(overcountUsd))
console.log('after dedupe  : live ' + kept.length + '  (was ' + live.length + ')  usd ' + fmt(keptUsd) + ' (was ' + fmt(liveUsd) + ')')
console.log('file usd      : ' + fmt(totalUsd) + '  →  ' + fmt(keptUsd + deadPrefix.reduce((s, r) => s + usd(r), 0)) + ' (live + dead prefix)')
if (rollup.calls) {
  console.log('')
  console.log('NOTE: the rollup (' + rollup.calls + ' folded calls / $' + fmt(rollup.usd) +
    ') holds compacted-away records not in this file. Its inherited-prefix')
  console.log('inflation CANNOT be un-aggregated here. --clean-slate wipes it (loses that')
  console.log('history); otherwise it stays inflated. A full rebuild from session logs')
  console.log('(if retained) is the only way to clean it precisely.')
}

if (!APPLY) {
  console.log('')
  console.log('dry-run — nothing written. Re-run with --apply to dedupe.')
  process.exit(0)
}

// ── apply ────────────────────────────────────────────────────────────────────
const ts = new Date().toISOString().replace(/[:.]/g, '-')
const bak = (p) => p + '.bak-' + ts
fs.copyFileSync(recPath, bak(recPath))
console.log('backup        : ' + bak(recPath))

const lines = []
if (CLEAN_SLATE) {
  // Whole-file dedupe: dead prefix is also de-duplicated against itself.
  const wholeSeen = new Set()
  const wholeKept = []
  for (const r of records) {
    const k = identity(r)
    if (wholeSeen.has(k)) continue
    wholeSeen.add(k)
    wholeKept.push(r)
  }
  for (const r of wholeKept) lines.push(JSON.stringify(r))
  console.log('clean-slate   : deduped whole file → ' + wholeKept.length + ' records (was ' + records.length + ')')
} else {
  // Dead prefix verbatim + deduped live ring. fileSkip stays valid.
  for (const r of deadPrefix) lines.push(JSON.stringify(r))
  for (const r of kept) lines.push(JSON.stringify(r))
  console.log('applied       : dead prefix ' + deadPrefix.length + ' (verbatim) + live ' + kept.length)
}

const tmp = recPath + '.tmp-' + ts
fs.writeFileSync(tmp, lines.join('\n') + (lines.length ? '\n' : ''))
fs.renameSync(tmp, recPath)
console.log('wrote         : ' + recPath)

if (CLEAN_SLATE) {
  fs.copyFileSync(rollupPath, bak(rollupPath))
  fs.rmSync(rollupPath, { force: true })
  console.log('rollup        : deleted (backup ' + bak(rollupPath) + ')')
  console.log('next start    : loadPersisted reads the deduped file with fileSkip=0; a fresh')
  console.log('                rollup rebuilds from it (no inherited-prefix dupes).')
} else {
  console.log('rollup        : untouched (fileSkip=' + fileSkip + ' still valid; dead prefix verbatim)')
}
console.log('done.')
