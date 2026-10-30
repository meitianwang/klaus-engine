#!/usr/bin/env node

import { readdirSync, rmSync, statSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

// 构建产物预算：Cargo 对旧指纹产物从不回收，而本工作区单个 crate 的 debug
// 产物本就巨大（core 一份 rlib 逾 500 MiB），于是每次换 feature 组合都叠一层，
// target/debug 曾无声积到 248 GiB、40 万文件，占满四分之一磁盘。
//
// 三种用法各自独立：
//   --report                 分层列出体积构成，只读
//   --prune [--keep-days N]  回收根目录 scratch 与陈旧产物（含陈旧增量缓存）
//   --check [--budget 40G]   超预算即非零退出，可挂 CI 或本地钩子
//
// 根目录 scratch 是人往 target 根倒的临时输出，不参与产物身份，无条件回收。
// 其余按 mtime 判定：缺失的产物 Cargo 会在下次构建时重建，故剪除只影响耗时不
// 影响正确性。增量缓存同样按 mtime 走，不再无条件清空——实测本仓改一行重编开
// 增量 5 秒、关增量 37 秒，清掉正在用的那份等于把每次改动都拖成七倍。因此增量
// 缓存按 crate 逐个成单元，陈旧的那些照常回收，今天在用的留下；要腾空间到底就
// 用 --keep-days 0。

// Cargo 自用文件：CACHEDIR.TAG 让备份工具跳过整个目录，.rustc_info.json 缓存
// 编译器探测结果。二者都在 target 根目录且不是 scratch，必须原样保留。
export const PROTECTED_ROOT_FILES = Object.freeze(['CACHEDIR.TAG', '.rustc_info.json'])

export const DEFAULT_KEEP_DAYS = 7
export const DEFAULT_BUDGET = '40G'

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const DAY_MS = 24 * 60 * 60 * 1000

const SIZE_UNITS = Object.freeze({ B: 1, K: 1024, M: 1024 ** 2, G: 1024 ** 3, T: 1024 ** 4 })

/** Parse a human size such as `40G`, `500MiB` or `1024` into bytes. */
export function parseSize(text) {
  const match = /^(\d+(?:\.\d+)?)\s*([BKMGT])?(?:i?B)?$/u.exec(String(text).trim())
  if (!match) throw new Error(`unparseable size: ${text}`)
  return Math.round(Number(match[1]) * SIZE_UNITS[match[2] ?? 'B'])
}

export function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`
  for (const unit of ['K', 'M', 'G', 'T']) {
    const scaled = bytes / SIZE_UNITS[unit]
    if (scaled < 1024 || unit === 'T') return `${scaled.toFixed(1)} ${unit}iB`
  }
  return `${bytes} B`
}

/**
 * Assign one target-relative path to a budget category. Categories drive both
 * the report grouping and what `--prune` is allowed to touch.
 */
export function classify(relativePath) {
  const segments = relativePath.split('/')
  if (segments.length === 1) {
    return PROTECTED_ROOT_FILES.includes(segments[0]) ? 'protected' : 'scratch'
  }
  const [, second] = segments
  if (second === 'incremental') return 'incremental'
  if (second === 'deps') return 'deps'
  if (second === 'build') return 'build'
  if (second === 'examples') return 'examples'
  return 'output'
}

/**
 * Decide what to remove. Root scratch goes unconditionally; everything else,
 * including per-crate incremental caches, goes once its age reaches `keepDays`,
 * so `--keep-days 0` prunes every artifact and a larger window keeps more.
 */
export function planPrune({ units, keepDays = DEFAULT_KEEP_DAYS, now = Date.now() }) {
  const maxAgeMs = keepDays * DAY_MS
  const removals = []
  for (const unit of units) {
    if (unit.category === 'protected' || unit.category === 'output') continue
    if (unit.category === 'scratch') {
      removals.push({ ...unit, reason: 'regenerable' })
      continue
    }
    if (now - unit.mtimeMs >= maxAgeMs) removals.push({ ...unit, reason: `older than ${keepDays}d` })
  }
  return {
    removals,
    bytes: removals.reduce((sum, unit) => sum + unit.bytes, 0),
  }
}

export function summarize(units) {
  const groups = new Map()
  for (const unit of units) {
    const group = groups.get(unit.category) ?? { bytes: 0, files: 0 }
    group.bytes += unit.bytes
    group.files += unit.files
    groups.set(unit.category, group)
  }
  return {
    totalBytes: units.reduce((sum, unit) => sum + unit.bytes, 0),
    totalFiles: units.reduce((sum, unit) => sum + unit.files, 0),
    groups: [...groups.entries()]
      .map(([category, group]) => ({ category, ...group }))
      .sort((left, right) => right.bytes - left.bytes),
  }
}

export function checkBudget({ totalBytes, budgetBytes }) {
  if (totalBytes <= budgetBytes) return []
  return [
    `target is ${formatBytes(totalBytes)}, over the ${formatBytes(budgetBytes)} budget by ${formatBytes(totalBytes - budgetBytes)}`,
  ]
}

/**
 * Walk `target` into prunable units. A unit is one whole directory that prune
 * may drop (incremental caches) or one leaf file, so that `--prune` never has
 * to reason about partially-removed directories.
 */
export function scanTarget(targetDir, { readdir = readdirSync, stat = statSync } = {}) {
  const units = []

  const measure = (absolute) => {
    let bytes = 0
    let files = 0
    let mtimeMs = 0
    const stack = [absolute]
    while (stack.length > 0) {
      const current = stack.pop()
      for (const entry of readdir(current, { withFileTypes: true })) {
        const child = join(current, entry.name)
        if (entry.isDirectory()) {
          stack.push(child)
          continue
        }
        const stats = stat(child)
        bytes += stats.size
        files += 1
        mtimeMs = Math.max(mtimeMs, stats.mtimeMs)
      }
    }
    return { bytes, files, mtimeMs }
  }

  const visit = (relative) => {
    const absolute = join(targetDir, relative)
    const category = classify(relative)
    // Aggregate whole-directory units; descend only through profile roots and
    // the per-profile buckets so deps/build/incremental stay per-entry for age
    // pruning. Incremental descends one level so each crate's cache ages on its
    // own: collapsing the bucket made the whole cache carry today's mtime, which
    // left "drop the stale part, keep the live part" impossible to express.
    if (category === 'examples' || category === 'output') {
      units.push({ path: relative, category, ...measure(absolute) })
      return
    }
    for (const entry of readdir(absolute, { withFileTypes: true })) {
      const child = `${relative}/${entry.name}`
      if (entry.isDirectory()) {
        units.push({ path: child, category, ...measure(join(targetDir, child)) })
        continue
      }
      const stats = stat(join(targetDir, child))
      units.push({ path: child, category, bytes: stats.size, files: 1, mtimeMs: stats.mtimeMs })
    }
  }

  for (const entry of readdir(targetDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      const stats = stat(join(targetDir, entry.name))
      units.push({
        path: entry.name,
        category: classify(entry.name),
        bytes: stats.size,
        files: 1,
        mtimeMs: stats.mtimeMs,
      })
      continue
    }
    // A profile root (debug/release/<triple>): split it into its buckets.
    for (const bucket of readdir(join(targetDir, entry.name), { withFileTypes: true })) {
      const relative = `${entry.name}/${bucket.name}`
      if (bucket.isDirectory()) {
        visit(relative)
        continue
      }
      const stats = stat(join(targetDir, relative))
      units.push({
        path: relative,
        category: 'output',
        bytes: stats.size,
        files: 1,
        mtimeMs: stats.mtimeMs,
      })
    }
  }

  return units
}

export function parseArguments(argv) {
  const options = { mode: null, keepDays: DEFAULT_KEEP_DAYS, budget: DEFAULT_BUDGET }
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index]
    if (argument === '--report' || argument === '--prune' || argument === '--check') {
      if (options.mode) throw new Error('pick exactly one of --report, --prune, --check')
      options.mode = argument.slice(2)
    } else if (argument === '--keep-days') {
      options.keepDays = Number(argv[(index += 1)])
      if (!Number.isFinite(options.keepDays) || options.keepDays < 0) {
        throw new Error('--keep-days needs a non-negative number')
      }
    } else if (argument === '--budget') {
      options.budget = argv[(index += 1)]
    } else {
      throw new Error(`unknown argument: ${argument}`)
    }
  }
  if (!options.mode) {
    throw new Error(
      'usage: target-budget.mjs --report|--prune|--check [--keep-days N] [--budget 40G]',
    )
  }
  return options
}

function main() {
  const options = parseArguments(process.argv.slice(2))
  const targetDir = join(REPO_ROOT, 'target')

  let units
  try {
    units = scanTarget(targetDir)
  } catch (error) {
    if (error.code === 'ENOENT') {
      console.log('target-budget: OK (no target directory)')
      return
    }
    throw error
  }

  const summary = summarize(units)

  if (options.mode === 'report') {
    console.log(`target-budget: ${formatBytes(summary.totalBytes)} in ${summary.totalFiles} files`)
    for (const group of summary.groups) {
      console.log(
        `  ${group.category.padEnd(12)} ${formatBytes(group.bytes).padStart(10)}  ${group.files} files`,
      )
    }
    return
  }

  if (options.mode === 'check') {
    const violations = checkBudget({
      totalBytes: summary.totalBytes,
      budgetBytes: parseSize(options.budget),
    })
    if (violations.length > 0) {
      console.error('target-budget: FAILED')
      for (const violation of violations) console.error(`  - ${violation}`)
      console.error('  run: node scripts/target-budget.mjs --prune')
      process.exit(1)
    }
    console.log(
      `target-budget: OK (${formatBytes(summary.totalBytes)} within ${options.budget} budget)`,
    )
    return
  }

  const plan = planPrune({ units, keepDays: options.keepDays })
  for (const removal of plan.removals) {
    rmSync(join(targetDir, removal.path), { recursive: true, force: true })
  }
  console.log(
    `target-budget: pruned ${formatBytes(plan.bytes)} across ${plan.removals.length} entries ` +
      `(kept artifacts newer than ${options.keepDays}d); ` +
      `${formatBytes(summary.totalBytes - plan.bytes)} remains`,
  )
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) main()
