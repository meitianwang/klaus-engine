/** Preserve the full suite while bounding concurrent test processes on busy hosts. */
export function parseGateOptions(args) {
  let keepGoing = false
  let testJobs = 4
  let jobsSupplied = false
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index]
    if (argument === '--keep-going') {
      keepGoing = true
      continue
    }
    if (argument === '--test-jobs' || argument.startsWith('--test-jobs=')) {
      if (jobsSupplied) throw new Error('--test-jobs must be supplied only once')
      const value = argument === '--test-jobs'
        ? args[++index]
        : argument.slice('--test-jobs='.length)
      if (typeof value !== 'string' || !/^[1-4]$/.test(value)) {
        throw new Error('--test-jobs requires an integer from 1 through 4')
      }
      jobsSupplied = true
      testJobs = Number(value)
      continue
    }
    throw new Error(`unknown gate argument: ${argument}`)
  }
  return Object.freeze({ keepGoing, testJobs })
}

export function nextestGateArguments(testJobs) {
  if (!Number.isInteger(testJobs) || testJobs < 1 || testJobs > 4) {
    throw new Error('test jobs must be an integer from 1 through 4')
  }
  return ['nextest', 'run', '--locked', '--workspace', '--all-features', `-j${testJobs}`, '--no-fail-fast']
}
