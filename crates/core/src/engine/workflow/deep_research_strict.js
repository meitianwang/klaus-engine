export const meta = {
  name: 'deep-research-strict',
  description: 'High-assurance deep research with code-enforced citation integrity: structured sources, per-claim adversarial verification, and a deterministic citation audit so every [N] in the report points to a real, support-confirmed source.',
  whenToUse: 'When a research answer will be shown to others or treated as fact for a decision and a fabricated or misattributed citation is unacceptable. Heavier and slower than the deep-research skill — use it when the guarantee matters.',
  phases: [
    { title: 'Decompose' },
    { title: 'Research' },
    { title: 'Verify' },
    { title: 'Synthesize' },
  ],
}

// args: a question string, or { question: "..." }.
const question = typeof args === 'string' ? args : (args && typeof args.question === 'string' ? args.question : '')
if (!question) {
  return { error: 'deep-research-strict requires a question (pass args = "..." or { question: "..." }).' }
}

const SUBQ_SCHEMA = {
  type: 'object',
  properties: { subquestions: { type: 'array', items: { type: 'string' } } },
  required: ['subquestions'],
}
const CLAIMS_SCHEMA = {
  type: 'object',
  properties: {
    claims: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          text: { type: 'string' },
          sourceUrl: { type: 'string' },
          sourceTitle: { type: 'string' },
          quote: { type: 'string' },
        },
        required: ['text', 'sourceUrl'],
      },
    },
  },
  required: ['claims'],
}
const VERDICT_SCHEMA = {
  type: 'object',
  properties: { supported: { type: 'boolean' }, reason: { type: 'string' } },
  required: ['supported'],
}

const normUrl = (u) => {
  try { return String(u).trim().replace(/#.*$/, '').replace(/\/+$/, '') } catch (e) { return String(u) }
}

// ---- 1. Decompose -------------------------------------------------------------
phase('Decompose')
const plan = await agent(
  'Break this research question into 2-6 INDEPENDENT sub-questions that can be researched separately. ' +
  'Independent = answering one does not depend on another. If the question is already simple, return a single-element array. ' +
  'Return ONLY JSON matching the schema.\n\nQuestion: ' + question,
  { schema: SUBQ_SCHEMA, label: 'decompose', phase: 'Decompose' },
)
const subs = (plan && Array.isArray(plan.subquestions) && plan.subquestions.length > 0)
  ? plan.subquestions.filter((s) => typeof s === 'string' && s.trim()).slice(0, 6)
  : [question]

// ---- 2. Research: structured claims, each tied to a source (parallel) ---------
phase('Research')
const research = await parallel(
  subs.map((sq) => () => agent(
    'Research this question with web_search and web_fetch. Extract every load-bearing fact as a separate CLAIM. ' +
    'For each claim provide: text (the claim), sourceUrl (the exact page URL it came from), sourceTitle, and quote (a short verbatim excerpt from that page that supports it). ' +
    'NEVER include a claim you cannot tie to a specific page you actually opened. Return ONLY JSON matching the schema.\n\nQuestion: ' + sq,
    { schema: CLAIMS_SCHEMA, agentType: 'researcher', label: 'research', phase: 'Research' },
  )),
)

// Collect claims defensively (failed researchers → null).
const rawClaims = []
for (const r of research) {
  if (r && Array.isArray(r.claims)) {
    for (const c of r.claims) {
      if (c && typeof c.text === 'string' && c.text.trim() && typeof c.sourceUrl === 'string' && c.sourceUrl.trim()) {
        rawClaims.push({
          text: c.text.trim(),
          sourceUrl: c.sourceUrl.trim(),
          sourceTitle: typeof c.sourceTitle === 'string' ? c.sourceTitle.trim() : '',
          quote: typeof c.quote === 'string' ? c.quote.trim() : '',
        })
      }
    }
  }
}
if (rawClaims.length === 0) {
  return {
    question,
    report: 'No verifiable claims could be established for this question.',
    sources: [],
    audit: { subQuestions: subs.length, claimsFound: 0, verified: 0, dropped: 0, sources: 0, strippedCitations: 0 },
  }
}

// ---- 3. Verify each claim against its source (gate ②, parallel) ---------------
phase('Verify')
const verdicts = await parallel(
  rawClaims.map((c) => () => agent(
    'Decide whether the SOURCE actually supports the CLAIM. Re-read the source with web_fetch if needed. ' +
    'Answer supported=true ONLY if the source clearly states or directly entails the claim. ' +
    'If you are unsure, the page does not actually say it, or you cannot access the source, answer supported=false. ' +
    'Return ONLY JSON {"supported": boolean, "reason": "..."}.\n\n' +
    'CLAIM: ' + c.text + '\nSOURCE URL: ' + c.sourceUrl + (c.quote ? '\nRESEARCHER QUOTE: ' + c.quote : ''),
    { schema: VERDICT_SCHEMA, agentType: 'researcher', label: 'verify', phase: 'Verify' },
  )),
)
const verified = []
for (let i = 0; i < rawClaims.length; i++) {
  const v = verdicts[i]
  if (v && v.supported === true) verified.push(rawClaims[i])
}
const dropped = rawClaims.length - verified.length
if (verified.length === 0) {
  return {
    question,
    report: 'No claims survived independent source verification — nothing can be stated as fact for this question.',
    sources: [],
    audit: { subQuestions: subs.length, claimsFound: rawClaims.length, verified: 0, dropped, sources: 0, strippedCitations: 0 },
  }
}

// ---- canonical source list over VERIFIED claims (deterministic [1..]) ---------
const urlToId = new Map()
const sources = []
for (const c of verified) {
  const key = normUrl(c.sourceUrl)
  if (!urlToId.has(key)) {
    const id = sources.length + 1
    urlToId.set(key, id)
    sources.push({ id, url: c.sourceUrl, title: c.sourceTitle })
  }
}
const validIds = new Set(sources.map((s) => s.id))
const taggedClaims = verified.map((c) => ({ id: urlToId.get(normUrl(c.sourceUrl)), text: c.text }))

// ---- 4. Synthesize, then deterministically audit citations (gate ①) -----------
phase('Synthesize')
const sourcesBlock = sources.map((s) => '[' + s.id + '] ' + (s.title || s.url) + ' — ' + s.url).join('\n')
const claimsBlock = taggedClaims.map((c) => '(' + c.id + ') ' + c.text).join('\n')
const draft = await agent(
  'Write a clear, conclusion-first report answering the QUESTION using ONLY the VERIFIED CLAIMS below. ' +
  'Lead with the direct answer in the first 1-3 sentences, then supporting detail organized by theme. ' +
  'Each claim is tagged with a source id in parentheses; when you state that claim, cite it inline as [id] using that SAME id. ' +
  'Do NOT introduce any fact that is not in the verified claims, and do NOT use any citation id not present in SOURCES. ' +
  'Do NOT write a Sources section — it is appended automatically.\n\n' +
  'QUESTION: ' + question + '\n\nVERIFIED CLAIMS — (id) text:\n' + claimsBlock + '\n\nSOURCES:\n' + sourcesBlock,
  { label: 'synthesize', phase: 'Synthesize' },
)
let body = typeof draft === 'string' ? draft : ''

// Gate ①: strip any [N] the model emitted that code did not mint. A fabricated or
// out-of-range citation cannot survive — only ids in validIds remain.
let strippedCitations = 0
body = body.replace(/\[(\d+)\]/g, (m, n) => {
  if (validIds.has(Number(n))) return m
  strippedCitations++
  return ''
})
// Cosmetic cleanup after removals.
body = body.replace(/[ \t]+([.,;:!?])/g, '$1').replace(/[ \t]{2,}/g, ' ').trim()

const report = body + '\n\nSources:\n' + sourcesBlock
return {
  question,
  report,
  sources,
  audit: {
    subQuestions: subs.length,
    claimsFound: rawClaims.length,
    verified: verified.length,
    dropped,
    sources: sources.length,
    strippedCitations,
  },
}
