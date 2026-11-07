## Consolidating durable guidance

What to write:

- Extract the takeaways from the existing topic notes and their supporting evidence, especially sections like
  "Preference signals", "Reusable knowledge", "References", and "Failures and how to do differently".
- Wording-preservation rule: when the source already contains a concise, searchable phrase,
  keep that phrase instead of paraphrasing it into smoother but less faithful prose.
  Prefer exact or near-exact wording from:
  - user messages,
  - task `description:` lines,
  - `Preference signals:`,
  - exact error strings / API names / parameter names / file names / commands.
- Do not rewrite concrete wording into more abstract synonyms when the original wording fits.
  Bad: `the user prefers evidence-backed debugging`
  Better: `when debugging, the user asked / corrected: "check the local cloudflare rule and find out. Don't stop until you find out" -> trace the actual routing/config path before answering`
- If several sources say nearly the same thing, merge by keeping one of the original phrasings
  plus any minimal glue needed for clarity, rather than inventing a new umbrella sentence.
- Retrieval bias: preserve distinctive nouns and verbatim strings that a future grep/search
  would likely use (exact error text, API or parameter names, file names, and commands).
- Keep original wording by default. Only paraphrase when needed to merge duplicates, repair
  grammar, or make a point reusable.
- Overindex on user messages, explicit user adoption, and code/tool evidence. Underindex on
  assistant-authored recommendations, especially in exploratory design/naming discussions.
- First extract candidate user preferences and recurring steering patterns from task-level
  preference signals before clustering the procedural reusable knowledge and failure shields. Do not let the procedural
  recap consume the entire compression budget.
- For `## User preferences` in the topic notes, preserve more of the user's original point than a
  terse summary would. Prefer evidence-aware bullets that still carry some of the user's
  wording over abstract umbrella statements.
- For `## Reusable knowledge` and `## Failures and how to do differently`, preserve the source's
  original terminology and wording when it carries operational meaning. Compress by deleting
  less important clauses, not by replacing concrete language with generalized prose.
- `## Reusable knowledge` should contain facts, validated procedures, and failure shields, not
  assistant opinions or rankings.
- Do not over-merge adjacent preferences. If separate user requests would change different
  future defaults, keep them as separate bullets even when they came from the same task group.
- Optimize for future related tasks: decision triggers, validated commands/paths,
  verification steps, and failure shields (symptom -> cause -> fix).
- Capture stable user preferences/details that generalize so the MEMORY.md topic index can provide useful retrieval hooks.
- Preserve cwd applicability in the block header and task details when it affects reuse.
- When deciding what to promote, prefer information that helps the next agent better match
  the user's preferred way of working and avoid predictable corrections.
- It is acceptable for the topic notes to preserve user preferences that are very general, general,
  or slightly specific, as long as they plausibly help on similar future runs. What matters is
  whether they save user keystrokes and reduce repeated steering.
- The topic notes do not need to be aggressively short. It is the durable operational middle layer:
  richer and more concrete than the MEMORY.md topic index, but more consolidated than a conversation recap.
- When the evidence supports several actionable preferences, prefer a longer list of sharper
  bullets over one or two broad summary bullets.
- Do not require a preference to be global across all tasks. Repeated evidence across similar
  tasks in the same block is enough to justify promotion into that block's `## User preferences`.
- Ask how general a candidate memory is before promoting it:
  - if it only reconstructs this exact task, keep it local to the task subsections or conversation recap
  - if it would help on similar future runs, it is a strong fit for `## User preferences`
  - if it recurs across tasks/conversations, it may also deserve a more prominent retrieval hook in the MEMORY.md topic index
- The topic notes should support related-but-not-identical tasks while staying operational and
  concrete. Generalize only enough to help on similar future runs; do not generalize so far
  that the user's actual request disappears.

- Each block should be useful on its own and materially richer than the MEMORY.md topic index:
  - include the user preferences that best predict how the next agent should behave,
  - include concrete triggers, reusable procedures, decision points, and failure shields,
  - include outcome-specific notes (what worked, what failed, what remains uncertain),
  - include cwd scope and mismatch warnings when they affect reuse,
  - include scope boundaries / anti-drift notes when they affect future task success,
  - include stale/conflict notes when newer evidence changes prior guidance.
- Keep task sections lean and routing-oriented; put the synthesized know-how after the task list.
- In each block, preserve the same kinds of good stuff that the extraction process already identified:
  - put validated facts, procedures, and decision triggers in `## Reusable knowledge`
  - put symptom -> cause -> pivot guidance in `## Failures and how to do differently`
  - keep those bullets comprehensive and wording-preserving rather than flattening them into generic summaries
- In `## User preferences`, prefer bullets that look like:
  - when <situation>, the user asked / corrected: "<short quote or near-verbatim request>" -> <future default>
  rather than vague summaries like:
  - the user prefers better validation
  - the user prefers practical outcomes
- Preserve epistemic status when consolidating:
  - validated repo/tool facts may be stated directly,
  - explicit user preferences can be promoted when they seem stable,
  - inferred preferences from repeated follow-ups can be promoted cautiously,
  - assistant proposals, exploratory discussion, and one-off judgments should stay local,
    be downgraded, or be omitted unless later evidence shows they held.
  - when preserving an inferred preference or agreement, prefer wording that makes the
    source of the inference visible rather than flattening it into an unattributed fact.
