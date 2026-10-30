# Read-only side-question input

`session/sideQuestion` accepts the existing raw `Input` wire shape in `question`: either a string (unchanged) or an array of text, image, and PDF document parts. It does not accept attachment paths and never reads files to resolve them.

```json
{
  "id": 20,
  "method": "session/sideQuestion",
  "params": {
    "sessionId": "parent-session",
    "submissionId": "side-question-unique-id",
    "question": [
      { "type": "text", "text": "Compare these attachments with the earlier discussion." },
      { "type": "image", "mimeType": "image/png", "base64": "<standard base64 of the image bytes>" },
      { "type": "document", "mimeType": "application/pdf", "base64": "<standard base64 of the PDF bytes>", "filename": "report.pdf" },
      { "type": "text", "text": "<Office/TXT/HTML content extracted by the host's real parser>" }
    ]
  }
}
```

The acknowledgment still carries `submissionId`; the eventual `session/event` contains `event.submissionId` and `event.sideQuestion` or `event.error`. To cancel precisely this request, call `session/cancelRequest` with `requestId` equal to the **submission ID**, not the outer JSON-RPC request ID. Unknown request IDs do not cancel another request. Main turns and other read-only queries remain independent.

## Validation and isolation

- At most five binary parts in total, counting images and PDFs together. Each is at most 10 MiB decoded, so binary content is at most 50 MiB total. Text parts do not consume attachment slots.
- Binary limits do not waive provider context limits. The standard PDF token estimate can make a 50 MiB batch exceed a model's context capacity; normal context/compaction handling still applies. The transport boundary test explicitly declares a sufficiently large simulated context window.
- Reject empty/all-whitespace questions, malformed standard base64, invalid image MIME/signatures, invalid PDF MIME/signature, unsafe document filenames, and PDFs unsupported by the effective model before calling the provider or touching main history.
- Image MIME types are `image/png`, `image/jpeg`, `image/gif`, and `image/webp`. The signature must match the declared MIME type. This is signature validation, not full image decoding.
- Standard `validate_input_for_model` runs for each part and `to_content_blocks` constructs provider content. Per-part validation deliberately avoids applying the ordinary turn's separate 20 MiB aggregate PDF limit to this 50 MiB read-only budget. Ordinary turn size/count limits are unchanged.
- A side question with image attachments fails before inference if the selected model cannot see them. Unlike the ordinary turn's omission behavior, drafting must not pretend it read an attachment. PDF capability failures likewise remain errors.
- The protocol permits a 72 MiB serialized frame for `side_question`, enough for base64 expansion plus text/JSON overhead. Other operations retain the 32 MiB frame limit. Existing request-queue backpressure remains in effect.
- The provider sees actual image/document blocks, not paths or attachment-name substitutes. The inherited main history is only reference context, followed by the side-question boundary and the input.
- Both the tool registry and advertised tool specs stay empty. No approval handler or hook bus is installed in the query; no tool refresh, message queue, or native web search is enabled. `skip_cache_write` remains `true`.
- The side input, response, and validation failures never append to main history or its persisted transcript. Normal provider usage accounting remains in place. Cancellation signals the provider and drops its stream.

## Rust interfaces and schema

Core keeps `ask_side_question(String)` and `ask_side_question_with_signal(String, AbortSignal)`. New entry points are `ask_side_question_input(Input)` and `ask_side_question_input_with_signal(Input, AbortSignal)`.

SDK `Thread` and the internal protocol client keep their original string methods, and add `side_question_input(Input)` / `side_question_input_with_signal(Input, AbortSignal)`. Existing string call sites remain source-compatible. CLI strings are converted to the raw JSON string value.

The named facade uses `SessionSideQuestionParams.question: serde_json::Value` with a published `rawInput` string-or-parts schema. Internally `OpKind::SideQuestion.question` is `Input`; its exact runtime codec uses the same `wire_input` adapter as `Run` and `Steer`, including UTF-16 content preservation and type errors. Host-generated schema adapters should map `question` to their existing `rawInput` contract rather than a string. No host schema file is maintained in this engine change.

Title generation still uses the same read-only query with `Input::Text`, the low-tier model, title output schema, token budget, and existing persistence behavior.
