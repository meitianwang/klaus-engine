# 宿主接入口子（Host Integration Seams）

本文件回答一个问题：**哪些事情是引擎做的，哪些必须由接入方宿主做。**

引擎是支撑层。它提供工具执行骨架、上下文管理、模型协议适配和权限判定框架，但
**不持有任何一方的业务策略、合规规则或外部服务凭据**。凡是"由谁说了算"的判断，
一律通过下列口子交给宿主。

## 判定准则

一个能力属于宿主还是引擎，用四条准则判断：

1. **策略归宿主，机制归引擎。** 引擎负责"怎么执行"，宿主负责"允不允许"。域名黑
   名单、审批规则、合规裁决都是策略。
2. **引擎不得内置外部服务客户端。** 引擎不应该知道"有个远端可以问"。需要外部信
   息的判断，把入参交给宿主回调，由宿主决定去哪里问、等多久、失败算过还是算不过。
3. **不接入必须是安全且可解释的。** 缺一个口子时，要么能力干净消失，要么按安全
   默认拒绝，不允许静默降级成"看起来能用但结果不对"的状态。
4. **外部能力按"模型侧 → 插件/MCP"取。** 需要外部服务才能完成的能力（检索、抓取
   增强等），优先用模型侧原生能力（server-side tool / grounding），此路零额外凭据、
   零额外端点；模型侧不具备该能力时，经插件或 MCP 由宿主提供。引擎只保留工具骨架
   与注入口，**不得内置任何具体供应商的端点、私有签名算法、请求/响应格式适配或凭据
   槽位**——那些是业务逻辑，不是支撑层的东西。

第七节记录已经按这四条准则清理掉的偏离，以及每项的替代做法。

## 如何审查新增的外部依赖

按环境变量名或私有域名搜代码是**不可靠**的：这类接入点的端点和凭据往往在仓库里留空，
等部署时注入，字符串搜索根本扫不到。可靠的入口是**引擎自己发 HTTP 的位置**：

```bash
rg 'reqwest::Client::(new|builder)' crates/*/src
```

逐个判它属于哪类。只有三类是合法的：

1. **provider 模型对接**——引擎核心职责，端点可注入且只有公网默认值。
2. **MCP 协议传输**——连接配置由宿主/用户提供。
3. **按用户或模型给定的 URL 抓取/安装**——端点来自输入，不是引擎钉的服务。

落在这三类之外的，就是偏离。三个辅助特征可以交叉验证：

- `rg 'BUILTIN_' crates/*/src` —— 编译期端点/凭据槽位应为零（本地数据常量不算）。
- `rg 'pub fn default_.*(catalog|backend|proxy|options)'` —— 自动拨号工厂；读网络的应为零。
- 凭据回退链：拿模型凭据（如 `ANTHROPIC_AUTH_TOKEN`）去打非模型端点，一律是偏离。

---

## 零、客户端从哪一层接入

这一节在所有具体口子之前，因为它决定后面每个口子长在哪。

**引擎对外只有一个出口：`crates/server/src/app_server/` 里的 app-server JSON-RPC
facade。** 所有客户端都走它，包括本仓自带的 CLI。凡是"驱动一个不是自己造出来的
会话"的代码，都算客户端。

裸 Op/Event 协议（`OpKind`、`ProtocolEvent`、`ProtocolClient`）是 facade 的**内部
接线**，不是接入面。这条禁令精确落在**提交侧**：客户端不许自己拼一个 op 塞进去。
读不算——facade 的回复和事件本身就是协议 crate 的类型，客户端解 `ProtocolEvent`
和各个 result model 是常态，本仓 CLI 依赖 `klaus-agent-protocol` 就是为了解码。

所以真正的红线是：**点名 `OpKind`，或者持有 `ProtocolClient`**。只有两类代码可以
越线：

1. facade 自己，它负责把具名方法翻译成 op。
2. 承载内部协议的传输层（`serve`、`connect`，以及 stdio / WebSocket 那两个 shim）。

服务模式（`klaus proto`、`klaus serve`、`klaus app-server`）**不是**第三类豁免：它们
把字节流整个交出去就退场，流上的 op 属于调用方、由调用方自己的客户端做类型检查，
所以 `crates/cli/src` 里这两个名字一次都不出现。

### Complete original-history follow

`session/follow` accepts the optional `includeOriginalHistory: true` flag. It
keeps the original persisted conversation across provider prompt compaction,
applies actual conversation rollbacks, and overlays real turn-owned live rows.
The rollout flush, saved read, and terminal history reply hold one protocol
output lease; the facade assigns the reply the revision at that ordered cut.
No client may read a second history snapshot after the cut and reuse its cursor.
The default follow retains the existing model-context history contract.

An original-history snapshot also carries:

- `activeItems: [{turnId, item}]`, captured by the single event pump at the same
  cut. These are the complete partial text, reasoning, tool input, and progress
  aggregates, including records not yet represented by durable messages.
- `coveredMessageIds`, including original row UUIDs and the authoritative text,
  reasoning, and tool item identities carried in complete message blocks.
  Persistence can run ahead of notification delivery, so clients reconcile
  later deltas by these identities rather than guessing text overlap.
- `coveredToolResults: [{turnId, toolUseId}]`. Tool IDs may be reused in another
  turn. A persisted start does not cover a terminal result; tool-result coverage
  is therefore separate and always scoped to its real turn.

Active `tool_progress` items retain one chronological stdout/stderr/status tail
per tool, limited to 20,000 Unicode characters (at most 80 KB of UTF-8). Their
`truncated` and `truncatedBytes` fields make retention explicit. File changes
remain separate entries for each changed path. Completed original tool results
and original messages are not truncated by this live progress buffer.

`thinking.delta.messageId` is optional for older event producers; current
producers provide the reasoning identity shared with the completed item.

The rollout writer exposes a monotonic accepted-mutation revision. Complete
capture samples it before flush/read and after live-history/turn-index capture,
then retries if any append, compaction or rollback moved the rollout. Eight
unsuccessful attempts return an explicit busy error; backends without this
revision reject complete follow explicitly. The ordered output lease alone
cannot fence the independent model producer.

Reasoning deltas and persisted thinking blocks carry the same optional
`messageId`, assigned when reasoning starts and retained through abortion.
Finalized content filtering cannot change that identity. Product item and delta
notifications also expose optional canonical `turnId` from the event pump's
active submission mapping.

Live turn metadata derives `running` from the session's actual active leases,
while preserving explicit terminal statuses even before their notifications.
Both original follow and provider-free `engine/readThreadHistory` overlay this
already-loaded owner metadata, so a cold reader cannot mistake an unfinished
rollout for a recovered interrupted turn. Reads still create no session or
provider work; sleeping and archived threads retain saved recovery semantics.

### Task identity for deferred continuation

`session/taskIdentity({sessionId, taskId})` is a read-only query for one exact
persisted task, including tasks archived by a later plan. Engines advertising
`initialize.capabilities.taskIdentity: true` return
`{taskIdentity: {taskId, generation, fingerprint, status, subject, description}}`,
or `{taskIdentity: null}` when that task no longer exists or is host-internal.
The query does not start inference or invoke a model tool. Invalid task IDs and
unreadable task storage return explicit errors; clients must not reinterpret an
error as task cancellation.

`generation` is an opaque token for the task instance and task-list storage
incarnation. Deleting and recreating a numeric ID, recreating the task directory,
or rebinding the session to another task list invalidates it. `fingerprint`
covers only the exact task subject and description. Status, owner, active-form
text, accounting, and changes to unrelated tasks do not change that fingerprint.
Target semantics must be included in the task description. Clients save both
tokens with an explicit task ID, then compare that task before admitting a
deferred continuation; the current plan or current in-progress task is not an
identity substitute.

### 为什么卡这一条

两个客户端站在两个面上，每个能力就要加两遍，然后在第二遍之前先漂移。四个 MCP
请求 op 把这个失效模式演全了：协议里有类型、server 里有 handler、功能是完整的，但
facade 从没给它们起名字，于是任何客户端都摸不到；一个会 panic 掉整条连接的
dispatcher 误路由就一直躺在那里，直到 CLI 成为第一个真去提交它们的调用方才被踩出来。

所以**新增能力的顺序是先在 facade 上加具名方法**，让方法表始终等于对外契约。永远
不要让客户端自己拼 op 载荷——那等于把契约挪到了客户端里，引擎再也说不清自己对外
承诺了什么。

这个顺序不是风格偏好。`crates/server/src/app_server/methods.rs` 里的一行才是把方法名
与 params / result 类型绑在一起的东西；它不存在时，客户端无从调用，schema bundle
也无从发布。加完那一行，另外三件事自动跟上：

- `ALL_METHODS` 多一条，发布出去的 schema bundle 就描述了它。
- `app_server::tests::every_declared_method_is_dispatched` 会一直红，直到 `service.rs`
  里给它加上 dispatch 臂——声明了但没有臂的方法两侧都能编过，然后运行时回
  `METHOD_NOT_FOUND`。
- `crates/cli/tests/facade_coverage.rs` 会一直红，直到 CLI 侧的表写明 `klaus` 怎么
  抵达它，或者写明为什么故意不暴露。

只有客户端那一处调用是需要手写的。

### 通用透传已退役，且不许复活

`session/op` / `engine/op` 这两个"任意 op 直投"的方法，已经通过把每个 op 逐条
promote 成具名方法而下线。**不许以任何形式复活**：不许藏在 debug flag 后面，不许
"临时加一下"，不许"只给内部调用方用"。只要存在一条透传，facade 的方法表就不再是
契约，前面那套判定准则也就没有着力点了。

### 怎么验证

```bash
# 声明表是 CLI 唯一的门，而且 CLI 走遍了整张表
cargo nextest run -p klaus-agent-cli facade_coverage
# 每个声明的方法都被发布、且都有 dispatch 臂
cargo nextest run -p klaus-agent-server app_server::
```

---

### Live inference publication

`session/beginModelUpdate({sessionId, changeId, expectedRevision?})` establishes
an engine-owned fence for subsequent inference captures and returns
`{revision, changeId}`. It bypasses the ordinary turn queue, so a running stream,
tool, approval, or MCP question cannot postpone fence acknowledgement. Existing
captures keep their immutable request configuration.

`session/reconfigureModel({sessionId, init, expectedRevision?, changeId?})`
prepares and atomically publishes a complete inference selection, returning
`{revision, changeId?}`. The `changeId` identifies the matching fence; preparation
never recreates or supersedes an earlier fence. Core owns compare-and-swap,
idempotent replay, and supersession checks. Preparation, including source
resolution, is bounded to 75 seconds and cancelled on session close. Any failure
with a known change ID fails only that matching fence; it cannot clear a newer
selection or admit a new request on an unconfirmed cached provider.

`session/failModelUpdate({sessionId, changeId, error})` reports a host-side
preparation failure and returns `{revision, changeId}`. It fails only a matching
pending fence, leaves a newer selection untouched, and never releases a failed
fence into cached-provider admission. A stale failure is an acknowledged no-op.

`session/modelRuntime({sessionId})` includes optional `changeId`,
`pendingChangeId`, and `modelUpdateError`. An unknown publish acknowledgement
must be reconciled with that change identity, not guessed from the revision or
model name. A successful publish makes the bundle available for the next capture;
it does not relabel a request already in flight.

Inference init accepts the non-secret `modelSource: {source, modelId, selection?}`
and the existing `modelTierTargets` map. Each high/medium/low target remains null
(follow the main model), source-qualified, resolved with its private provider
initializer, or explicitly unavailable. Those private target initializers are
not public runtime metadata. Item and text/reasoning delta product notifications
retain the engine event's optional `inference: {revision, changeId?, model,
source?, tier?, selection?}`. Raw events and usage payloads pass through their
captured attribution; no facade consumer labels an aggregate turn with its last
request's model. Missing attribution stays missing for old history.

`session/start` and complete `session/reconfigureModel` init also accept
`advisorTarget: null | ModelTierTarget`. This independent target uses the same
source, resolved, and unavailable shapes as one tier slot, but is never selected
from `modelTierTargets`. Omission and null both turn advisor consultation off;
a full reconfiguration is not a patch of the previous advisor selection. A
model-tier-only reconfiguration leaves the advisor unchanged.

Source resolution errors and resolved advisor provider-construction failures
become an unavailable advisor target without rejecting the main model. Invalid
wire shapes remain request errors. Startup installs the prepared advisor before
session publication; complete reconfiguration publishes it atomically with the
main inference selection and includes it in named-publication replay identity.
`session/modelRuntime` reports `advisorTarget` as null or a non-secret ready or
unavailable projection. It never returns the provider initializer, credentials,
or pricing table. These are fields on existing methods, not new endpoints.

The schema bundle derives these method/result and notification contracts from
the declared types. The CLI coverage table deliberately withholds live model
publication because the CLI selects its provider at session startup with the
advisor disabled.

### Session metadata catalog

`engine/listSessions({options})` remains the catalog query. The first page
reconciles canonical rollout names and file identities against a durable,
derived first-line metadata index. Unchanged files are not reopened. New opaque
v2 continuation cursors retain a coherent, immutable first-page snapshot: they
perform no directory walks, file stats, or transcript reads. At most 16 snapshots
are retained for five minutes per store. An expired or foreign snapshot returns
`session catalog cursor expired; restart without cursor`; query-filter mismatch
and malformed cursors are explicit errors, never silent restarts. Older
`(updatedAt, id)` keyset cursors are accepted with a fresh reconciliation and
upgraded to snapshot cursors. A fresh first-page request sees external create,
update, delete, archive, and unarchive operations; an in-progress snapshot keeps
its original rows even if canonical files change between pages.

Every page includes `membershipFingerprint`, the SHA-256 of its sorted eligible
IDs after archive, role, and cwd filters. The value is identical across one
snapshot and unaffected by title/content updates or ordering. A remote catalog
may stop an incremental head scan at its retained watermark only when the
membership fingerprint is unchanged. Changed membership requires a complete
reconciliation before deleting cached IDs, including equal-count delete/create.

`metadataFingerprint` separately hashes the complete canonical `SessionMeta` of
eligible IDs, sorted by ID with recursively sorted UTF-16 object keys through
the exact runtime codec. It excludes filesystem identity, mtime, size and rollout
body, so ordinary transcript appends keep it stable. External header/host-metadata
edits that preserve `updatedAt` change this fingerprint and require metadata
reconciliation even when membership is unchanged. Both fingerprints are frozen
for all pages of one snapshot.

`engine/readSessionSummary({sessionId})` returns `{session: row | null}`, where
`row` is the exact `engine/listSessions` summary shape. It includes archived
sessions and validates a known target's file identity without hydrating history,
recovering lifecycle journals, starting a provider, or taking a writer lock.
A missing store capability or persistence error is an explicit error, not null.
Display and metadata-only lookups never write derived sidecars; explicit catalog
listings persist validated snapshots. Catalog cache files are expendable derived
data; corruption or cache-write failure never replaces canonical rollouts.
Namespace scan failures publish no partial catalog snapshot.

### Durable permission rule strings

`engine/permissionRules`, `engine/addPermissionRules` and
`engine/removePermissionRule` own the durable `{allow, deny, ask}` string lists.
The grammar is `Tool` or `Tool(literal prefix)`: the first `(` and final `)` are
outer delimiters. Interior parentheses and backslashes belong to the prefix;
there is no backslash decoding. This preserves existing Windows and UNC paths.
For example, the structured prefix `echo (ok)` serializes to `Bash(echo (ok))`.
Clients should delete using the original stored string when projecting aliases
or tool-wide `(*)` rows. Do not apply a separate UI escaping grammar.

Structured writes validate the entire batch before any persistence. An invalid
tool identifier, a non-string argument prefix, or a wildcard rule whose argument
would be ignored rejects with invalid params; a successful grant must survive
the same parser used during permission evaluation. Existing malformed rows stay
visible and removable by their original raw key rather than disappearing from
the store during an unrelated write.

The three administration methods return an empty store only when the file is
absent. Corrupt JSON, invalid list shapes and I/O failures return an engine error;
edits preserve the existing bytes. Writes publish through an atomic replacement.
The session-start loading fallback remains a separate policy.

`session/reloadPermissionRules({sessionId})` strictly reads the same engine-owned
store and returns `{allow, deny, ask}` after replacing that live session's durable
layer. This is a direct terminal RPC result, not a submission ACK. It also works
on a registered lazy session without materializing it. Unknown or closing owners
are rejected. The host sends no rule arrays and must not flatten the durable
snapshot into `init.config`; those config lists are independent project/host rules.

Reload replaces all three stored classes atomically for the next permission
evaluation. An evaluation already in progress retains its own complete snapshot.
Project rules, session-only grants, mode, provider, pending approvals, history and
hidden side-task prefixes are unchanged. A durable approval becomes effective
immediately in a pending layer; after its event is saved under the same store
mutex, the facade moves only that grant into the stored layer. This prevents a
concurrent reload from losing an unsaved approval and makes a saved approval
removable from the existing session. All query and background executors share
the same layer.

Absent files reload as empty rules. Malformed JSON, invalid lists, read errors or
semantically invalid rule strings fail without changing current policy or disk
bytes. Legacy invalid strings remain visible/removable through the three store
administration methods so a user can repair them before retrying reload.

### Persistent forks from live sessions

`session/forkLive({sourceSessionId, init, beforeMessageUuid?})` returns the same
`{sessionId, initialized}` shape as `session/start`. The source must already be
initialized and live; this method never resumes it from storage. Capture rejects
a running or closing source under the same admission lock used by real turns.
`init.threadId` may name the new persistent destination. Existing live, active
stored, and archived IDs are rejected. A failed init, provider resolution, or
target creation leaves the source unchanged.

The destination is an ordinary main thread with `forkedFromId` pointing to the
source and no `parentThreadId`. Its visible history is the captured model history,
including an ephemeral side task's inherited hidden prefix. Only the destination
exposes that prefix; the source keeps its role, visibility, and history. A supplied
cut keeps messages strictly before that UUID and rejects an unknown UUID.

The response waits for the inherited transcript to be flushed. Closing and
resuming the destination preserves the same history. The source may continue
after capture without changing the destination. Execution resources, usage,
queues, background agents/processes, memory maintenance, and permission callbacks
are not copied; the host supplies the destination's normal init/provider policy.
Hosts must not serialize source history back into init. The existing store-based
`session/forkSession` contract is unchanged.

### Replace a live session's inference configuration

`session/modelRuntime({sessionId})` reads an existing initialized session without
activating it. Its non-secret projection contains `revision`, `model`,
`modelTiers`, `effort`, `thinking`, `contextWindow`, `maxOutputTokens`, and the
selected model's `capabilities`. It never returns provider credentials.

`session/reconfigureModel({sessionId, init, expectedRevision?})` prepares a new
provider and atomically replaces inference on that same Session. Success returns
`{revision}`. `init` is a complete inference selection, not a generic session-init
patch. It allows exactly `provider`, `auth`, `modelSelection`,
`modelTierSelections`, `capabilities`, `capabilitiesByModel`, `pricing`, and
`config`. Supply exactly one of `provider` and `modelSelection`. The `config`
object allows only `model`, `modelTiers`, `effort`, `thinking`, `contextWindow`,
and `maxOutputTokens`. Omitted inference fields resolve from the new selection
and engine defaults; old tiers, context pins, effort, and pricing do not leak into
the replacement. Hosts update permissions and MCP/extensions through their
existing named methods, never by passing the full session init here.

Credential/model resolution has a 60-second bound and is cancelled when the
session starts closing. During preparation the old session remains usable. The
commit compares `expectedRevision`; when omitted, the engine captures the live
revision before preparation. An intervening reconfiguration or legacy runtime
model change makes that attempt fail with a revision conflict. Hosts re-read the
runtime and decide whether to retry; the engine does not silently overwrite a
newer choice.

Commit shares model admission with turns, readonly queries, reviews, subagents,
and memory maintenance. Running inference, pending prompts, unfinished background
processes, or speculative compaction reject the change. A healthy idle MCP
connection does not block it. Provider, model/tiers, effort/thinking, capabilities,
context/output limits, future pricing, and provider-dependent compaction calls
change together. Failed preparation, a busy session, or a revision conflict
leaves the old bundle and resources intact.

Custom injected compactors must implement the explicit `rebind_model` seam or
replacement fails rather than discarding their policy. A persistent background
teammate retains its model lease until the owned task ends, including mailbox
waits; the host must end it before replacing that session's inference.

History and its hidden prefix, ephemeral/persistent identity, role/parentage,
turn indices, queues, tasks, file changes, permissions, MCP/LSP connections,
historical usage/cost, and compaction policy/breaker state remain on the existing
Session. This method never closes/resumes the session and does not persist a
replacement transcript. Hosts still persist their model-source preference and
enforce product authorization before both replacement and each subsequent turn.

### Assistant message identity and phase

The existing product notifications carry optional assistant text metadata:

- `item/started`, `item/updated`, and `item/completed` expose `item.id` and `item.phase` for `agent_message` items.
- `item/agentMessage/delta` exposes the same identity as `messageId`, alongside optional `phase` and the existing `delta`.
- `phase` is `commentary` or `final_answer` when the provider explicitly supplies that intent. An absent phase stays unknown while streaming. Clients must not infer a final answer from text content or from the absence of a tool call so far.
- When a provider supplies no phase, the completed Turn's authoritative final identity produces an `item/updated` notification with `phase: "final_answer"`. Interrupted and failed Turns do not receive this final promotion.

One provider response can contain several assistant message items. Each keeps its own stable display identity; persisted text content blocks carry `messageId` and `phase`, and Responses history replay preserves those phase boundaries. `turn/completed.result.finalMessageId` addresses the final displayed text item. The `session/history` `turnFinalMessageIds` index continues to address the enclosing persisted assistant record; clients use the content-block metadata to identify its individual displayed messages. A logical message may have several text blocks separated by tool blocks, so clients append deltas only to the current contiguous text segment and preserve the original order.

`engine/readThread` also preserves these identities and explicit phases in live and persisted `agentMessage` items. Live deltas update their matching message item instead of creating an additional temporary text item, and an absent provider phase remains absent while that message streams.

## 一、模型对接层

### 1.1 请求拦截 —— 企业网关鉴权接在这里

`ProviderRequestInterceptor`（`crates/core/src/provider/interceptor.rs:67`）是宿主
塑形和观察 provider 流量的唯一正道口子，两个阶段：

```rust
fn before_request(&self, req: &ProviderRequestInfo) -> ProviderRequestDirectives
fn after_response(&self, res: &ProviderResponseInfo)
```

能力边界是刻意收紧的：

- `ProviderRequestDirectives` 只有两个字段——`extra_headers`（追加请求头）和
  `deny`（否决本次请求，作为**不可重试**错误）。
- **不能改 URL，不能改 body。** 提示词和流式内容永不经过拦截器。
- 追加的 header 位于 provider 静态 header 之后、鉴权/协议 header 之前，**冲突时鉴
  权 header 优先**，所以拦截器无法破坏签名。
- 每次 establishment attempt 都重跑一遍（**含重试**），因此短时效的网关凭据能保鲜。
- 回调在请求路径上同步执行，必须快且非阻塞——从缓存状态推导值，不要在里面做 I/O。

适用场景就是企业网关请求头、轮转令牌、trace id 注入，以及成本/策略护栏（用 `deny`）。
不注入时全部放行且无观察。

### 1.2 凭据与鉴权

| 口子 | 位置 | 语义 |
| --- | --- | --- |
| `AuthSource` | `provider/auth.rs:176` | `get_headers`（可透明刷新）、`base_url` 覆盖（订阅后端与公开 API 不同源）、`force_refresh`（401/403 后强刷一次重试） |
| `CredentialStore` | `credential_store.rs:14` | `get`/`set`/`delete` 的异步密钥存储；MCP 与 provider OAuth 共用。引擎自带 AES-256-GCM 加密文件实现（`~/.klaus/credentials.enc` + `cred.key`，0600），全平台一致。不走 OS keychain CLI：macOS `security` 裸 `-w` 从控制终端读提示、不收管道 stdin，曾把 key 静默存空。值不得进日志与快照 |
| `ProviderCallAdmission` | `engine/query/mod.rs:166` | per-turn provider 边界准入。`before_call` 失败即 fail-closed（`call_model` 不执行）；`call_returned`/`call_panicked` 返回必须落地的 receipt。实现必须幂等，因为模型回退/重试会重复调用 |
| `ModelProvider` | `types/provider.rs:1337` | provider 本体。`info`/`capabilities`/`stream` 必填，`estimate_tokens`/`resolve_auth`/`prewarm_transport` 有默认实现 |
| `GeminiClient` | `provider/gemini.rs:496` | Gemini 传输层抽象，可注入替换 HTTP 实现 |

不注入 `AuthSource` 时退回 API key 模式（env 或构造参数）。不注入 `CredentialStore`
时 OAuth 无法持久化 token，每次都要重新授权。

**没有全局 provider registry。** provider 由宿主构造并在构造参数里注入上述依赖，
一旦建成不可替换。

### 1.3 能力目录

`KLAUS_MODEL_CATALOG_URL` 指向一份远端模型能力文档（`provider/model_catalog.rs:31`），
带 ETag 条件请求、4 小时 revalidate、落盘缓存。用 `with_model_catalog` 包装任意
provider 即生效，它只改写 `capabilities()`，其余原样转发。

**这是失败处理的正面样板**：不配则从不联网，拉取失败保留已缓存文档，未命中的模型
落回内置基线。内置能力始终是权威离线基准。

---

## 二、执行、沙箱与出网

### 2.1 两条网络链路互不相同

这是最容易搞错的地方。引擎里有**两条独立的出网管控链路**，共用
`evaluate_host_policy`（`tools/builtins/host_policy.rs`）这一个域名策略函数，但裁
决点和裁决方向完全不同。

**链路 A：沙箱命令出网** → `NetworkProxy`（`network_proxy.rs`，引擎自建的本地
HTTP/SOCKS 代理）。判定顺序在 `evaluate_request`（`network_proxy.rs:1886`）：

1. 受限目的端口 → 直接拒。
2. `evaluate_host_policy` 判 `Deny` 时，才去问 `NetworkPolicyDecider`。
3. 有 decider 且返回 `Allow` → **救回**，继续放行；返回非 Allow → 拒。
4. 没有 decider → 硬拒（`domain_denied`）。

由此得到一个反直觉但必须知道的事实：

> **`NetworkPolicyDecider` 只能放宽，不能加严。** host_policy 放行的请求根本不会
> 送到 decider 面前。它是"被静态策略拒掉之后的救回口"，不是"放行前的复核关"。

`NetworkPolicyDecision` 带 `source` 和 `reason` 字段，就是为了让每次裁决可解释。
配套还有 `BlockedRequestObserver`（观察被拦请求，做审计）和 `ConfigReloader`
（热重载代理配置），均在 `network_proxy.rs:921` / `:926`。

**链路 B：`web_fetch` 工具出网** → 不经过 NetworkProxy。顺序是：URL 合法性 →
私有地址/SSRF 拒绝 → `evaluate_host_policy` → `request_network_access` 交宿主批准
→ 抓取（或走 2.3 的代理）。

**所以宿主想统一管出网，两条链路都要接**：链路 A 接 `NetworkPolicyDecider`，链路
B 接 `request_network_access`。只接一个会留下另一条的敞口。

### 2.2 执行与隔离

| 口子 | 位置 | 语义 |
| --- | --- | --- |
| `SandboxBackend` | `types/sandbox.rs:225` | `kind` / `is_available`（运行时探测 `sandbox-exec`、`bwrap` 是否存在）/ `transform`（把 `ExecRequest` + `SandboxPolicy` 变成 `SpawnSpec`） |
| `ExecBackend` / `ExecProcess` | `command_runtime/exec_server/backend.rs:151` | 进程启动与单进程生命周期控制。宿主可整体替换为远程执行、容器执行 |
| `HardenEnvironment` | core | 环境加固 |
| `FileIO` | core | 文件 IO 抽象 |

`SandboxKind` 内置五种：`Process`（最低档，只清环境变量和管进程组，不做文件系统/
网络隔离）、`Seatbelt`（macOS）、`Bubblewrap`（Linux）、`NativeHelper`（Linux
Landlock + seccomp，不需要 user namespace）、`WindowsRestrictedToken`。引擎按平台
和可用性自动选择，宿主注入用于替换成自有隔离机制。

### 2.3 宿主注入的渲染型抓取

宿主若要接管抓取（渲染、走自家代理、自带网络栈），用
`web_fetch_page: Arc<dyn PageFetcher>`（`tools/builtins/web_fetch.rs`）：引擎只把已过
SSRF/策略校验的 URL 交出去，渲染器自带网络栈，引擎仅负责把返回文本截到
`max_chars`。超时、重试、代理鉴权全由宿主实现决定。

---

## 三、工具运行时（ToolContext）

`ToolContext`（`crates/core/src/types/tool.rs`）是每次工具调用的宿主接触面。

### 3.1 必填字段

`process_context`（进程命名空间）、`cwd`、`signal`、`file_state`
（`Arc<dyn FileStateCache>`，记录读写过的文件以检测陈旧覆盖和外部编辑）、
`permission`、`sandbox`、`emit`（结构化进度事件出口）。

两个安全默认值必须知道：

- `network_allowed: Option<bool>` —— `None` 表示**拒绝**沙箱命令联网，不是放行。
- `sandbox_retry_on_denial: Option<bool>` —— `None`/`Some(false)` 表示沙箱拒绝后
  **永不**降级为无沙箱重跑，原样上报拒绝。

### 3.2 裁决类回调

| 口子 | 缺失时 |
| --- | --- |
| `request_network_access` | 该上下文不限制网络出口（见 2.1 链路 B） |
| `request_approval` | 视为"无法升级"（沙箱拒绝后不能申请无沙箱重跑） |
| `request_mode_change` | 无法变更权限模式 |
| `elicit`（`ElicitHandler`） | `ask_user` 不可用 |
| `run_hook` | 无生命周期钩子 |

配套权限 trait：`PermissionEngine`（`permission/engine.rs:69`，裁决 Allow/Deny/
Ask/Fatal）、`ApprovalReviewer` 与 `ApprovalHandler`（`types/permission.rs:757`
/`:764`，前置复核 + 最终裁决，复核非允许即 fail-closed）。

### Optional user-question lifecycle

`ask_user_async` returns acceptance without waiting for a user answer. It is
preferred for optional preferences and clarifications outside plan mode when
independent work can continue. `ask_user` keeps the blocking elicitation contract
for required decisions and plan clarifications; MCP elicitation is unchanged.

Clients call `session/getPendingQuestions` with `sessionId` and receive a direct
`{questions:[...]}` snapshot. Each record contains `requestId`, originating
`turnId`, `toolCallId`, `request` (full `questions`, `prompt`,
`attachmentsSupported`, optional `metadata`), `state`, and cumulative `answers`.
The full question array preserves the stable identity `requestId:index` when
some questions are skipped. Question text is unique within a batch.

`session/answerQuestion` takes `sessionId`, `requestId`, `turnId`, a stable
`clientResponseId`, `answers` keyed by question text, and optional `attachments`
keyed by answered question text. It resolves only the supplied subset. Its direct
receipt follows persistence, with `accepted`, the request/turn/response IDs,
`userMessageId`, and `state`. Repeating exactly the same submission returns the
same receipt; reusing the ID for different answers or images is rejected. A new
response cannot be admitted after the original turn ends and never enters the
ordinary steering queue. Accepted replies are drained before that same turn can
finish, including a text-only closing model response.

Product notifications are `question/requested` (`question`) and
`question/resolved` (`questionResolved`), with session/submission/revision fields.
Their raw `EngineEvent` tags are `question.requested` and `question.resolved`.
Resolved partial batches remain `pending`; terminal unresolved batches become
`expired` or `cancelled`. A write admitted before interruption may finish
persisting: its terminal resolution includes the real `userMessage`, and its
receipt has the terminal state. Terminal settlement republishes durable replies
with stable UUIDs to cover a producer-close race; clients deduplicate by UUID.
Failed persistence emits a `pending` reset with cumulative prior answers and null
receipt/message fields, so reconnected submitting clients can retry. Delayed
request publication uses the current snapshot and cannot reopen an answered
batch. Per-turn requests are capped at 32 batches of one to four questions.

Each answer is an ordinary persisted user message with optional typed
`questionAnswer` provenance: request/turn/tool-call/client-response IDs, original
question definitions, and the answer subset. The client renders its question
context as a quoted preview and uses normal user-message/history presentation.
Question expansion, deferral, countdown, and transcript collapsing belong to the
client. Reconnecting to the still-running session uses the pending getter;
resuming an ended rollout restores history without an interactive request.

### 3.3 能力类注入

不接入时对应工具**不注册**，模型看不见，不会出现"调用了但报错"。

- **子代理与编排**：`spawn_sub_agent` / `spawn_background_agent` /
  `spawn_workflow_agent`、`background_tasks`（`BackgroundTaskRegistry`，支撑
  `task_output`/`task_stop`）、`swarm`（`SwarmController`）、`depth`、
  `forward_subagent_text`、`enqueue_background_notice`。
- **技能**：`activate_skill_paths`、`apply_skill_context`、`run_bash`（受权限门约束
  的技能内联命令）。
- **工具自省与上下文**：`tool_catalog` + `surface_tools`（`tool_search`）、
  `conversation_snapshot` + `snip_messages`（`snip`）、`invoke_tool`（按名走完整引
  擎管道）、`record_rollout_event`。
- **其他控制器**：`worktree`（`WorktreeController`）、`cron`（`CronController`）、
  `lsp`（`LspController`）、`workflow_journal`（`WorkflowJournal`，确定性恢复）、
  `mcp_clients`、`browser` + `browser_tool_mapping`、`task_list_id`。
- **环境**：`shell_env`（登录 shell 环境捕获；缺失视为该能力关闭）、`native_pdf`
  （缺失**视为支持**）、`plan_file`、`pre_plan_mode`。

### 3.4 工具集装配（BuiltinsOptions）

`tools/builtins/index.rs`。与 3.3 的区别是这些在**建工具集时**决定，而非每次调用：

`task`、`web_search`（检索后端；后端级替换仅此一途，跨进程宿主可用 3.6 整体接管）、
`agents`、`skills`、`skill_gate`、
`workflows`、`web_fetch_page`（抓取实现，**仅此一途**）、`browser_runtime`、
`browser_runtime_available`、`mcp_catalog`、`skill_catalog`、`plugin_catalog`（三个商店
目录，**均仅宿主注入**）、`plugin_management_home`、`process_context`（code-mode 宿主
子进程启动上下文；没有则 cell 退化到本进程执行）。

注意：本节与 3.3 的所有口子都是 Rust trait/闭包，**只对同进程宿主（SDK 嵌入）可用**。
跨进程宿主（stdio 协议）的工具注入口是 3.6 的 dynamic tools。

### 3.5 生命周期钩子

`HookFn`（`hooks/types.rs:413`）+ `HookEvent`（`:31`）共 18 个时机：
`Setup`、`SessionStart`、`SessionEnd`、`UserPromptSubmit`、`PreToolUse`、
`PostToolUse`、`PostToolUseFailure`（工具**抛错**而非返回结果，携带 `error`/
`is_interrupt`）、`Stop`、`SubagentStart`、`SubagentStop`、`PreCompact`、
`PostCompact`、`Notification`、`TaskCreated`、`TaskCompleted`（后两者返回 `block`
可**否决**创建/完成）、`WorktreeCreate`（返回 `worktreePath` 即接管创建）、
`WorktreeRemove`。

钩子不只是观察位——`PreToolUse`、`TaskCreated`、`TaskCompleted` 能否决动作，
`WorktreeCreate` 能接管实现。

### 3.6 跨进程宿主的工具注入（dynamic tools）

3.3/3.4 都是同进程口子；跨进程宿主给模型加工具走协议的 `dynamicTools`：

- **声明**：session init 携带 function / namespace 两种规格（name / description /
  inputSchema / deferLoading）；引擎为每个规格装配一个适配器进工具集
  （`protocol/src/server.rs` `DynamicToolAdapter`），deferLoading 映射为延迟曝光。
- **调用**：模型调到时引擎发 `dynamicToolCall` 事件（携带 callId / threadId /
  turnId / arguments，会话归属天然在位），阻塞等宿主的
  `session/resolveDynamicToolCall`——**无超时，只有 abort 释放**，所以宿主必须对
  每个调用都给答复，不认识的工具也要答失败而不是沉默。
- **结果**：`inputText` 与 `inputImage`（base64 data URL → `OutputBlock::Image`，
  图片不占文本预算）；文件类产物由宿主自行落盘、以文本回路径。
- **命名**：`^[a-zA-Z0-9_-]+$`，`mcp` / `mcp__*` 保留；namespace 工具的模型名是
  `<namespace>__<tool>`，且 namespace 不得碰 OpenAI 保留命名空间。
- **重名即接管**：宿主声明的名字归宿主所有——与内建工具重名时内建让位
  （`engine/agent.rs` 的 host_owned 去重），模型永远只看到一个名字的一个实现。
  宿主声明自己的 `web_search` 就是整体接管检索面，不需要引擎侧再留代理开关。

与 MCP 的分工：MCP 面向**第三方服务**（自带握手、OAuth、信任、连接生命周期、
双向 elicitation）；dynamic tools 面向**宿主自身能力**（检索代理、生图、生视频、
定时任务这类纯服务调用），无进程边界、无 `mcp__` 前缀。需要在执行中反问用户的
能力（如逐 App 授权的电脑操控）仍应走 MCP——dynamic tools 没有反问通道；浏览器
类能力走 capability root + MCP（`hostReference` 在 stdio 上被明确拒绝，见
`runtime_capability_root_from_init`）。

---

## 四、存储、会话与上下文

| 口子 | 位置 | 语义 |
| --- | --- | --- |
| `SessionStore` | `store/session_store.rs:543` | `create`（返回 `LiveRollout` 写句柄）/`resume`/`list`/`archive`/`unarchive`/`delete` |
| `LiveRollout` | `store/session_store.rs:55` | 单会话追写句柄：`append_message`、`append_turn_context`、`append_compaction`、`append_event`、`append_rollback`、`append_checkpoint`、`append_usage`、`append_subagent_lifecycle`、`update_title`、`flush`、`shutdown` |
| `CompactionPolicy` / `Compactor` | core | 上下文压缩的触发策略与压缩实现 |
| `ReminderProvider` | core | 系统提醒注入 |
| `AgentMemoryHost` / `AgentMemoryLayout` | core | 记忆宿主与磁盘布局 |

引擎自带 `JsonlSessionStore`（`store/jsonl_store.rs`）——append-only JSONL。SDK 的
`StoreOption`（`crates/sdk/src/lib.rs:243`）把选择摆明：`Dir(path)` 用文件式实现
（**适合桌面应用的生产选择**，不是仅供测试），`Custom(Arc<dyn SessionStore>)` 用于
云服务的库式后端。

store 本身是 `Option`：**不提供就没有持久化**，turn 结束历史即丢，`resume` 不可用。

---

## 五、MCP 与扩展

- `McpCatalog`（`mcp/mcp_hub.rs`）：`search`/`get`/`install`，MCP 商店。**仅宿主注入**
  （`BuiltinsOptions::mcp_catalog`）——引擎不内置任何商店端点或客户端。
- `McpAuthorizeHandler`、`McpElicitationPresenter`、`OAuthClientProvider`：MCP 侧
  鉴权与交互展示。**注意职责分离**：MCP Hub 只负责检索与写入连接配置，不参与连接
  建立和鉴权。
- `McpInProcessServer`、`McpResourceClient`：进程内 server 与资源客户端。
- **启用态随声明走，不由宿主私存**：`mcpServers` 条目自己的 `"enabled": false` 就是
  停用开关，`parse_mcp_server_entry` 会把它读进 `McpServerOptions.enabled`，
  `McpConnectionManager` 的 `connect_all` / `upsert` 直接跳过（`upsert` 还会把已连上的
  客户端关掉），`load_mcp_server_configs` 也不会把它算进 connect-ready 集。宿主不要
  另建一份私有启用表：那样同一份配置文件在不同客户端读出来的答案会不一致，引擎侧
  也无从判断该不该连。
- `SkillCatalog`（`extensions/skills/catalog.rs`，**仅宿主注入**
  `BuiltinsOptions::skill_catalog`）、`PluginCatalog`
  （`extensions/plugins/catalog.rs`）。插件目录走本地目录
  （`KLAUS_PLUGIN_CATALOG_DIR`），不走网络。
- `CodeModeSession` / `CodeModeSessionDelegate` / `CodeModeSessionProvider`。
- `WorkflowRunner` / `WorkflowScriptRunner` / `WorkflowJournal`。
- `Transport` / `JsonRpcTransport`：传输层抽象。

### 配置开关

`ResolvedConfig`（`schemas/config.rs`）的启停位与上述注入是**与关系**——开关打开
且后端存在才注册工具：`skill_catalog.enabled`、`mcp_catalog.enabled`、`workflow`、
`swarm`、`cron`、`lsp`、`code_mode_host`、`history_snip`、`todo_v2`、
`worktree`，以及 `web_search.{mode,max_uses,allowed_domains,blocked_domains,...}`
与 `web_fetch.{allowed_domains,blocked_domains}`。

---

## 六、环境变量直连

引擎仍从环境变量直读的配置。它们都是路由地址、目录位置或 provider 凭据，不包含
第三方服务的专有协议适配。列出来是为了让接入方知道**哪些行为可以从外部注入**。

### 6.1 清单

| 变量 | 作用 | 缺失时 |
| --- | --- | --- |
| `KLAUS_MODEL_CATALOG_URL` | 远端模型能力目录 | 不拉取，用缓存 + 内置基线 |
| `KLAUS_CONFIG_DIR` | 配置与缓存目录 | `~/.klaus` |
| `KLAUS_BASE_URL` / `ANTHROPIC_BASE_URL` / `OPENAI_BASE_URL` / `GEMINI_BASE_URL` | 模型网关地址 | 各 provider 公网默认端点 |
| `ANTHROPIC_VERTEX_PROJECT_ID` + `CLOUD_ML_REGION` | Vertex 路由 | 不走 Vertex；region 默认 `global` |
| `ANTHROPIC_FOUNDRY_BASE_URL` / `_RESOURCE` / `_API_KEY` | Azure Foundry 路由 | 不走 Foundry |
| `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` / `GEMINI_API_KEY` / `ANTHROPIC_AUTH_TOKEN` | provider 凭据 | 需 `AuthSource` 或构造参数 |

模型网关地址与 provider 凭据**不算偏离**——任何引擎都需要模型端点，可注入且有公网
默认值是正确设计。剩下的条目也均为路由/目录类配置，不再持有第三方服务的专有协议
与凭据。

---

## 七、已清理

### 7.1 `web_fetch` 的域名 preflight（`domain_info`）已删除

原实现在引擎内置了一个 HTTP 客户端，抓取前打一个远端服务问 `can_fetch`，
fail-closed（问不到就拒绝抓取）、10 秒超时、无重试、只缓存放行结果，且位于代理分派
**之前**——即使配了抓取代理也照打一次。

这违反准则 1 和 2：合规判断是宿主策略，引擎不该知道有个远端可以问，更不该让整个
抓取能力依赖一个引擎自己选的单点。相关常量、配置结构、环境变量入口
（`KLAUS_WEBFETCH_DOMAIN_INFO_URL` 等）、缓存、调用点与测试已一并移除。

**替代做法**：宿主在 `request_network_access` 里查自己的清单服务（链路 B）；若要同
时覆盖沙箱命令，再接 `NetworkPolicyDecider`（链路 A，注意它只能放宽）。超时、重试、
缓存和"连不上算过还是算不过"全部由宿主决定，这些本来就是宿主的策略。

### 7.2 内置检索后端（material hub）已删除

原实现在引擎里焊了一个具体检索服务的完整适配：私有签名算法
（`md5(secretKey + unixSeconds)` 塞进 `X-Auth-Signature`）、两种端点变体的请求体、
响应格式与富文本标签解析，外加 `BUILTIN_MATERIAL_HUB` 这个**编译期凭据槽位**
（全局可变静态量，等着发版时把 AK/SK 编进二进制）和 `KLAUS_MATERIAL_HUB_*` 环境
变量入口。它还是 `web_search` 的**默认**后端，优先于任何模型侧能力。

配套的 `with_fallback` 把主后端的 `Err(_)` 和"返回空结果"一并无声退到 DuckDuckGo，
错误对象直接丢弃且不记日志——主服务挂掉、凭据过期或签名错误时，表面现象只是
"搜出来的东西变差了"，无法从行为上区分，也无从排查。

这违反准则 2 和 4：供应商协议适配和凭据都是业务逻辑，不属于支撑层；把密钥编进
二进制更是没有任何一层能补救。整个 `material_hub.rs`（1448 行）、模块声明与装配点
已移除。

**当前搜索路径（2026-09-19）**：

1. 默认 `Client` 模式下，`web_search` 使用会话浏览器的 `TabsContent` 读取 Bing
   搜索页，返回有编号的标题、原始链接和摘要。浏览器由宿主或插件提供，无浏览器能力
   时明确报错；引擎不再直接抓 DuckDuckGo，也不隐式切换到模型检索服务。
2. 只有显式选择 `Native` / `Auto` / `Indexed` 时才探测 provider 的 native 检索能力，
   可用时丢掉客户端工具并发 server-side 请求；不支持时保留客户端搜索。
   `Off` / `Disabled` 整体关闭搜索。
3. 同进程宿主可经 `BuiltinsOptions::web_search` 注入后端；跨进程宿主可声明同名
   dynamic tool 接管（见 3.6）。失败会保留错误，不再静默换搜索源。

默认 Bing 请求只读取一个固定站点的搜索页，浏览器等待最多 20 秒，搜索时限 25 秒。
取消或超时后最多再等 1 秒让浏览器适配器完成请求清理。
遇到验证页、导航失败或无法识别的页面时返回错误并暂停该工具实例的 Bing 请求 30 秒；
只有页面明确标记无结果时才返回正常空集。搜索摘要仅供发现链接，正文仍由 `web_fetch`
读取。受限子代理保留内部搜索所需的隐藏 `TabsContent` 依赖，依赖在执行前校验固定
Bing 查询形状，不因此获得任意浏览器操作能力。

### 7.3 内置 MCP / Skill 商店的 HTTP 客户端与常量已删除

原实现在引擎里焊了两个具体商店服务的 HTTP 客户端（MCP 商店与技能商店
各一），各自带私有 REST 契约（`/api/v1/mcp`、`/api/v1/skills`、
`items` 数组、`map_item` 字段映射、Bearer 头）和两个**编译期端点+凭据槽位**
（空白时 `default_*_catalog` 返回 None，填上就自动拨号）。

这违反准则 2 和 4：商店是产品层概念，引擎不该知道"商店长什么样"，更不该把端点
和 token 编进二进制。两个 HTTP 客户端、常量、市场专属格式映射与全部相关测试
已移除（`mcp_hub.rs` 834→≈110 行，`catalog.rs` 1020→≈110 行）。

**替代做法**：引擎只保留 `McpCatalog` / `SkillCatalog` 两个 trait 与对应工具骨架，
商店后端由宿主经 `BuiltinsOptions::mcp_catalog` / `::skill_catalog` 注入（skill 侧
本次新增了注入口，之前只能走内置常量）。未注入时对应工具不注册，行为与以前
常量空白的构建一致。写入 MCP 配置的共享 helper（`mcp/config_file`）与本地 skill
安装（`extensions/skills/remote`）保留不动。

### 7.4 抓取 / 搜索的内置 HTTP JSON 代理已删除

原实现给 `web_fetch` 和 `web_search` 各挂了一条 HTTP JSON 代理路径：端点来自
`KLAUS_WEBFETCH_PROXY_URL` / `KLAUS_WEBSEARCH_PROXY_URL`，或在 CCR 模式下由
`KLAUS_BASE_URL` + `KLAUS_CODE_SESSION_ID` **拼出网关侧端点**；鉴权头取
`..._PROXY_AUTHORIZATION`，取不到就**回退用模型凭据 `ANTHROPIC_AUTH_TOKEN`**。
配上之后它**替代**直连（`web_fetch`）或**替代**搜索后端（`web_search`），是优先于
一切注入口的第一条分支。`web_fetch` 侧还附带一整套 provenance 协议
（`PROVENANCE_REQUIRED` → 询问用户批准 → 重试）。

这违反准则 2 和 4：引擎自己拼外部端点、自己拿模型凭据去打它，并内置了该代理的
专有请求/响应协议。而这条路是**多余的第三条路**——已有的
`web_fetch_page: Arc<dyn PageFetcher>` 与 `BuiltinsOptions::web_search` 注入口本就
能完整覆盖"走代理"场景，且由宿主自带凭据与超时策略。

两个代理的选项类型、环境变量解析、CCR 端点拼装、结果枚举、请求实现、
provenance 分支与相关测试已全部移除。

**替代做法**：宿主要让抓取或搜索走代理：同进程宿主在自己注入的 `PageFetcher` /
`WebSearchBackend` 实现里去打；跨进程宿主声明同名 dynamic tool（见 3.6）接管检索，
抓取代理则在宿主侧 MCP 工具里实现。凭据与失败语义归宿主。

### Atomic pending-input continuation

`turn/start` and `session/resumePending` acknowledge admission to the live
session's operation queue, not successful execution or model validation. A
closing endpoint or failed enqueue returns JSON-RPC `ENGINE_ERROR` without a
success result. A rejected enqueue releases only that submission's in-flight
reservation and live-item mapping; retrying the same identifier is not rejected
as a duplicate, and thread inspection does not report a phantom active turn.

`session/resumePending({sessionId, afterSubmissionId, submissionId?})` is the
host's post-turn continuation surface. `afterSubmissionId` is the last ordinary
`turn/completed.params.turnId`, which is the corresponding `turn/start` ACK's
`turnId`; it is not the core lifecycle event's independently minted `turnId`.

The request ACK is `{turnId}`. Before any new turn events, the same submission
emits `pendingResume: {started: true}` on the raw event stream. An empty queue,
an outdated/non-completed predecessor, or an interrupt fence emits
`{started: false, reason}` instead. This is terminal without a synthetic
`turn/completed`. A failure is a correlated error and is never retried by the
engine. A stopped or failed turn, a meta-input run, a goal-driven turn, and a
child or side-task session do not authorize automatic main-session continuation.
Enabling goal tools alone does not make an ordinary turn goal-driven. A turn
that starts with an active goal or changes its stored goal cannot authorize
this driver; a later ordinary user turn after a completed goal can.

The engine owns the entire handoff. One current FIFO batch of main-thread
now/next messages becomes the first new turn; later and child-directed input
stays queued. The normal run lifecycle still owns goal accounting and any goal
created during that turn; a second queue driver must not replace it. Each message keeps its original UUID, meta flag, content and provenance;
live steer submissions receive applied only after the handoff is durable. PDF
counts and bytes remain engine-owned and share the new turn's aggregate limits.
Do not round-trip a batch through `session/takePending` and `turn/start`.

Reservations for this boundary are recoverable. A dropped or cancelled turn
before handoff releases them. Recovery after process loss checks original UUIDs
against durable user records: unhanded messages return to pending, while already
persisted messages settle as drained without being displayed a second time.
Submission correlation remains endpoint-local, as it is for every queued input.

The host must serialize decisions for a live binding, wait for this outcome,
continue only after ordinary completed turns, cap consecutive continuations
(the desktop uses 25), and stop on any false outcome or failure. An interrupt
fences both currently active work and already-ACKed runs waiting on the engine's
serial chain, so a host must not retry those submissions after stop.

### Complete stored history without provider activation

`engine/readThreadHistory({threadId})` reads the full saved main transcript.
`engine/readSubagentHistory({threadId, agentId})` reads a saved child transcript
and adds its optional `meta` and `surfacedTools`. Both return original `history`
messages with durable turn maps and `agentNotifications`; structured tool output
is associated with the original result-message UUID. A repeated tool-use ID in
a different turn must not overwrite an earlier result's output.

These are display-history reads. Prompt compaction retains the older messages
and adds a compaction marker, while an explicit conversation rollback removes
the rewound user turns. Each opened JSONL file is read through its captured
length, so a concurrent append does not create an unbounded reader. Store hooks
must not acquire continuation writers, restore archived files, or initialize a
provider to answer either method. A store without the read-only hook returns an
error rather than falling back to session resumption.

### Foreground follow-up identity and explicit queue continuation

`session/steer` accepts optional `expectedTurnId` and `queueMessageId`. New
interactive hosts send both: `expectedTurnId` is the facade turn/submission ID,
and `queueMessageId` is a Host-selected UUID used by the visible message. The
engine checks the expected foreground identity and enqueues under the same
admission mutex that orders turn admission, interruption and terminal release.
A stale or inactive turn returns a correlated error without adding queue input.
The serial foreground roster is independently limited to 64 admitted operations,
including the active turn; overflow emits a correlated error before queueing.
Omitting these fields preserves compatibility with existing queue-management
clients. A supplied queue UUID stays unchanged in authoritative snapshots and
persisted history; duplicate pending or previously drained IDs are rejected.

`session/resumeQueued({sessionId, submissionId?})` is an explicit user-owned
foreground continuation after interruption, failure, or reconnect. It returns
`{turnId}` and emits the same correlated `pendingResume` decision as
`session/resumePending`; it does not require a completed predecessor. The
existing `session/resumePending` success fence remains unchanged, so background
automatic continuation cannot restart work stopped by the user. Both methods
use the core's atomic original-identity queue handoff; neither exports input to
the host or manufactures a replacement prompt.
