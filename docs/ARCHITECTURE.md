# 架构总览

本文档描述仓库的**现状**架构，面向新成员与维护者。历史计划/设计文档
反映的是构建期决策，不再随代码演进维护；与本文冲突时以本文
和代码为准。

## 仓库布局

Cargo workspace，23 个成员 crate：

| crate | 职责 |
| --- | --- |
| `crates/core` | 无头引擎核心：会话、turn 循环、工具、Provider、沙箱、持久化 |
| `crates/protocol` | 宿主 ↔ 引擎的 NDJSON 线协议（`Op` 入 / `ProtocolEvent` 出） |
| `crates/server` | 网络传输层：WebSocket、HTTP+SSE、stdio 两个二进制入口 |
| `crates/sdk` | 进程内嵌入门面（`AgentEngine → Thread → run()`）+ 浏览器宿主 |
| `crates/icu-data` | 嵌入式 V8 所需的 ICU 77 数据 |
| `crates/windows-sandbox{,-path,-pty}` | Windows 受限令牌沙箱服务（上游改造，见 UPSTREAM.md） |
| `crates/browser-pipe-napi` | Windows 专用浏览器 broker 管道的 Node-API 桥 |
| `crates/ripgrep-*`（12 个） | 平台专属 ripgrep 二进制载体，经 target cfg 依赖选择 |

## 引擎核心（crates/core）

### 会话与 turn 循环

- `engine/agent.rs` —— `Agent` 工厂：持有引擎级配置，装配工具集/子代理/
  技能/工作流，铸造 `Session`（`start/open/resume_session`）。
- `engine/session.rs` —— `Session`：拥有对话历史、rollout、权限、hooks、
  MCP hub、后台代理、cron、memory 维护等全部会话状态。实现按领域分片在
  `session_*.rs` 文件中（`#[path]` 模块 + glob re-export，路径语义不变）。
  共享形态为 `Arc<Session>` + 内部可变性（`Mutex`/`RwLock`/原子量）。
  可变运行态已按职责收敛为十三个内聚子结构体字段：`SessionMcpRuntime`、
  `SessionSubagentState`、`SessionMaintenanceState`、`SessionSetupState`、
  `SessionMemoryScheduler`、`SessionSnapshotState`、`SessionModeControl`、
  `SessionMemoryRoots`、`SessionTurnState`、`SessionConversationState`、
  `SessionWorkspaceState`、`SessionLifecycleState`、`SessionPersistenceState`；
  剩余散置字段均为单一职责小型独立态，详见债务登记册 #2 ⑨。
- `engine/query.rs` —— 单个 turn 的模型调用循环：组装 `ModelRequest`、
  流式消费 `ModelStreamEvent`、调度工具（权限 + hooks + 沙箱）、折叠
  compaction / loop-detection / token 预算，一切以 `EngineEvent` 出流。

已知架构债：`Session` 构造期只读依赖约 30 字段保留原位，可变运行态已按
“回合栈/对话历史/工作树/生命周期/持久化”五簇收敛完毕（已否决独立子
类型 + `Weak<Session>` 委托方案，理由见债务登记册）；剩余散置字段为小型
独立态，仅在触碰时顺带评估；`query.rs::run_turn` 已按八阶段拆至 486 行。
setup / memory 维护 / snapshot 决策各自的 `*_event_gate` +
`*_persistence_serial` + `*_persistence_finalized` 三件套已随各簇收敛入
对应子结构体。

### 三套脚本执行体系

| 体系 | 位置 | 用途 |
| --- | --- | --- |
| 嵌入式 V8 isolate | `v8_platform.rs`（v8 = 147.4.0 + ICU 77） | 进程内 JS 执行（含 code-mode cell 运行时与持久 REPL 会话） |
| Node 子进程桥 | `engine/js_runtime.rs` | workflow 的 JS 程序（Node `vm`） |
| Flow 解释器 | `engine/flow/`（JSON 步骤 + CEL 表达式） | 原生工作流程序，效果全部经宿主沙箱 |

`HybridWorkflowScriptRunner`（`js_runtime.rs`）按源码判别 Flow 与 JS：
`is_flow_source` 命中走 Flow 解释器，否则走 Node。新增程序类语义时优先
扩展 Flow。

### code-mode 会话接缝

code-mode（`code_exec`/`wait`）的可移植性切在**会话/传输层**，而非脚本
引擎层：同一个进程内 V8 cell 运行时（`engine/code_mode/runtime_impl/`）
可以在本进程跑，也可以换到 host 子进程里跑。

| 层 | 位置 | 职责 |
| --- | --- | --- |
| 接缝契约 | `engine/code_mode/session.rs` | `CodeModeSession`（execute/wait/terminate/shutdown）、`CodeModeSessionDelegate`（invoke_tool/notify/cell_closed 反向回调）、`CodeModeSessionProvider` |
| 面向模型线协议 | `engine/code_mode/protocol.rs` | exec/wait 请求与响应、cell 事件 |
| host 连接线协议 | `engine/code_mode/host/` | 长度前缀帧 + V1 钉链载荷，双 id 空间（操作/delegate） |
| 进程内承载 | `engine/code_mode/runtime.rs` | `InProcessCodeModeSession` + Provider |
| 远程承载（客户端） | `engine/code_mode/remote_session/` | 连接驱动、会话状态机、子进程 Provider |
| 远程承载（host 侧） | `engine/code_mode/host_service.rs` + `klaus-code-mode-host` | 服务一个客户端的会话 |

无论细胞在哪侧执行，**嵌套工具调用总是回到拥有会话的引擎**走完整的
权限 + 钩子 + 沙箱管线（host 侧把回调转为对客户端的请求）。

配置：`code_mode` 开启工具对；`code_mode_host` 选择 host 子进程承载；
`code_mode_host_required` 禁止在 host 拉不起时退回进程内（要求隔离的部署
应硬失败而非静默在本地执行）。host 程序路径：`KLAUS_CODE_MODE_HOST_PATH`
覆盖 > 与当前可执行文件同目录的 `klaus-code-mode-host`。

### Provider 层

`provider/{anthropic,openai,gemini}.rs` 各自实现请求组装、SSE 解码与
事件映射（三者的 SSE 帧边界语义存在有意差异，不共享解码器）；逐字相同的
诊断/错误折叠助手集中在 `provider/util.rs`。`behavior.rs` 负责按 provider
风格渲染提示词 envelope。错误统一折叠为 `RawProviderError` →
`classify_error` → `ProviderError`。

### 运行时等价层

`runtime_process/` 在 Rust 中实现与 Node 20.19.4 / libuv 1.46.0 等价的进程与
路径语义（三平台各一套 spawn 实现，`node20_path_case_data.rs` 为脚本生成
的 Unicode 表）。`runtime_values/` 提供 UTF-16 语义的 `RuntimeString` /
`runtime_json`，与 Rust 原生 `str`/serde_json 并存；跨界必须经显式投影
（`try_to_utf8`、`to_node_utf8`、`diagnostic_string`）。

等价语义由行为测试直接覆盖（进程/路径/JSON 的功能测试与 codec 单元
测试）；曾经的源锚 lock、Node oracle 与源结构契约已于 2026-07 整体退役
（见 `docs/debt-register.md` #7），资产封条仅保留：

- `bundled-browser-assets.lock.json`、`playwright-process-primitives.lock.json`
- `provider-assets.lock.json` 与发行包检查脚本
- `rust-1.93-std-source.lock.json`（仅作浏览器 Windows addon 源闭包的
  数据输入，外部构建回执绑定其哈希；自身守卫已退役）

### 目标模式（goal/）

目标模式让一个线程带着一句「目标」自己一轮接一轮地跑，直到模型宣布达成
或卡住、预算花完、宿主暂停。`config.goal` 为真才建 `GoalRuntime`，关着时
目标工具不进工具表，续跑循环也不存在。

- `types.rs`：六态（active / paused / blocked / usage_limited /
  budget_limited / complete）+ `ThreadGoal` 记录。`goalId` 是代际号，过期
  写者的 `expectedGoalId` 对不上就被拒；只有 complete 的目标可被新目标
  替换，所以模型不能悄悄换掉目标。
- `store.rs`：落 `<config_home>/goals/<threadId>.json`，跨进程文件锁。两条
  不变式钉在存储层——「只能替换已完成的目标」和「到预算的同一次写入就翻
  budget_limited」。
- `accounting.rs`：按响应累加 `input - cached + output`。基线在建目标那一刻
  重置，所以建目标之前烧的 token 不记在这个目标头上。
- `runtime.rs`：单许可 semaphore 串行化所有状态转换；回合开始重置基线，
  回合结束决定是否续跑，回合失败/中断封住目标。模型只能要 complete 或
  blocked，暂停/恢复是宿主那一半。
- `steering.rs` + `templates/`：三种续跑指令（继续 / 预算收尾 / 目标被改），
  以 meta 输入注入，不进用户可见的消息流。
- 观测：目标转换不走事件流（快照直发宿主的 goal sink），所以
  `observability/otel.rs` 的 goal 计数器由 `session_goal.rs` 的发布漏斗喂——
  create / resume / 四个终态计数 + 终态的 token 与用时直方图。

两处刻意的取舍：目标转换不进任何产品埋点管道，也不为此新建一条向外发数据的
通路，观测面只留 otel；建目标时不用目标原文补线程标题，因为宿主在发第一条消息
时就已经用那条消息种好标题（`titles.rs`），不存在没有标题的空档。

### 其他子系统

- `tools/`：Tool trait 与 49 个内建工具；超额结果两级溢出在
  `result_storage.rs`。
- `store/`：追加式 JSONL 会话存储，崩溃容忍 resume，unarchive 走
  intent + marker 的多阶段持久化事务。
- `memory/`：Agent Memory 快照决策状态机（exactly-once + 崩溃安全
  marker）、consolidation 跨进程锁。
- `context/`：compaction（micro + full）、usage 锚定的 token 估算。
- `mcp/`：手写 MCP 客户端（JSON-RPC 2.0，协议版本 2025-11-25）+ 完整
  OAuth（PKCE / RFC 7591 / 8414 / 9728）。
- `lsp/`：手写 LSP 客户端（stdio Content-Length 分帧，pull/push 诊断）。
- `sandbox/`: canonical read-only, workspace-write and full-access policies use
  the platform's enforcing backend. Workspace roots, network access and temporary
  directory exclusions are captured together. Confined execution never degrades
  to an unrestricted process when platform isolation is unavailable.
- `permission/`、`hooks/`、`checkpoint/`、`worktree/`：权限规则引擎、
  hook 总线与 Setup 生命周期、检查点、隔离工作树。

### Runtime permission policy

The stored policy family uses `approval_policy`, `approvals_reviewer`,
`sandbox_mode` and `sandbox_workspace_write`. The workspace table owns
`writable_roots`, `network_access`, `exclude_tmpdir_env_var` and
`exclude_slash_tmp`. Approval review and filesystem enforcement are separate:
changing a reviewer never expands a sandbox. A session can capture an explicit
network selection independently of read-only filesystem access.

Legacy backend selection, read confinement, extra-root and enforcement/retry
controls are migrated once by the configuration writer and produce migration
notices. They do not participate in backend selection or execution policy.
Explicit rules, task resource scopes and planning ceilings continue to apply.

## 协议与宿主面

三种驱动方式，语义同源：

1. **进程内**：直接使用 core（或经 `sdk` 门面）。
2. **本地 stdio**：`protocol::ProtocolServer` 读取行分隔 JSON `Op`，回写
   `ProtocolEvent`；审批/elicitation 以 pending-map 反向请求宿主。
3. **网络**：`server::serve` 在同一端口暴露 WebSocket 与 HTTP+SSE，
   复用同一个 `ProtocolServer`；非回环绑定强制 Bearer token。

## 测试与金样基建

- **单元测试**：大量内联于 src（`session_turn_tests.rs`、`query_tests.rs`
  经 `#[path]` 外置但仍属生产模块），共享私有可见性。
- **线协议金样**：`tests/runtime_wire_schema_golden.rs` 以 syn AST 扫描
  serde 面并投影为输出级 wire-schema 金样（`UPDATE_GOLDEN=1` 重录）；
  insta 快照覆盖 codec 往返字节。
- **Provider 行为基线**：`src/testing/provider_behavior.rs`（经非默认 feature
  `testing` 门控，生产构建不参与编译）+ 签入的
  `tests/fixtures/provider_behavior/v1/` 快照；刷新流程见
  `docs/provider-behavior-runbook.md`。
- **门禁**：`node scripts/gate.mjs` 一次跑完 fmt / 浏览器资产锁 /
  发行资产校验 / 产物体积预算 / 脚本测试 / `clippy -D warnings` /
  `cargo nextest run`（预算与例外见 `.config/nextest.toml`）。

## 已知债务与演进方向

全量债务盘点与逐项处置见 `docs/debt-register.md`（已修项引用提交，
战略项含现状数据与决策）。仍开放的演进方向摘要：

1. `Session` 单体与超长函数（#2/#4）—— 行为基线护栏下分阶段收敛。
2. `protocol/src/server.rs`（现 7.8k 行）—— 沿 `activity.rs` 模式按 op 域
   继续拆分。
3. JSONL 存储的 schema 版本化与 SDK 公开面收敛（#10/#16）—— 待立项。
4. 引擎内置外部服务客户端的收敛 —— 引擎作为支撑层不应持有外部服务地址与
   凭据；宿主接入口子的完整清单、判定准则与当前偏离点见
   `docs/host-integration-seams.md`。
