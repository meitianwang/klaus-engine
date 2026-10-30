# 工程债务台账

本文档是 2026-07 全量工程债务梳理（22 项）的处置台账：已修项引用提交，
战略项记录现状数据与处置决策。与代码冲突时以代码为准；后续债务变化应
更新本台账。相关架构背景见 `docs/ARCHITECTURE.md`。

状态图例：✅ 已修复（引用提交）· 📋 已评估-立项待续 · 🤝 已评估-接受现状。

## 已修复项

| # | 债务 | 处置 | 提交 |
| --- | --- | --- | --- |
| 1 | Session 持久化并发字段散置（9 字段三组重复） | 收敛为 `PersistenceGate` 组件（event_gate/serial/finalized 三件套） | `6e3ca109` |
| 5 | `protocol/server.rs` 19.9k 行 | 七阶段：①测试外置 + `activity` 准入跟踪真模块化，19 869→7 807 行；②八个测试门闸与捕获汇迁出 `test_gates` 模块；③④记忆快照/队列/任务清单/记忆维护/子代理转写/侧任务列表十四个 op 分支方法化；⑤⑥⑦侧任务域七个、会话生命周期域六个、快照读与编排域十个分支方法化收官，`dispatch_secondary` 877→218 行、退化为纯路由（累计三十七个 op 域方法化） | `157c0f67` `66b5ba91` `1d3641f5` `72b70fe2` `e36537c1` `364426d3` `003c4912` `6208d16f` |
| 6 | Provider 层三份逐字重复辅助 | 提取到 `provider/util.rs`（diagnostic_string 等四个），净删约 110 行 | `544e8524` |
| 11 | 3 个基线漂移失败测试 | 验证 HEAD 已修复，行为基线 160/160 | —（验证项） |
| 12 | 文档文物（构建期计划当作现状文档） | 新增 `docs/ARCHITECTURE.md` 现状架构，构建期一次性计划移出仓库 | `c2b20185` |
| 13 | 19.2k 行 provider_behavior 评估装置参与生产编译 | `testing` 改为非默认 feature：生产构建零参与、封条零漂移；eval 显式开启，protocol/sdk/server 仅 dev-deps 开启 | `955cd4db` |
| 14 | 工具层重复辅助（shell 族 first_chars×4/code_str×2/输出预算常数×2） | 收敛到 `tools/builtins/shell_common.rs`；权限自检各工具语义各异，属合理内聚不强行抽象 | `473feea0` |
| 19 | LSP `Shared` 十个测试插桩字段污染生产结构 | 收敛为独立 `SharedTestProbes` 测试成员 | `2bbdb340` |
| 21 | CI 缺封条守卫 | 工作区门禁补入五个守卫脚本（std 源锚/浏览器资产锁/rust+tokio 进程源锁/carrier-guard 九目标） | `9a86c6e9` |
| 22 | Token 估算常数魔数化 | 常数已具名+文档化；模型差异化估算属行为变更，需随基线刷新立项 | —（评估项） |

修复过程中一并偿还的清单外积压（均为主干预存缺陷）：

| 缺陷 | 处置 | 提交 |
| --- | --- | --- |
| fmt/clippy 历史漂移与告警 | 全工作区清零并纳入 CI `-D warnings` | `09136219` `7419a14d` |
| 线协议金样漏录 role 字段 | 金样补录 | `005d7dc6` |
| Unix Rust193 浏览器 broker 四通道缺失 + sidecar 测试 14 失败 | KBU1 四 lane 实现 + 测试宿主 transport 合同迁移 | `1bdc05cf` `b4be9635` |
| 领域清单三轮积压漂移（session 拆分路径 / 基线更名误替换冻结绑定 / 68 个未登记线协议面） | 逐层补录，29/29 全绿 | `f7119a18` |
| 路径合同积压漂移（4451→4641 槽位、10 条新边）+ process 扫描器 test 挂载盲区 + wire 契约 testing 门控适配 | 全链修复，runtime-carrier-guard 九目标全绿 | `dad79ebd` |
| background/protocol 两处测试 flake | 文件信号化 / 环境抖动甄别 | `361914ac` 等 |
| 本机高并发负载抖动（环境特性，非缺陷） | 复现条件：`provider_behavior_trace --test-threads=4` 下最重三项（scripted_provider_full_emits_contract_complete_traces / adapted_code_mode_contract_mutations_still_fail_closed / code_mode_trace_uses_declared_exec_wait_replacement_without_legacy_alias）及 core lib 偶发 1–6 项 process/timing 类失败，伴随总耗时异常拉长（lib 常态 ~110s，抖动窗口 900–1200s）；甄别预案：降并发 `--test-threads=2` 或同参数复跑，确定性全绿则判负载抖动（语义破坏应确定性复现），如实记入提交信息 | 多轮取证：`3f5f54ab` `4ad50734` `6807e739` 等 |

## 立项待续项 📋

**#2 Session 拆分只是文件级**。session.rs 已按区域拆为 14 个兄弟分片
（`#[path]` 挂载，最大分片 session_core_state.rs 3.4k 行），但仍是单一
`Session` 类型持有全部状态。

设计评估（2026-07-27）：Session 本体约 60+ 字段，按职责可分五簇——会话状态机
（id/meta/active_turn_stack/模式覆盖等 ~15）、持久化（rollout/store/三个
PersistenceGate/durability 登记 ~12）、工具运行时（tools/mcp_*/lsp/browser ~15）、
并发与生命周期（abort/门控/maintenance/后台代理 ~12）、其他依赖（provider/
权限/钩子/可观测 ~15）。结论：不做大爆炸式“三大组件拆分”（字段间锁序与
门控耦合密集，行为风险不可控），改走“组件化蚕食”路径，每步零语义、
单独提交、过行为基线 160 项：
① MCP 簇已收敛为 `SessionMcpRuntime`（`1dcdc8dd`）；② 后台代理/子代理簇已
收敛为 `SessionSubagentState`（background_agents/lifecycle/transcripts/surfaced/
memory_recall_providers 五字段，约 20 处访问重写为 self.subagents.*）；③ 记忆维护簇已
收敛为 `SessionMaintenanceState`（snapshot/query_fatal/persistence/event_sink/
isolated_sink 五字段，约 30 处访问重写为 self.maintenance.*；注意与同名读取
方法 `memory_maintenance_query_fatal()` 区分）；④ Setup 生命周期簇已收敛为
`SessionSetupState`（serial/context_admission_serial/lifecycle/persistence/durability
五字段，约 106 处访问（含测试）重写为 self.setup.*；先核实无同名方法/无
`.field(` 调用形方提取）；⑤ 自动记忆抽取+Dream 调度簇已收敛为
`SessionMemoryScheduler`（last_memory_message_uuid/extraction_in_progress/
extraction_pending/last_dream_scan_at/dream_in_progress/next_dream_owner/
active_dream_owner 七字段，约 70 处访问重写为 self.memory_scheduler.*；
测试专用 dream_before_lock_hook 保留为独立 Session 字段）；⑥ Agent Memory
快照决策簇已收敛为 `SessionSnapshotState`（policy/decisions/persistence/
durability/fatal_events/self_ref/ordered_sink/events 八字段，约 72 处访问重写为
self.snapshot.*；注意与同名查询方法 `agent_memory_snapshot_decisions()` 用负向
前瞻区分，`snapshot_self` 因 `self` 为保留字改名 `self_ref`；外部结构体
`StartSessionOptions`/`SessionInit` 的同名字段回退保留）；⑦ 权限模式
控制簇已收敛为 `SessionModeControl`（pending_mode_override/
pending_model_override/pre_plan_mode/permission 四字段，14 处访问重写为
self.mode.*；注意与 `ToolContext` 的同名 `pre_plan_mode` 字段区分（ctx.* 回退
保留））；⑧ 记忆根与嵌套记忆加载簇已收敛为 `SessionMemoryRoots`
（main/pinned_root/pin_error/internal_root/nested_triggers/nested_loaded_paths
六字段，30 处访问重写为 self.memory_roots.*；字段被 sandbox/file_state 分隔，
拆分时将 nested_* 上提入簇）。

⑨ 阶段 A 设计评估（2026-07-28）：盘点 Session 剩余字段分两类——A 类构造期
只读依赖约 30 个（provider/deps/config/tools/sandbox/file_state/governor/
permission_engine/approval_*/hook_bus/lsp_manager/browser_*/skills_thunk 等，
无内部可变性或仅注入一次，蚕食收益为零，保留原位）；B 类可变运行态散置
字段约 25 个，按三边界归簇如下（访问计数限 Session 分片文件 + 构造/rewind/
fork/query + turn 测试）：状态机边界——⑨-a 回合栈簇（active_turn_stack/
next_turn_generation/turn_active/current_abort/thread_started_emitted/
cache_policy_warned，约 25 处）、⑨-b 对话历史簇（history/
hidden_context_message_ids/message_queue/resumed_checkpoints，约 36 处）、
⑨-c 工作树簇（cwd/original_cwd/current_worktree，分片内约 29 处）、⑨-d 生命
周期簇（disposed/abandoned_resume_cleanup/abandoned_rollout_shutdown/
shutdown_completion，约 40 处）；持久化边界——⑨-e 持久化簇（rollout/store/
checkpoint_svc，约 42 处；side_task_* 维持既有裁定不并入）；工具运行时
边界——散置可变字段仅 surfaced_tools（2 处），其余已入 ① MCP 簇或属 A 类
只读注入，无需新簇，surfaced_tools 待后续搭车。锁序风险核查：
`ActiveTurnLease::try_new/drop` 在持 active_turn_stack 锁期间嵌套获取
current_abort 锁并调用 mcp_elicitation_router（内部锁）；session_isolation.rs
在 current_worktree 锁作用域内写 cwd/original_cwd 两把 RwLock——字段搬家不改
加锁语句顺序故蚕食安全，但禁止簇级合并锁/统一锁，逐字段保留原锁粒度。
同名冲突面：Session 有 `fn cwd()`/`cwd_now()`/`fn history()`/
`history_snapshot()` 同名方法（重写用负向前瞻排除调用形）；cwd/store/history/
disposed 在 apply_patch/runtime_process/lsp_manager/browser/oauth/code_mode/
js_runtime/subagent_lifecycle/command_runtime/session_store 等其他类型上有
同名字段（全仓 .cwd 303 处、.store 169 处、.history 129 处），批量重写必须
限 Session 分片文件白名单并逐文件核对。结论：否决“提取独立子类型 +
Weak<Session> 委托”——late-bound OnceLock 字段（reminder_providers/
edited_files_reminder/shutdown_completion）与 snapshot.self_ref 的两段式初始化
会扩散到子类型构造，pub(crate) 字段访问改委托方法的等价性验证面过大，且
触发路径合同槽位大规模迁移；三边界仅作簇的归类框架，继续按内聚簇小步
蚕食，阶段 B 按 ⑨-a…⑨-e 每簇一次提交、沿用既有验证门禁全套。阶段 B 进展：
⑨-a 回合栈簇已收敛为 `SessionTurnState`（active_stack/next_generation/
active/current_abort/thread_started_emitted/cache_policy_warned 六字段，
22 处访问重写为 self.turn.*；构造点三处分散初始化上提入簇，均为无副作用
字面量故重排零语义；ActiveTurnLease 的嵌套持锁顺序逐语句保留）；
⑨-b 对话历史簇已收敛为 `SessionConversationState`（history/
hidden_message_ids/resumed_checkpoints/queue 四字段，32 处访问重写为
self.conversation.*；用负向前瞻排除 `fn history()`/`fn resumed_checkpoints()`
同名方法调用形、负向后顾排除构造点 init.hidden_context_message_ids 外部
同名字段；browser/command_runtime/agent/query 的同名字段回退保留）；
⑨-c 工作树簇已收敛为 `SessionWorkspaceState`（cwd/original_cwd/
current_worktree 三字段，17 处访问重写为 *.workspace.*；重写限定接收者
self/this/this_e/session，排除 wt.original_cwd（WorktreeSession）、
init.current_worktree/init.cwd（SessionInit）、ctx.cwd/opts.cwd 外部同名字段
与 `fn cwd()`/`cwd_now()` 调用形；SessionContextSnapshot 的同名 cwd 字段
回退保留；current_worktree 锁作用域内写两把 cwd RwLock 的语句顺序逐行
保留）；⑨-d 生命周期簇已收敛为 `SessionLifecycleState`（disposed/
abandoned_resume_cleanup/abandoned_rollout_shutdown/shutdown_completion
四字段，41 处访问重写为 *.lifecycle.*；重写限定接收者 self/this/this_e/
session/task_session/shutdown，排除 lsp_manager 自有 lifecycle.disposed 与
`is_disposed()` 调用形；三处多行链式访问（接收者换行）正则漏改由编译
报错兜住后手工补改；disposed 保持 Arc<AtomicBool> 克隆语义不变）；
⑨-e 持久化簇已收敛为 `SessionPersistenceState`（rollout/store/
checkpoint_svc 三字段，36 处访问重写为 *.persistence.*；重写限定接收者
self/this/this2/session，多行链式访问用换行感知规则覆盖；排除
sopts./init./resumed./rollout_guard. 外部同名字段、ResumedRolloutInitGuard
自有 rollout、AtomicBool::store()/`fn rollout()`/`fn checkpoint_svc()`
调用形）。至此阶段 B 五簇全部收官，Session 可变运行态已收敛为 13 个
内聚子结构体字段；剩余散置面为小型独立态（workflow_journal_log/swarm 两
字段/task_list 三字段/side_task 两字段（维持既有裁定）/surfaced_tools/
background_tasks/maintenance_tasks/maintenance_admission/transport_prewarm
两字段/shell_env/cost 等），均为单一职责单字段或二元小簇，聚簇收益递减，
后续仅在触碰相应代码时顺带评估，不再单独立项。

复查修正（ 2026-07-27）：原拟的
“side_task_persistence_gate 封装进 PersistenceGate”一步撤销——该门控只有
`touch_side_task` 单入口串行语义，无 event_gate/finalized 需求，强套三件套
会引入两个死成员，不属于 #1 收敛范式的适用面。
每步都触发路径合同槽位迁移（工具链已在 #4 十四轮中验证）。

**#4 超长函数**。`query.rs::run_turn` 已按编号阶段拆完：八轮提取（入口
hooks/预压缩/流重试/反应式压缩/工具结果/循环守卫/Stop hook/预算续跑），
1 102→486 行，余下循环骨架与 Outcome 分流及 6/6b 两个 20 行小 nudge 块
（提取样板成本高于收益，保留），见 `67e21b92`…`f396c210` 八提交。
`session_spawn.rs::run_sub_agent` 已拆六阶段（系统提示 thunk/工具上下文工厂/
reminder 装配/resume 加载/记忆包/收尾终结），1 141→773 行，见 `2f9be287`
`96588732` `a9f5c042` `f39215aa` `cdc8cc86` `115ab1af`；stream! 宏体内提取限于
无 yield 闭合块（提前退出经 bool/Result 承载），宏体槽位 token 随体刷新、迁出后
浮出的新槽位按 in-memory 模板补登记。余下块保留：QueryParams 组装与回调接线
（局部变量密集，提取样板成本高于收益）及事件转发循环（含 yield 不可迁）。
其余粗测 top 经复核已非长函数（`validate_input_for_turn`/`apply_permission_update`
均 ~13 行，早期粗测数据陈旧）；当前实测最长的生产函数 `shutdown_once`（会话
拆卸，381 行）已拆出清理阶段（`run_shutdown_cleanup_phase`：SessionEnd hooks/
LSP/MCP/隔离维护 sink/TodoV2 watcher/swarm TeamCreate 六路并发清理）与持久化
阶段（`run_shutdown_persistence_phase`：Setup/快照 durability 排空 + 四类持久化
生产者围栏 + 最终 rollout flush/shutdown），381→121 行，槽位与 fs syscall 边
锚点随符号迁移重绑。

**#5 后续：Session 发布事务模块化**。`dispatch_secondary` 的全部 op 分支已
方法化收官（218 行纯路由，见已修复项 #5）；剩余方向是 Session 发布事务主体
沿 `activity.rs` 模式模块化，按需推进。app-server 面的逃生舱收敛已同步推进：
实测残留清单（run + 13 个 session 查询 + session/engine shutdown +
list_sessions）已升格为一等 JSON-RPC 请求方法，`session/op`/`engine/op` 仅剩
写操作类原生 op（steer/rewind/MCP 变更/队列变更等），后续沿同模式按实测
用量逐组升格。

**#10 store 无迁移框架**。JSONL 存储的行格式已有 kind 判别（9 种行型）与
容忍读（坏行/未知 kind 跳过），且 `ROLLOUT_SCHEMA_VERSION = 2` 已在
session_meta 头行携 schemaVersion（v2 相对 v1 全部为可选字段、向后兼容）。
真实缺口比最初评估小：缺的不是版本标记而是“读侧版本分派 + 升版迁移通道”。
方案（随基线刷新立项，不单独改 wire）：① 读侧先行——resume 时把头行
schemaVersion 透出到 ResumeResult，对 ≥ 当前版本+1 的文件拒绝而非静默容忍
（避免新写旧读静默丢语义）；这一步会改变“未来版本文件”的可观测行为，属
wire 行为变更，须与基线刷新同批落地并同步金样；② 升版时约束“新增字段必为
可选、新增 kind 走容忍 event 壳”的 v2 成例，避免引入硬迁移；③ 仅当确实需要
破坏性重写时才引入逐文件重写迁移器（目前无需求，不预建）。

**#16 SDK 公开面偏大**。盘点（2026-07-27）：`sdk/src/lib.rs` 约 45–50 个顶层
pub 项，其中嵌入门面本体仅 ~8 项（AgentEngine/Options/StoreOption/SystemPrompt/
SdkError/EventStream 等），其余主体是 core 类型透传 re-export；browser_host 已是
private 模块仅导出两个 CLI 入口函数（两个 3 行 bin 包装），内部模块均不可达
（有 compile_fail 封条），工作区内除契约文件外无其他使用方。结论：耦合风险
已被 private 模块 + 封条控制，独立 crate 拆分收益不足（会牵动路径/领域两契约
的文件身份登记）；收敛方向改为：① 待浏览器 cutover 完成后将两个 CLI 入口函数
标 `#[doc(hidden)]`（仅 bin 使用）；② re-export 面在出现真实外部嵌入方后按实际
使用面裁剪，在那之前保持现状（无使用方即无裁剪依据，提前收窄只会阻碍首个
嵌入方接入）。

## 接受现状项 🤝

**#3 三套执行通道**（turn 循环 / `engine/flow` JSON 步骤解释器 /
`engine/workflow` 运行时，合计约 10k 行）。三者服务不同产品面
（对话 turn、声明式 flow、工作流 Agent）；实测修正（2026-07-30）：flow/
workflow 两域对 `engine::query` 零引用，与 turn 循环共享的不是 query 执行
内核，而是 `ToolContext` 工具调用接缝（仅 flow/interp.rs 触及）；
是既定的产品语义，不是意外重复。合并会改变行为面，不做。

**#7 自制契约封条体系**。已于 2026-07-28 按产品决策整体退役：源结构级
契约（路径合同/领域清单/wire 契约/进程面契约四份冻结 JSON + AST 扫描
测试 + carrier-guard）与 Node 等价层（源锚 lock、Node oracle 金样与
脚本、活体 Node 差分测试、源 pin 文件）全部删除。它们的使命是移植期
保真，移植收官后守护价值与输出级金样重叠而重构税由每次改动承担。
例外：`rust-1.93-std-source.lock.json` 作为数据文件保留——它被浏览器
Windows addon 的源闭包（exact-browser-runtime-assets 的
addonSourceClosure）引用，已发布的外部构建回执绑定了其哈希，本地无法
重签；其自身的守卫脚本与测试已删。
保留的输出级面：wire-schema 金样（runtime_wire_schema_golden）、insta
快照、codec 单元测试、provider 行为基线、资产锁与 history 哨兵。

**#8 巨型文件内联测试**。`provider_behavior.rs` 19.2k 行（含内联测试）
已通过 `testing` feature 脱离生产编译（`955cd4db`）；源级契约退役后
（#7）物理拆文件已无冻结登记阻碍，后续可按需拆分，不再单独立项。

**#9 `memory/snapshot_decision.rs` 6.2k 行**。Agent 记忆快照决策的
全部状态机 + 密集内联测试；行为由行为基线与穷举式单元测试覆盖。体量主要
来自穷举式测试，非过度工程；保持现状。

**#15 MCP/LSP 自研实现**。行为基线与线协议金样要求输出逐字节稳定，
外部协议库无法保证这一点。不替换。

**#17 windows-sandbox 上游漂移**。上游改造关系已记录于
`crates/windows-sandbox/UPSTREAM.md`，随上游同步属维护例行事项。

**#18 双语言浏览器栈**（Rust 宿主 + Node sidecar/extension）。Chrome
扩展与 Playwright sidecar 必须是 JS；Rust 侧 broker 已按 KBU1 协议
钉死通道语义（`1bdc05cf`），边界清晰。

**#20 ripgrep 平台壳 crate ×13**。每个 17 行，是按 target cfg 选择
嵌入资产的标准 Cargo 模式，且被分发检查脚本锁定；无收敛价值。

## 引擎结构梳理专项（2026-07-30）

对 `crates/core/src/engine/`（92k 行）的代码实测梳理出 9 项结构问题，
已按 ④→①→②→③→⑤→⑥→⑦→⑧→⑨ 顺序逐项整改、每项独立提交：

| # | 问题 | 处置 | 提交 |
| --- | --- | --- | --- |
| ④ | 可见性未收口 | 14 个 crate 外零使用模块降 `pub(crate)`；session.rs glob 再导出改显式清单 | `1914d46` |
| ① | `use super::*` 顶层 glob 导入 | query 8 文件 + session 13 分片全部展开为显式导入（仅 `mod tests` 内保留） | `2a68997` `b194ccf` |
| ② | 下层反向依赖 engine | schema 类型归还 schemas/types；command_runtime 迁出 engine 至 core 顶层 | `9cff09e` `2773475` |
| ③ | 行为未落簇 | 13 簇全遍历：7 簇字段访问收归 + Dream 协议上移；mcp/persistence/maintenance/snapshot 提取同步字段面；subagents/setup 定性编排型留原地 | `0a44225`…`70501bc`（八提交） |
| ⑤ | 脚本执行域散置 | code-mode 三分片收入 `engine/code_mode/{protocol,runtime,runtime_impl}`；备选 runner lane 后续整体删除（见下） | `f9e88fd` |
| ⑥ | 线协议与队列行为同文件 | message_queue 的 12 个 wire 类型 + 手写 codec 拆出 `message_queue/wire.rs`，对外路径经显式再导出不变 | `22b91da` |
| ⑦ | 撞名/名实不符 | agents→agent_definitions、session_state→session_lifecycle_state、session_support→session_outcomes、session_turn→session_run | `a1b1d9f` |
| ⑧ | 测试/夹具位置失范 | `__fixtures__`→`fixtures` ×10、query_tests 入 query/、session_run_tests 与被测分片配对 | `5a7a211` `dd3689e` |
| ⑨ | 残余大文件 | 评估后均不拆（见下） | —（评估项） |

**⑤ 后续裁定（2026-07-30 复评，改为删除）**：`CodeModeRunner` 接缝及其三个
实现（Flow / NodeJs / Hybrid）生产零构造、仅测试引用，且脚本引擎级抽象与
实际演进方向不符——可移植性应切在会话/传输层（同一运行时换进程或换端点），
而非换脚本引擎。故整体删除该 lane：`tools/code_mode/{mod,runtime}.rs`
（约 910 行）、`FlowCodeModeRunner`、`NodeJsCodeModeRunner`、
`HybridCodeModeRunner`、Node 桥的 code 分支、flow 的 8 个 code-mode-only
步骤（invoke/text/image/store/load/notify/exit/allTools）与 `FlowKind`
双上下文校验。code-mode 自此只有一条执行路线：`InProcessCodeModeSession`
（进程内 V8 cell 运行时）。flow 退化为工作流单一上下文，`validate_program`
去掉 kind 参数；`normalize_tool_name` 归入 `tools/builtins/code_mode.rs`
并由 repl 复用。

## 引擎结构梳理专项后续（2026-07-30）

**#21 code-mode 会话接缝已建成（2026-07-30 收官）**。删除脚本引擎级 lane 后，
可移植性重建在会话/传输层：`CodeModeSession` / `CodeModeSessionProvider` /
`CodeModeSessionDelegate` 三层契约（`engine/code_mode/session.rs`），两条生产
可达承载：进程内（`runtime.rs`）与 host 子进程（`remote_session/` 客户端 +
`host_service.rs` 与 `klaus-code-mode-host` 可执行），由 `code_mode_host` /
`code_mode_host_required` 两个配置项选择与约束回退。host 连接线协议在
`engine/code_mode/host/`（长度前缀帧 + V1 钉链载荷）。嵌套工具调用无论细胞
在哪侧执行都回到拥有会话的引擎管线。会话层另有静止前沿自省面
（`execute_to_pending` / `wait_to_pending`），以 `cfg(test)` 门控，因模型可见
契约只用让渡前沿。遗留：
`ToolExposure::allows_code_mode_bridge` 仍仅测试可达（工具目录过滤尚未接入
该判定），`engine/{flow,js_runtime,workflow}` 的 `pub` 可见性待下一轮收口。

**未建第三条承载（网络承载）的决定（2026-07-30）**。会话经网络连远端 host
的承载没有实现，是明确决定而非遗漏：本仓库不存在把 code-mode host 作为网络
服务部署的形态，建了就是一条生产不可达的备选 lane —— 正是 #21 本轮删除的那
类负债。前置条件也不齐：core 无 WebSocket 依赖（仅 server crate 有），且需要
一套等价的 TLS/代理策略载体。接缝已备好，届时无需改动状态机：连接层
（`remote_session/connection.rs`）对传输是泛型的，收发只要求
`AsyncRead` + `AsyncWrite`；连接生命周期归属由 `ConnectionOwner` 枚举表达，
非进程归属分支（`Detached`）已存在（当前 `cfg(test)`）。新增网络承载的工作
量是一个双工适配器加一个 Provider，不触碰相关性状态机与线协议。

**⑨ 评估结论（2026-07-30，均不拆）**：session_core_state.rs（3.9k，其中
3.3k 是单个 `impl Session` 状态访问器块）与 session_core_control.rs（3.2k，
单个 `impl Session` 控制块）无域级接缝，再拆只能在 impl 块内任意切割，
无内聚收益；真正的深层修复（Session 类型分解）已在 #2 中否决为高风险，
组件化蚕食路径已收官。session_controllers.rs（3.2k）唯一干净接缝是四个
controller 实现共约 280 行，体量不足以立分片。flow/interp.rs（删除
code-mode lane 后 2.1k）实为生产 0.8k + 内联测试 1.3k，生产面不大；
flow/program.rs（生产 1.6k）与 session_core_policy.rs（2.1k）各为单一域，
内聚完好。后续仅在触碰相应代码时顺带评估，不单独立项。

## 构建产物预算（2026-07-30）

**#22 debug 产物体积已纳入预算，单体 crate 为剩余根因**。target/debug 曾无声
积到 248 GiB（deps 169 GiB、40.9 万文件），占满四分之一磁盘。已做三层处置：
根 Cargo.toml 把 dev 降为 `line-tables-only`（三方依赖与构建脚本 `debug = 0`）；
`cargo clean --profile dev` 一次性回收；新增 `scripts/target-budget.mjs`
（`--report` / `--prune --keep-days N` / `--check --budget`）作常态治理。
实测单份 core rlib 由 550 MiB 降至 305 MiB，全工作区含测试目标一轮编译后
总体约 17 GiB。

2026-08-23 补两件。一是把 `--prune` 对增量缓存的无条件清空改成按 crate 逐个按
`mtime` 判定：实测本仓改一行重编开增量 5 秒、关增量 37 秒，清掉正在用的那份等
于把每次改动拖成七倍，所以常态清理只收已经不再编译的那些，腾空到底才用
`--keep-days 0`。二是把 `--check` 接进 `scripts/gate.mjs` 末位（预算 60G）：此前
该脚本只存在于文档里、没任何自动调用方，产物已无声涨回 91.9 GiB（增量缓存
50.9 GiB）。

剩余根因未动：`crates/core` 单体化使其单个 crate 就产出 6.78 万个 `.o`，
拆 crate 可同时改善编译时长与产物体积，但属高风险重构，与 #2 否决的
Session 类型分解同类，故只登记不安排。另：`debug = "line-tables-only"` 下
回溯仍带文件:行:列（已实测），但 lldb 看不到局部变量的类型化视图，如需
深度调试可临时以 `CARGO_PROFILE_DEV_DEBUG=2` 覆盖单次构建。
