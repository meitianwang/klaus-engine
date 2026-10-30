# Skill 编写规范

本文面向要接入引擎的业务方，说明一个 skill 内部应该怎么组织、`SKILL.md` 应该怎么写，以及怎样让 agent 最大化发挥 skill 的效果。本文不涉及 skill 安装位置和分发方式。

## 1. 设计目标

一个好的 skill 不是给人看的完整使用手册，而是给 agent 的“专业任务说明书”。它应该让 agent 在合适的时机知道：

- 什么时候应该使用这个 skill。
- 使用这个 skill 后要遵循什么工作流。
- 哪些资料、脚本、模板需要按需读取或执行。
- 哪些边界、禁忌、校验步骤不能省。

核心原则：

- **清晰触发**：`description` 和 `when_to_use` 要让模型准确判断何时使用。
- **正文精简**：`SKILL.md` 只放关键工作流，不堆大段背景资料。
- **按需展开**：详细资料放到 `references/`，脚本放到 `scripts/`，模板素材放到 `assets/`。
- **可验证**：复杂或高风险任务必须写清楚检查步骤、失败处理和交付标准。

## 2. 推荐目录结构

```text
your-skill/
├── SKILL.md              # 必需：metadata + 核心工作流
├── references/           # 可选：领域知识、接口文档、规则、示例
│   ├── api.md
│   ├── data-schema.md
│   └── policy.md
├── scripts/              # 可选：稳定、可重复、易出错的操作脚本
│   ├── validate.py
│   └── export_report.py
└── assets/               # 可选：模板、图片、样例文件、静态资源
    ├── template.docx
    └── example.json
```

各目录职责：

- `SKILL.md`：必需。写触发信息和核心执行流程。
- `references/`：放 agent 需要读的详细知识。适合接口说明、数据字典、业务规则、长示例。
- `scripts/`：放 agent 可执行的确定性逻辑。适合校验、转换、导出、批处理。
- `assets/`：放 agent 使用但通常不需要全文读入上下文的文件。适合模板、图片、字体、样例工程。

不要在 skill 内放无关文件，例如通用 README、安装教程、变更日志、会议纪要。skill 内容越干净，agent 越不容易跑偏。

## 3. SKILL.md 基本格式

`SKILL.md` 由两部分组成：

1. 文件开头的 frontmatter。
2. Markdown 正文。

示例：

```markdown
---
name: invoice-review
description: Review invoices for compliance, missing fields, duplicated charges, and policy exceptions.
when_to_use: Use when the user asks to review, audit, validate, or summarize invoice documents or invoice records.
argument-hint: <invoice file or record id>
allowed-tools: Read, Bash(python scripts/validate.py:*)
---

# Invoice Review

## Workflow

1. Identify the invoice source from the user request or arguments.
2. Read the invoice content and required policy references.
3. Validate required fields, duplicate charges, totals, tax, vendor information, and approval status.
4. Run `scripts/validate.py` when structured data is available.
5. Report findings grouped by severity.

## References

- For policy thresholds, read `references/policy.md`.
- For structured field definitions, read `references/data-schema.md`.

## Output

Return:

- Summary
- Blocking issues
- Warnings
- Suggested next actions
```

## 4. Frontmatter 字段规范

### 必填字段

`name`

- skill 的唯一名称。
- 使用小写字母、数字、连字符。
- 建议短、具体、面向动作。
- 好例子：`invoice-review`、`log-query`、`api-contract-check`。
- 差例子：`helper`、`tools`、`business-skill`。

`description`

- 最重要的触发字段之一。
- 写清楚 skill 能做什么、适合什么任务。
- 不要只写“处理发票”“查询日志”这种过短描述。
- 建议包含动词和任务边界。

好例子：

```yaml
description: Query and analyze centralized service logs for troubleshooting errors, latency, login failures, and service behavior.
```

差例子：

```yaml
description: Log helper.
```

### 推荐字段

`when_to_use`

- 进一步说明什么时候应该使用。
- 适合写触发场景、同义表达、边界条件。
- 如果同时写了 `when_to_use` 和 `when-to-use`，引擎优先使用 `when_to_use`。

```yaml
when_to_use: Use when investigating incidents, bug reports, latency spikes, failed logins, or service behavior from service logs.
```

`argument-hint`

- 给用户斜杠命令或调用入口看的参数提示。

```yaml
argument-hint: <trace id | user id | time range>
```

`arguments`

- 定义命名参数，让正文里可以用 `$name` 代替位置参数。
- 可以写空格分隔字符串，也可以写列表。
- 纯数字名称会被忽略，避免和 `$0`、`$1` 冲突。

```yaml
arguments: trace_id start_time end_time
```

正文里可使用：

```markdown
Trace: $trace_id
Time: $start_time ~ $end_time
```

`allowed-tools`

- 声明 skill 执行时需要自动允许的工具或命令范围。
- 对 inline bash 有用，也会作为 skill 上下文能力的一部分传给运行时。
- 权限越具体越好。

```yaml
allowed-tools: Read, Bash(python scripts/validate.py:*)
```

避免：

```yaml
allowed-tools: Bash(*)
```

除非这是明确且可接受的业务风险。

`paths`

- 让 skill 只在相关文件被读取、编辑或触达后激活。
- 适合代码规范、特定目录、特定文件类型。
- 支持逗号分隔和简单 brace 展开。

```yaml
paths: src/*.{ts,tsx}, api/**
```

注意：

- `paths: **` 会被视为没有路径限制。
- `docs/**` 会归一化为 `docs`。
- 条件 skill 未激活前不会出现在模型可调用 skill 列表里。

`context`

- 默认是 inline。
- `inline`：skill 正文进入当前主上下文，由当前 agent 执行。
- `fork`：skill 作为隔离子 agent 执行，适合长任务、独立审查、大量阅读。

```yaml
context: fork
agent: code-reviewer
```

使用建议：

- 用户期望主对话持续协作、需要边做边解释：用 `inline`。
- 任务可独立完成、需要大量资料阅读、不想污染主上下文：用 `fork`。

`model` / `effort`

- 用于提示运行时对该 skill 使用特定模型或推理强度。
- `model: inherit` 表示沿用父上下文模型，不会设置 model override。
- `effort` 可写 `low`、`medium`、`high` 或数字。

```yaml
model: inherit
effort: high
```

`user-invocable`

- 是否暴露为用户可直接调用的 slash command。
- 默认 `true`。
- 只有布尔 `true` 或字符串 `"true"` 会被视为 true；`yes`、`1`、`on` 不会被视为 true。

```yaml
user-invocable: false
```

`disable-model-invocation`

- 是否禁止模型主动通过 skill tool 调用。
- 默认 `false`。
- 适合只允许用户显式触发的高风险 skill。

```yaml
disable-model-invocation: true
```

`shell`

- 指定 inline bash 使用的 shell。

```yaml
shell: /bin/zsh
```

## 5. Frontmatter 解析限制

为了两端引擎行为稳定，frontmatter 只支持一个很小的 YAML 子集：

- 支持 `key: value`。
- 支持简单 block list：

```yaml
allowed-tools:
  - Read
  - Bash(python scripts/check.py:*)
```

- 支持单层单双引号剥离。
- 不支持嵌套对象。
- 不支持 flow collection，例如 `{ a: b }`、`[a, b]`。
- 不支持多行 scalar、anchor、复杂 YAML 类型。
- 未知字段不会导致 skill 失效，但运行时不会使用它们。

建议业务方只写简单结构，避免依赖完整 YAML 语法。

## 6. 正文组织规范

`SKILL.md` 正文应该优先写 agent 执行任务需要的流程，而不是背景说明。

推荐结构：

```markdown
# Skill Title

## Goal

一句话说明这个 skill 要达成什么。

## Inputs

说明 agent 应该从用户请求、参数、文件或上下文里识别什么输入。

## Workflow

用编号步骤写清楚执行顺序。

## References

说明什么时候读取 references 下的哪些文件。

## Validation

说明必须做哪些检查。

## Output

说明最终回答或产物格式。
```

正文写法建议：

- 用明确动作开头：Read、Check、Run、Compare、Report。
- 用步骤代替长段落。
- 把“必须做”的事情写成硬约束。
- 把“可以考虑”的事情写成启发式，不要和硬约束混在一起。
- 对失败情况写清楚处理方式。

差例子：

```markdown
你是一个资深专家，需要根据实际情况灵活处理各种复杂问题，并输出高质量结论。
```

好例子：

```markdown
When the user provides a trace id:

1. Query logs for the exact trace id first.
2. If no result is found, widen the time range by 10 minutes.
3. Group findings by service name and error type.
4. Do not claim root cause unless at least two independent log signals support it.
```

## 7. 参数与变量

正文支持参数替换：

- `$ARGUMENTS`：完整参数字符串。
- `$ARGUMENTS[0]`：第一个解析后的参数。
- `$0`：第一个参数。
- `$1`：第二个参数。
- `$name`：由 frontmatter `arguments` 定义的命名参数。

示例：

```yaml
arguments: service_name trace_id
```

```markdown
Investigate service `$service_name` with trace id `$trace_id`.
```

如果正文里没有任何参数占位符，但调用时传了参数，引擎会在正文末尾追加：

```text
ARGUMENTS: <args>
```

正文还支持变量：

`${KLAUS_SKILL_DIR}`

- 当前 skill 目录。
- 用于引用 skill 内的 `scripts/`、`references/`、`assets/`。

示例：

````markdown
Run:

```!
python ${KLAUS_SKILL_DIR}/scripts/validate.py "$ARGUMENTS"
```
````

## 8. Inline Bash 使用规范

正文可以包含 inline bash 指令：

````markdown
```!
python ${KLAUS_SKILL_DIR}/scripts/validate.py input.json
```
````

也可以写行内形式：

```markdown
Result: !`python ${KLAUS_SKILL_DIR}/scripts/check.py`
```

建议：

- 能用脚本就用脚本，不要把复杂 shell 写在 `SKILL.md` 里。
- `allowed-tools` 尽量只放脚本或命令白名单。
- 脚本输出要简洁，避免把大量无关日志塞回上下文。
- 高风险操作不要放 inline bash，改成让 agent 明确说明并请求用户确认。

## 9. references 组织规范

当资料较长、变体较多、只在部分场景需要时，放到 `references/`。

推荐：

```text
references/
├── api.md          # API 接口、字段、错误码
├── policy.md       # 业务规则、阈值、审批规则
├── examples.md     # 输入输出示例
└── troubleshooting.md
```

在 `SKILL.md` 里必须明确什么时候读：

```markdown
## References

- For API parameters and response fields, read `references/api.md`.
- For approval thresholds and exceptions, read `references/policy.md`.
- For output examples, read `references/examples.md`.
```

不要让 agent 猜 references 里有什么。

长 reference 文件建议开头加目录：

```markdown
# API Reference

## Contents

- Authentication
- Query parameters
- Response fields
- Error codes
- Rate limits
```

## 10. scripts 组织规范

适合放脚本的情况：

- 规则稳定、重复执行。
- 手写容易错。
- 需要解析结构化文件。
- 需要生成固定格式产物。
- 需要做确定性校验。

脚本要求：

- 参数清晰。
- 输出稳定。
- 错误时返回非 0 exit code。
- 错误信息能指导 agent 下一步怎么处理。
- 不依赖当前工作目录，尽量用 `${KLAUS_SKILL_DIR}` 定位资源。

示例：

```text
scripts/
└── validate_invoice.py
```

`SKILL.md` 中写：

````markdown
When invoice data is available as JSON, run:

```!
python ${KLAUS_SKILL_DIR}/scripts/validate_invoice.py "$ARGUMENTS"
```

If the script exits non-zero, report the script error and do not invent validation results.
````

## 11. assets 组织规范

`assets/` 放最终产物会用到的静态文件，例如：

- 文档模板。
- 报表模板。
- 图片或 logo。
- 示例 JSON。
- 前端样板工程。

`SKILL.md` 里要说明如何使用：

```markdown
Use `assets/report-template.docx` as the base template when the user asks for a formal report.
Do not recreate the template from scratch.
```

## 12. 让 agent 效果更好的写法

### 写清楚触发边界

不要只说“处理 X”。要说“当用户要做 A/B/C 时使用；当只是问概念时不要使用”。

```yaml
when_to_use: Use for producing a compliance review of invoice files. Do not use for general accounting concept questions.
```

### 写清楚输入缺失时怎么做

```markdown
If the user did not provide a time range, ask for it before querying logs.
If a trace id is available, prefer trace id over keyword search.
```

### 写清楚输出格式

```markdown
Return findings in this order:

1. Blocking issues
2. Warnings
3. Evidence
4. Recommended next actions
```

### 写清楚证据要求

```markdown
Do not claim root cause from a single log line.
Quote the service name, timestamp, and error code for every incident finding.
```

### 写清楚禁止事项

```markdown
Do not modify production data.
Do not call write APIs.
Do not guess missing approval status.
```

### 把复杂知识拆出去

如果某段内容超过几十行，通常应该放到 `references/`，并在 `SKILL.md` 里写清楚读取时机。

## 13. 常见反模式

### 反模式：description 太短

```yaml
description: Query logs.
```

问题：agent 不知道哪些日志、什么场景、做什么分析。

改成：

```yaml
description: Query and analyze service logs to troubleshoot service errors, latency, login failures, and incident reports.
```

### 反模式：SKILL.md 堆满资料

问题：浪费上下文，agent 很难抓住执行流程。

改法：核心流程留在 `SKILL.md`，长资料放到 `references/`。

### 反模式：只有原则，没有步骤

```markdown
请专业、准确、全面地分析。
```

改成：

```markdown
1. Identify the affected service.
2. Query exact trace id if present.
3. Group errors by timestamp and host.
4. Compare first error timestamp with deployment time.
5. Report only evidence-backed conclusions.
```

### 反模式：权限过大

```yaml
allowed-tools: Bash(*)
```

改成：

```yaml
allowed-tools: Read, Bash(python ${KLAUS_SKILL_DIR}/scripts/check.py:*)
```

### 反模式：没有失败处理

问题：工具失败后 agent 容易猜结果。

改成：

```markdown
If the query returns no logs, state that no matching logs were found, show the query range, and ask whether to widen the time window.
```

## 14. 推荐模板

```markdown
---
name: your-skill-name
description: Clear action-oriented description of what this skill does and what problems it solves.
when_to_use: Use when the user asks to ...
argument-hint: <primary input>
allowed-tools: Read
---

# Your Skill Name

## Goal

Describe the concrete outcome this skill should produce.

## Inputs

Identify required and optional inputs.

Required:

- ...

Optional:

- ...

If required inputs are missing, ask the user for them before proceeding.

## Workflow

1. ...
2. ...
3. ...

## References

- Read `references/...` when ...

## Validation

Before final response:

- Check ...
- Verify ...
- Do not ...

## Output

Return:

1. Summary
2. Evidence
3. Findings
4. Next actions
```

## 15. 提交前检查清单

业务方提交 skill 前，至少检查：

- `name` 短、清晰、唯一。
- `description` 能准确表达能力和触发场景。
- `when_to_use` 写清楚使用边界。
- `SKILL.md` 正文是执行流程，不是长篇背景资料。
- 大段领域知识已拆到 `references/`。
- 稳定重复逻辑已放到 `scripts/`。
- 模板和静态资源已放到 `assets/`。
- 脚本能通过 `${KLAUS_SKILL_DIR}` 定位。
- 高风险操作有明确禁止事项或确认流程。
- 输出格式和证据要求清楚。
- 失败或缺少输入时不会要求 agent 猜结果。

一个 skill 写得越像“可执行任务流程”，agent 的效果越稳定；写得越像“泛泛的专家人设”，效果越不可控。
