---
name: full
description: Full metadata skill
when_to_use: when complete metadata is required
when-to-use: ignored because underscore wins
argument-hint: <target>
allowed-tools: Read, Bash(echo hi, --flag)
model: gpt-test
effort: 80
context: fork
agent: code-reviewer
arguments:
  - target
  - 1
  - mode
user-invocable: false
disable-model-invocation: true
shell: /bin/zsh
---
Full body.
