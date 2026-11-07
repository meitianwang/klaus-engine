---
description: Inline expansion parity
allowed-tools: Bash(echo hi)
shell: /bin/zsh
---
dir=${KLAUS_SKILL_DIR}
session=${KLAUS_SESSION_ID}
arg=$ARGUMENTS
out: !`echo hi`
