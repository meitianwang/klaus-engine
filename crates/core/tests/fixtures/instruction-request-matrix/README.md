# Instruction request fixtures

These fixtures pin complete session instructions and native requests, including mode changes, user-input boundaries, tool batches, goals, compaction and resumption.

The mode authority boundary refers to trusted application instructions instead of a transport role name. A provider projects that authority into its supported instruction channel. This keeps mode changes controlled by the application on system-only endpoints as well as endpoints with a developer role. The mode behavior and mutation restrictions remain intact.

Role negotiation is exercised through the public provider surface in `r20261019_chat_instruction_roles.rs`. It preserves these native requests by default and changes only trusted message roles after a precise pre-output validation refusal.
