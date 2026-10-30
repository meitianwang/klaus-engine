# Third-party notices

Klaus Engine is MIT licensed (see LICENSE). That license applies only to original
project material; third-party components keep their own licenses and notices.
This file lists the third-party material that ships inside this source tree and
where its license and notice files are kept. The files next to each component
are authoritative; this list is an index. Dependencies resolved by Cargo carry
their own license files in their packages, and release builds collect the
runtime notices into a `licenses/` directory beside the executable.

## Adapted source and instruction text (Apache License 2.0)

Each directory holds the Apache License 2.0 text and a NOTICE that names the
original work, its copyright and the modifications made here.

| Directory | Material |
|---|---|
| `crates/core/src/context/prompts/` | Instruction policy text, including the memory policy templates in `crates/core/src/memory/prompts/` |
| `crates/core/src/goal/templates/` | Goal instruction templates and the goal-tool policy text they reference |
| `crates/core/src/planning/prompts/` | Collaboration instruction templates |
| `crates/core/src/engine/approval_context/assets/` | Automatic review policy assets and context assembly |
| `crates/core/src/engine/approval_review_support/` | Review execution support |
| `crates/core/src/provider/approval_classifier_transport/` | Classifier transport |
| `crates/core/src/sandbox/linux/` | Linux sandbox policy and process setup (`LICENSE-APACHE`) |
| `crates/sandbox-win/` | Windows sandbox (`LICENSE-APACHE`; the crate's own integration code is MIT, `LICENSE-MIT`) |
| `crates/sandbox-win/src/elevated/pty_native/` | Windows pseudo-terminal runtime |

Several of these NOTICE files also record code derived from Ratatui (MIT,
Copyright (c) 2016-2022 Florian Dehau, Copyright (c) 2023-2025 The Ratatui
Developers).

## Vendored components

| Location | Component | License | License files |
|---|---|---|---|
| `crates/sandbox-linux-bwrap/vendor/bubblewrap/` | bubblewrap 0.11.2, Copyright (C) 2016 Red Hat, Inc. and the bubblewrap contributors | LGPL-2.0-or-later | `COPYING`, `LICENSE`; integration changes in `crates/sandbox-linux-bwrap/NOTICE` |
| `crates/sandbox-linux-bwrap/` (integration, build script, capability adapter) | Klaus integration | MIT / Apache-2.0 | `LICENSE-MIT`, `LICENSE-APACHE` |
| `crates/sandbox-win/src/mxc/` | Microsoft MXC SDK, Copyright (c) Microsoft Corporation | MIT | `LICENSE-MIT` |
| `crates/core/src/engine/code_mode/runtime_impl/vendor/` | acorn, Copyright (C) 2012-2022 by various contributors | MIT | `LICENSE-acorn` |
| `crates/ripgrep-*/` | ripgrep executables from `@vscode/ripgrep-<target>` 1.18.0; ripgrep Copyright (c) 2015 Andrew Gallant, packaging Copyright (c) Microsoft Corporation | MIT | `LICENSE-RIPGREP-MIT`, `LICENSE-MICROSOFT` in each crate |
| `crates/icu-data/` | ICU 77.1 common data, Copyright © 2016-2025 Unicode, Inc. | Unicode-3.0 (data), MIT (crate) | `ICU-LICENSE.txt`, `LICENSE-MIT` |

## Trademarks

Product and vendor names that appear in this repository are trademarks of their
respective owners. They identify the corresponding product or service and
indicate integration compatibility only; their use does not imply endorsement,
and the MIT license granted here conveys no right to use those marks.
