# Tab Cleanup

Before ending browser work, close tabs that were created only for inspection or
intermediate steps. Use `browser_tabs` with `finalize` or `tab.close()` when the
tab is no longer useful.

Use `browser_mark_tab` / `tab.markDeliverable()` for tabs the user should keep
as an output. Use `tab.markHandoff()` for tabs that should stay available for a
later turn. Treat finalize/cleanup as the last browser action unless new browser
work is required.
