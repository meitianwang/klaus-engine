# Visibility

Prefer visible, user-reviewable browser actions for state-changing work. Use
screenshots, GIF artifacts, or page trees to verify the page state after
important interactions.

Headless Playwright is useful for tests and extraction. Use the extension
backend when the task depends on a user's logged-in Chrome profile, visible
tabs, existing cookies, browser extensions, or user review of the active page.
