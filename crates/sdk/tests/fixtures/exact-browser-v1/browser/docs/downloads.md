# Downloads

Use `browser_download` or `tab.downloadMedia` for linked files, images, media,
and download links. Prefer a selector or direct URL; coordinates are a fallback
when a visible media element has no stable selector.

The Playwright backend captures real browser download events and saves files to
the artifact directory. If a selector resolves to an href/src URL, it can also
download that URL directly. The Chrome extension backend uses Chrome downloads
and returns the Chrome download id.

Do not run downloaded software through browser automation without explicit user
approval.
