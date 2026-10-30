# File Uploads

Do not click upload buttons or file inputs when the goal is to set a file.
Clicking opens a native picker that the browser runtime may not be able to see.
Use `browser_file_upload` or `tab.fileUpload(paths, { selector | ref })`.

Resolve a file input with `browser_read_page` or `browser_find`, then pass the
returned ref or a stable selector. The Playwright backend calls
`locator.setInputFiles`; the Chrome extension backend uses Chrome debugger
protocol `DOM.setFileInputFiles`.

Only upload files that the user provided or that the current task created. The
sidecar enforces `KLAUS_BROWSER_MAX_UPLOAD_BYTES` with a default of 10 MB.
