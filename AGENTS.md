# WhatsApp MCP coding guidance

This is a standalone repository. Run build, deployment, and test commands from
this root; the Docker build context is `.` and Railway uses `Dockerfile` here.

For installation work, read `docs/INSTALLATION.md` and `RAILWAY.md`. Each
installation has its own account, `.access` files, session, and runtime volume.
Do not infer credentials or deployment IDs from another installation. Verify
actual authenticated MCP calls through the intended client and report history
gaps before declaring installation complete.

Apply clarity improvements whenever changing this project. Preserve existing
behavior, tool signatures, defaults, response envelopes, errors, ordering, access
checks, and deployment settings. Keep unrelated fixes separate from refactoring.

Documentation and comments describe the current contracts, configuration and
operation. Explain saved sessions and checkpoints when needed to operate them;
keep iteration history, before/after narratives and change notes in Git or PRs.

Prefer explicit control flow and descriptive names. Avoid nested conditional
expressions, dense one-liners, and comments that repeat the next statement. Keep
comments that explain constraints, security boundaries, or protocol details.
Extract helpers for repeated concrete operations; avoid generic frameworks or
abstractions used only to shorten code.

The Go bridge is one package, organized into `main.go` for startup, `store.go` for
SQLite storage, `sync.go` for WhatsApp synchronization, `media.go` for media, and
`api.go` for the internal HTTP API. Build or run the package with `go build .` or
`go run .`; the Dockerfile must copy all runtime Go source files.

Python database reads share connection cleanup and row conversion in
`whatsapp.py`. Keep diagnostics' recoverable database errors separate from query
errors. Authentication belongs in `access_control.py`, before tool execution.
FastMCP publishes tool docstrings verbatim. Formatting skips prevent automated
reformatting of those public descriptions.

From this directory, run checks relevant to the change:

```sh
python -m unittest discover -s tests -v
python -m unittest discover -s whatsapp-mcp-server/tests -v
whatsapp-mcp-server/.venv/bin/python tests/verify_access.py
whatsapp-mcp-server/.venv/bin/python tests/verify_sdk.py
whatsapp-mcp-server/.venv/bin/python tests/verify_retrieval_sdk.py
whatsapp-mcp-server/.venv/bin/python tests/verify_sending_sdk.py
(cd whatsapp-bridge && go test ./... && go vet ./...)
bash -n deploy/entrypoint.sh
git diff --check
```

Use Ruff's formatter and import checks for Python and `gofmt` for Go. Validate the
Docker build when changing its source layout or build steps. Use temporary
fixtures and mocked network calls for tests; never put production data or keys
in source, test fixtures, or build contexts.

