# aisdk-go Fork — Provenance & Delta

## Provenance

- **Upstream**: [coder/aisdk-go](https://github.com/coder/aisdk-go)
- **Fork**: [nmn28/aisdk-go](https://github.com/nmn28/aisdk-go)
- **Upstream base**: `c099d9f` (latest upstream as of fork date 2026-03-20)
- **License**: MIT — Copyright (c) 2025 Kyle Carberry (see `THIRD_PARTY_NOTICES.md`)
- **Fork created**: 2026-03-20, commit `730facd`

## Fork Delta (10 commits as of 2026-08-05)

| Commit | Change | Merge conflict risk |
|--------|--------|---------------------|
| 730facd | v6 UI Message Stream protocol — new `uistream.go` file | Low (new file) |
| 79842c1 | data- prefix for custom endure-title event | Low (new file) |
| 155089c | Strip finishReason/usage from finish-step wire output | Low (only `uistream.go`) |
| aa93fb9 | Anthropic native web search server tool | **Medium** (touches `anthropic.go`) |
| 18a5116 | UIStatusPart for transient status events | Low (new type in `uistream.go`) |
| 0fc8fd7 | Fix WithAccumulator yield return value | **Medium** (touches `stream.go`) |
| 96f5e2b | Anthropic prompt caching (cache_control) | **High** (touches `anthropic.go` tool construction) |
| 9030719 | Fix empty tool name from ServerToolUseBlock | **Medium** (touches `anthropic.go`) |
| d073533 | Fix UITitlePart wire format for strictObject | Low (only `uistream.go`) |
| c7bd438 | Expose cache_read/cache_creation tokens in Usage | **Medium** (touches `stream.go` + `anthropic.go`) |

### 155089c safety note

This commit strips `Usage` from `UIFinishStepPart.UIStreamJSON()` (client-facing SSE wire format).
It does NOT affect the metering path: `anthropic.go` reads cache tokens from `MessageStartEvent` into
`finalUsage`, which flows through `FinishStepStreamPart` → data stream accumulator → `acc.Usage()`.
The handler reads the accumulator, not the UI stream. Verified 2026-08-05.

## Sync Protocol

1. `git fetch upstream`
2. Check: `git log upstream/main --oneline | head -10` — review what changed
3. `git merge upstream/main` — resolve conflicts (likely in `anthropic.go`)
4. Run tests: `go test ./...`
5. Update `endureapigo/go.mod`: `go get github.com/nmn28/aisdk-go@<new-commit>`
6. Verify: `go build ./...` in endureapigo

## Consumer

- `endureapigo/go.mod` pins to commit hash (pseudo-version), not branch
- Current pin: `v0.0.0-20260805235431-c7bd4383d9c1`
- `proxy.golang.org` caches this module, so builds don't depend on GitHub availability
