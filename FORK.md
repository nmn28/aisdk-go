# aisdk-go Fork

**Upstream**: [coder/aisdk-go](https://github.com/coder/aisdk-go)
**Fork**: [nmn28/aisdk-go](https://github.com/nmn28/aisdk-go)

## Fork Delta (10 commits as of 2026-08-05)

| Commit | Change | Upstreamable? |
|--------|--------|---------------|
| 730facd | v6 UI Message Stream protocol support | No (Endure-specific wire format) |
| 79842c1 | data- prefix for custom endure-title event | No |
| 155089c | Strip finishReason/usage from finish-step | Maybe |
| aa93fb9 | Anthropic native web search server tool | Yes |
| 18a5116 | UIStatusPart for transient status events | No (Endure UI) |
| 0fc8fd7 | Fix WithAccumulator yield return value | Yes (bug fix) |
| 96f5e2b | Anthropic prompt caching (cache_control) | Yes |
| 9030719 | Fix empty tool name from ServerToolUseBlock | Yes (bug fix) |
| d073533 | Fix UITitlePart wire format for strictObject | No (Endure UI) |
| c7bd438 | Expose cache_read/cache_creation tokens in Usage | Yes |

## Sync Protocol

1. `git fetch upstream`
2. `git merge upstream/main` (or rebase if clean)
3. Resolve conflicts in Endure-specific files
4. Update endureapigo go.mod to new commit hash
5. Verify `go build ./...` in endureapigo

## Consumer

- `endureapigo/go.mod` pins to commit hash (pseudo-version), not branch
- Current pin: `v0.0.0-20260805235431-c7bd4383d9c1`
