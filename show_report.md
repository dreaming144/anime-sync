## Show report

**3** change(s) across **3** show(s) · error: 2 · dates: 1

### Errors

- By type: `FakeErr` ×1, `TimeoutError` ×1
- By platform: `mal` ×1, `anilist` ×1

| Show | Platform | Error |
|------|----------|-------|
| Rate Limited Show | `mal` | FakeErr: too many requests / HTTP 429 / {"error":"rate limit exceeded"} |
| Slow Show | `anilist` | TimeoutError: read timed out |

| Show | Platform | Action | Status | Progress | Score | Detail |
|------|----------|--------|--------|----------|-------|--------|
| Rate Limited Show | `mal` | **error** | completed |  |  | FakeErr: too many requests / HTTP 429 / {"error":"rate limit |
| Slow Show | `anilist` | **error** | watching |  |  | TimeoutError: read timed out |
| Fine Show | `simkl` | **dates** | completed |  |  | ok |

