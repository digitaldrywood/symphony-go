# Pre-turn instance failure validation

## Failure taxonomy

Infrastructure failure classification requires no started turn, recorded turn,
tokens, or pushed work. Known provider capacity, cancellation, and typed
deliverable failures retain their existing specialized handling.

| Failure | Durable error class |
| --- | --- |
| Backend startup deadline, including merge startup | `backend_startup_timeout` |
| Backend startup handshake/process exit | `backend_startup_failure` |
| Workspace preparation, including a nonzero `after_create` hook | `workspace_preparation` |
| Codex JSON-RPC -32600/-32602 on `thread/start` or `turn/start` | `runner_error` |
| Other runner errors before the first turn | `runner_error` |

The existing instance breaker counts consecutive pre-turn outcomes across issues
and failure classes. Three drain dispatch; the existing blocked-recovery cooldown
admits a canary. A thread ID alone is insufficient recovery evidence; a started
turn clears the drain. The workspace per-issue parking path is removed, and
explicit zero-turn attempt history cannot park an issue. Historical runner
attempts with unknown turn counts retain their prior treatment.

## Browser verification

Rendered the real Go server with a seeded snapshot using a temporary Go test
overlay and `httptest.NewServer` on an ephemeral port. Used the worker's isolated
Chrome DevTools context. The live Detent process was untouched.

- [Health](health.png): one instance row marked **Drained**, naming `runner_error`
  and the Codex protocol error, with its resume time. The summary explains canary
  recovery after cooldown.
- [Board](board.png): three seeded issues remain in **Todo**; **0 blocked**.
- Both temporary preview tests exited successfully after closing the browser.

## Automated validation

- The new cross-issue regression failed on the original implementation.
- `go test ./internal/orchestrator ./internal/web/templates -count=1` passed.
- `make check` is the required final gate; completion evidence is in the issue
  Workpad. Coverage floors in `scripts/coverage-exceptions.txt` are unchanged.
