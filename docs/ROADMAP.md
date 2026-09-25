# Roadmap

Goal for 0.1: all 14 [use cases](use-cases.md) pass as integration tests, and
a 10-minute plugin really takes 10 minutes.

| Milestone | Delivers | Done when |
|---|---|---|
| M1 Relay kernel | ClientRequest, streams, splice relay, commit, Log, Format trait, OpenAI chat format, HTTP adapter, memory store, YAML config, `gw check` | OpenAI streaming relays with usage in the Log; overhead benchmarked |
| M2 Plugin API | Plugin trait end to end, map/filter, subrequests, atomic store batches, naming and API fixes before the API freezes | The three 10-minute plugins exist as tests and docs |
| M3 Formats | Anthropic, Gemini, Responses, embeddings, YAML generic; relay with correct usage | Golden-stream fixtures pass per format |
| M4 SDK and testkit | Fallback, hedge, limits, memo, ledger; mock provider, golden streams | Use cases 4, 9, 10 pass |
| M5 Translation | Chat events; OpenAI, Anthropic, Gemini translated both ways, streams included | A client in one format reaches a provider in another |
| M6 Hard cases | Sessions, Realtime relay then OpenAI Realtime to Gemini Live, jobs, required coverage, PII rewriting, admission | All 14 use cases pass |
| M7 0.1 release | Docs, examples, benchmarks, Redis store, public repo | Release decision |
| M8 MCP gateway | `mcp` format, single-server relay, aggregation with tool namespacing, per-key access control | Claude Code works through one `/mcp` over several servers |
| M9 Agent loop | Gateway-run MCP tools inside chat and Responses requests, as an SDK dispatch plugin | A non-OpenAI model uses MCP tools through the gateway |

Rules:

- Each use case becomes an integration test when its milestone lands.
- A change to a kernel concept needs a decision record.
- Kernel API changes are cheap until M2 ends; after that, treat it as frozen.
