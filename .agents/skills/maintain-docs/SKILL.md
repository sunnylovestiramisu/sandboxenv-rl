---
name: maintain-docs
description: >-
  Guide to authoring, updating, and validating architecture, glossary, developer guides, and
  AGENTS.md files in sandboxenv-rl against codebase ground truth.
---

# Maintaining and Vetting sandboxenv-rl Documentation

Use this procedure when authoring, expanding, or auditing the documentation suite in `sandboxenv-rl`
(`AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, `docs/architecture.md`, `docs/glossary.md`,
`docs/dev/code-layout.md`, `docs/dev/concurrency-and-pitfalls.md`, nested `README.md` files, or
`.agents/skills/`).

## 1. Single source of truth and document map

Every fact lives in **one** canonical file; other documents link to it using relative Markdown links
(`./` or `../`):

| File                                   |      Target size      | Canonical responsibility                                                                                                                                                                                                                                                                                                                                                            | Link to it when…                                                                                                |
| -------------------------------------- | :-------------------: | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `AGENTS.md`                            |     80–130 lines      | Behavior contract, anti-reinvention rules, architecture invariants with enforcement clauses, build/test commands, PR checklist.                                                                                                                                                                                                                                                     | Referencing contributor rules, invariant sign-offs, or entry-point commands.                                    |
| `CLAUDE.md`                            |        1 line         | Contains `@AGENTS.md` so tools expecting `CLAUDE.md` load `AGENTS.md`.                                                                                                                                                                                                                                                                                                              | Never link directly; maintain as single import line.                                                            |
| `GEMINI.md`                            |        1 line         | Contains `@./AGENTS.md` so Gemini CLI, which reads `GEMINI.md` by default, loads `AGENTS.md`.                                                                                                                                                                                                                                                                                       | Never link directly; maintain as single import line.                                                            |
| `docs/architecture.md`                 |     150–350 lines     | System design, component responsibilities (owns/calls/must not), `CreateEnvironment` and guest request flows, wake budgets, state persistence, trust boundaries, and design decisions.                                                                                                                                                                                              | Explaining _how_ pieces fit together, _why_ an architectural choice was made, or request sequences.             |
| `docs/glossary.md`                     | 1–2 sentences / entry | Precise definitions for API resources, Substrate primitives, components, runtime states, and telemetry terms. Structured with nested lists and explicit disambiguations (what a term is _not_).                                                                                                                                                                                     | Defining a term or resolving ambiguity between related concepts (`Env` vs. `SandboxHandle`, `TTFI` vs. `TTFE`). |
| `docs/dev/code-layout.md`              |     60–120 lines      | Annotated directory tree, answer-first package placement decision rules, test double locations, and the new RPC checklist.                                                                                                                                                                                                                                                          | Deciding where new code belongs or finding existing test doubles.                                               |
| `docs/dev/concurrency-and-pitfalls.md` |    ~120–150 lines     | Operational invariants and pitfalls ("what breaks if you change it"): PID 1 subreaper zombie reaping, negative-PID signal delivery, spool cursor ordering, router mTLS and header deduplication, wake/delete ordering, HTTP/2 stream concurrency, and Python `LoopRunner` isolation.                                                                                                | Explaining race conditions, signal traps, or lifecycle ordering constraints.                                    |
| Generated code `README.md`             |      3–15 lines       | Placed beside generated code directories (`proto/ateenv/v1alpha/README.md`, `clients/python/src/ate_env_rl/_gen/README.md`) with a strict "never hand-edit" rule and regeneration instructions.                                                                                                                                                                                     | Reminding contributors and agents how to regenerate stubs.                                                      |
| Directory inventory `README.md`        |      25–50 lines      | Placed in multi-component directories (`clients/README.md`, `integrations/README.md`, `examples/README.md`, `benchmarking/README.md`) to index subpackages, integrations, examples, or benchmark entry points and link each item's own `README.md`. Never list individual `.go` or `.py` source files. Leave file-level detail to package docstrings and `docs/dev/code-layout.md`. | Orienting readers to what lives under `clients/`, `integrations/`, `examples/`, or `benchmarking/`.             |

### Recommended authoring and update order

When updating documentation across multiple files, proceed in dependency order:

1. `docs/glossary.md`: define any new terms or distinguish new concepts.
2. `docs/architecture.md`: update system components, diagrams, and request sequences.
3. `docs/dev/code-layout.md`: update package placements or checklists.
4. `docs/dev/concurrency-and-pitfalls.md`: document any new invariants, races, or traps.
5. `AGENTS.md`: distill the top-level invariants, layout updates, and verification commands.
6. Directory inventory `README.md` files (`clients/README.md`, `integrations/README.md`,
   `examples/README.md`, `benchmarking/README.md`) and `README.md`: update the directory index and
   the top-level path table when adding or removing a client, integration, example, or benchmark
   entry point.

## 2. Hard rules for documentation

1. **The code wins over everything**: Every claim must hold in Go, Python, and protobuf source at
   the current commit SHA. Never document an aspirational design as if it exists today. If a feature
   or refactoring is planned, prefix it with `Planned:` or omit it. Keep the
   `Status: describes main at <sha>` line updated in `docs/architecture.md`.
2. **Real identifiers only**: Spell package paths, binary names, RPC names, CLI flags, and constants
   exactly as they appear in code (`-atenet-ca-file`, `-atenet-client-cert`,
   `apiservice.(*Server).guestConn`, `DefaultMaxLogBytes`, `ENVIRONMENT_STATUS_UNSPECIFIED`). Never
   invent shorthand or coin names.
3. **Canonical invariant syntax**: In `AGENTS.md`, format architecture invariants strictly as:
   `<pkg> owns X; <other> calls it and must not re-implement it; because Y (enforced by <symbol> | not enforced).`
   Every invariant must name its enforcer or state `not enforced`.
4. **Cite symbols over line numbers**: Line numbers go stale quickly across commits. Cite symbols
   (`apiservice.(*Server).ensureImageTemplate`, `guest/process.(*Tracker).Signal`), not line ranges.
5. **Name constants over magic numbers**: Name the code constant (`DefaultWakeBudget`,
   `DefaultWakeRunningBudget`, `DefaultReadBufferSize`, `DefaultMaxLogBytes`). Mention numbers only
   when explaining an intentional design divergence (e.g., `DefaultMaxConcurrentStreams = 4096` vs.
   Go's default of 250).
6. **Prose and formatting conventions**:
   - Write short active sentences with the subject first ("`ate-env-api` creates the actor", not
     "the actor is created").
   - Prefer active verbs to abstract nouns ("evicts", not "performs eviction").
   - Wrap every identifier, path, flag, and RPC in backticks.
   - Use relative links with leading `./` or `../` prefixes so navigation works in web renderers and
     agent tools.
   - Do not use `---` horizontal rules before section headings.
   - Do not use em-dashes (`—`), chains of semicolons, or contrast pairs ("X, not Y") as flourishes.
   - Do not use bold in running prose (reserve bold for term definitions and table keys).
   - Ban filler words: _load-bearing, footgun, surfaced, robust, comprehensive, seamless, leverage_.
   - Format Markdown with Prettier (`make format-docs`).

## 3. Where to validate claims in the codebase

Do not rely on memory, design notes, or comments when documenting runtime behavior. Check the
authoritative code entry points:

- **CLI flags and default values**: Inspect `cmd/ate-env-api/main.go`, `cmd/ate-env-guest/main.go`,
  and `cmd/ate-env/manifest.go` directly. Flags frequently diverge from common conventions (e.g.,
  single dash `-atenet-ca-file`, `-listen` default `:80`, `-workspace` default `"/"`).
- **Wire contracts and fields**: Check `proto/ateenv/v1alpha/env.proto` and `guest.proto`. Ensure
  messages, RPC names, and field types exist in the proto definitions before mentioning them in
  docs.
- **Server routing, wake retries, and template derivation**: Inspect
  `internal/apiservice/server.go`, `internal/apiservice/wakeretry.go`, and
  `internal/apiservice/imagetemplate.go`. Verify the exact call sequence (e.g., checking derived
  templates before base templates) and how `ensureAwake` is executed.
- **Substrate client and router communication**: Inspect `internal/ate/client.go` (Substrate API
  calls, suspend durability modes, delete sequencing, pooled router connections),
  `internal/routertls/routertls.go` (pod identity certificate and CA bundle reloading),
  `internal/portproxy/portproxy.go` (`CONNECT` tunnel dialing and wake loop), and
  `internal/mcp/server.go` (MCP tool mapping to `DialGuest`).
- **In-actor daemon, PID 1 init, and processes**: Inspect `cmd/ate-env-guest/init_linux.go`
  (`runIfPID1`, `supervise`), `cmd/ate-env-guest/sidecar.go` (readiness URL probing on `/readyz`),
  `guest/process/tracker.go` (process groups, PTY allocation, spooling, signal delivery), and
  `guest/filesystem/service.go` (path confinement and read chunk buffers).
- **Python SDK and concurrency**: Inspect `clients/python/src/ate_env_rl/loop_runner.py`,
  `handle.py`, `client.py`, `fleet.py`, and `backend/substrate.py`. Check how coroutines are
  dispatched from synchronous methods and how gRPC channels are managed.
- **Test doubles**: Inspect `internal/internaltest/` (`fakecontrol`, `fakerouter`, `testcerts`),
  `clients/python/src/ate_env_rl/backend/mock.py`, and `clients/python/tests/fakes.py` before
  documenting testing recommendations.
- **Import boundaries**: Run `go list -f '{{.Imports}}' ./...` to verify package import graphs
  before asserting an architectural boundary.

## 4. Inspiration callouts from upstream repositories

When adding new documentation sections or developer guides, consult specific patterns from the two
repositories that inspired this structure:

### 1. `https://github.com/agent-substrate/substrate`

- **`AGENTS.md`**:
  - _Rules for contributors_: Model for human submission requirements, diff self-review, and
    conciseness rules.
  - _Code_: Anti-reinvention core ("Search the repo and its dependencies before writing a helper,
    cache, client, flag set or test fixture. Reuse or extend what exists...").
  - _Commit Messages_: Standalone rationale without headings, markdown lists, or PR links.
  - _Testing Instructions_: Fail-closed CI predicates (named predicates instead of inline
    `os.Getenv("CI")`).
  - _Security Considerations_: Model for stating workload isolation, mTLS, and control-plane auth
    invariants directly.
- **`docs/architecture.md`**:
  - Model for problem framing (bursty, idle agent workloads multiplexed onto warm workers),
    component breakdowns (owns / calls / must not), and design decisions.
  - _Divergence rule_: Substrate opens with "Much of this architecture is aspirational"; in
    `sandboxenv-rl`, all docs must pin to verified code at `HEAD`.
- **`docs/glossary.md`**:
  - Model for grouping terms by kind (_Resources_, _Records_, _Components_, _Lifecycle_), defining
    each term in 1–2 sentences, and explicitly stating what a term is _not_.
- **`docs/dev/code-layout.md`**:
  - Model for the annotated directory tree and answer-first decision rules (`pkg/` vs. `internal/`,
    `cmd/<binary>/`, placement checklist).
- **`.agents/skills/agents-md/SKILL.md`**:
  - Model for progressive disclosure (keeping root files short, ~100–150 lines), decision tables,
    and concrete "Don't do X, do Y instead" rules.

### 2. `https://github.com/kubernetes-sigs/agent-sandbox`

- **`AGENTS.md` (_Repository layout_ table)**:
  - Model for explicit table formatting that marks each path as **generated** or hand-written.
  - Model for highlighting package naming triads (directory name vs. PyPI distribution name vs.
    import module name, e.g. `clients/python` vs. `ate-env-rl` vs. `ate_env_rl`).
- **`AGENTS.md` (_Agent Skills_ section)**:
  - Model for indexing specialized instructions in `.agents/skills/` and importing from `CLAUDE.md`.
- **`AGENTS.md` (_Coding conventions_ & _Writing_ section)**:
  - Distillation of Google Technical Writing principles: active voice, one idea per sentence,
    defining terms before use, and matching comment weight to code weight.
  - Concrete before/after examples demonstrating how to eliminate passive voice, long clause chains,
    and filler words.
