# Test coverage reporting

This document describes the test coverage reporting infrastructure for `sandboxenv-rl`, including how coverage is collected across Go and Python test suites, how reports are generated, and how to inspect coverage locally and in CI.

## Overview

The test coverage workflow provides unified visibility into Go and Python test suites without imposing failing coverage gates or arbitrary percentage thresholds. Reports are generated for both overall statement coverage and changed-line coverage ("diff coverage") against the PR base branch (`origin/main` or `$GITHUB_BASE_REF`). Summaries include Mermaid horizontal bar charts (`xychart-beta horizontal`) with each package's coverage in its label.

### Coverage Scope

| Component | Test Suite | Tooling | Output Artifacts |
|---|---|---|---|
| **Go packages** (`clients/go`, `cmd/*`, `guest/*`, `internal/*`) | Unit & integration tests (`ci-go`) | `go test -race -covermode=atomic -coverpkg=./...` | `coverage/go.filtered.out`, `coverage/go.html`, `coverage/go.xml` |
| **Guest Daemon** (`examples/guest-daemon`) | Guest daemon integration suites (`ci-go`, `ci-python`) | `go build -cover`, executed against `TestE2EGuestDaemon` (`ci-go`) and `test_guest_daemon.py` (`ci-python`), counters flushed on `SIGTERM` | `coverage/go-unit/` (`ci-go`) and `coverage/go-daemon/` (`ci-python`), merged via `go tool covdata` |
| **Python client & integrations** (`clients/python`, `integrations/nemo-gym`, `integrations/uni-agent`, `examples/Qwen3-1.7B-verl`, `benchmarking/*`, `internal/benchmarking/swebench/trajectories`) | Parallel pytest suites (`ci-python`) | `coverage run -m pytest`, combined via [`.coveragerc`](../../.coveragerc) | `coverage/py.xml`, `coverage/py.json`, `coverage/py-html/` |
| **Diff coverage** | Changed lines vs. base branch | `diff-cover` over combined Cobertura XMLs | `coverage/diff-cover.md`, included in `coverage/summary.md` |

> [!NOTE]
> Generated protobuf stubs (`*.pb.go`, `_gen/`) are excluded from coverage calculations so totals reflect authored application and library code.

---

## Running Coverage Locally

### 1. Unified Go & Python Coverage

To run both Go and Python test suites with coverage and generate HTML reports:

```bash
make coverage
```

This runs `ci-go` and `ci-python`, merges Go coverage data across unit tests and the guest-daemon integration suite, combines Python coverage profiles across all subprojects, executes `diff-cover` against `origin/main`, and prints the paths of the resulting HTML reports:

```text
Generated Coverage Reports:
  Go HTML:      /path/to/coverage/go.html
  Python HTML:  /path/to/coverage/py-html/index.html
  Summary MD:   /path/to/coverage/summary.md
```

> [!NOTE]
> When running under the Docker runner (`CI_RUNNER=docker make coverage`), diff-cover is skipped because the Docker image builds from a standalone baseline commit without git history.

### 2. Component Suites

You can also run individual suites with coverage:

```bash
# Run Go test suite with coverage, vet, format, proto checks, and summary
make ci-go

# Run Python lint, parallel pytest suites with coverage, and summary
make ci-python
```

Both targets update the corresponding files in the `coverage/` directory and print a formatted Markdown summary and Mermaid chart to stdout.

---

## Coverage Artifacts

All coverage output files are written to the gitignored `coverage/` directory at the repository root:

| Path | Description |
|---|---|
| `coverage/summary.md` | Markdown summary report with statement counts, Mermaid horizontal bar charts, and diff-cover results |
| `coverage/summary-go.md`, `coverage/summary-python.md` | Single-language Markdown summaries written by `ci-go` and `ci-python` |
| `coverage/go.html` | Interactive Go line-by-line coverage viewer (`go tool cover -html`) |
| `coverage/go.filtered.out` | Go coverage profile excluding generated protobuf stubs |
| `coverage/go.xml` | Go Cobertura XML format generated via `gocover-cobertura` |
| `coverage/go-func.txt` | Per-function Go coverage profile summary (`go tool cover -func`) |
| `coverage/py-html/index.html` | Interactive Python HTML coverage report |
| `coverage/py.xml` | Python Cobertura XML report |
| `coverage/py.json` | Machine-readable Python coverage data |
| `coverage/diff-cover.md` | Line-by-line coverage of changed files against the base git branch |
| `coverage/diff-cover-go.md`, `coverage/diff-cover-python.md` | Single-language diff-coverage reports written by `ci-go` and `ci-python` |

To view the interactive HTML reports in your web browser:

```bash
# View Go coverage
xdg-open coverage/go.html

# View Python coverage
xdg-open coverage/py-html/index.html
```

---

## Continuous Integration (CI)

In GitHub Actions (`.github/workflows/ci.yaml`):

1. **Full git history (`fetch-depth: 0`):** The `go` and `python` jobs checkout the repository with `fetch-depth: 0` so `diff-cover` can compare against the target branch (`$GITHUB_BASE_REF` or `origin/main`).
2. **Step summary & PR annotations:** `hack/ci/coverage-summary.py` detects `$GITHUB_STEP_SUMMARY` and appends the Markdown summary directly to the GitHub Actions job run summary. When running under GitHub Actions with diff coverage, it also emits `::notice` workflow annotations for uncovered changed lines directly into the PR diff view (up to 10 largest ranges).
3. **Artifact uploads:** Both `go` and `python` jobs upload the `coverage/` directory as workflow artifacts (`coverage-go` and `coverage-python`) using `actions/upload-artifact` with `if: always()`, ensuring reports are downloadable even if a test fails.

---

## Architecture & Implementation Notes

### Go Coverage & Covdata Merging
Go tests are run with `-coverpkg=./...` to record cross-package coverage during unit tests and `TestE2EGuestDaemon` (`ci-go`). In addition, the guest daemon (`examples/guest-daemon`) is built with `go build -cover` and executed against `TestE2EGuestDaemon` in `ci-go` (writing counters to `coverage/go-unit/`) and `test_guest_daemon.py` in `ci-python` (writing counters to `coverage/go-daemon/`). When the daemon receives `SIGTERM`, Go's runtime coverage counters are flushed to disk.

The script `hack/ci/coverage-summary.py` formats `coverage/go-unit/` (which in `make ci-go` and the GitHub Actions `go` job already combines unit tests and `TestE2EGuestDaemon`), and when `coverage/go-daemon/` is also present (`make ci` and `make coverage`, after `ci-python`) merges both directories with `go tool covdata merge`, converts them to a unified `go.out`, filters out protobuf stubs, and generates Cobertura XML via `gocover-cobertura`. In GitHub Actions, the `go` and `python` jobs run on separate machines and each reports its own language (the `go` job includes `TestE2EGuestDaemon` daemon counters, and the `python` job's artifact also contains `coverage/go-daemon/` from `test_guest_daemon.py`).

### Python Coverage Path Mapping
[`.coveragerc`](../../.coveragerc) at the repository root configures `coverage.py` with:
- `branch = true` under `[run]` for branch coverage measurement.
- `[paths]` entries mapping `clients/python/src/ate_env_rl` and the integration packages so coverage data recorded from different working directories (`clients/python`, `integrations/nemo-gym`, `integrations/uni-agent`) merge into canonical repository-relative paths.
- Unique `COVERAGE_FILE` paths per parallel pytest suite under `coverage/py/`, combined by `coverage combine`.

### Guest Daemon Graceful Stop
The guest daemon handles `SIGINT` and `SIGTERM` signals gracefully to ensure Go coverage runtime data is flushed to disk before process exit. When running under `ci-go` and `ci-python`, each daemon process is supervised and automatically terminated and cleaned up via bash traps upon completion or unexpected failure.
