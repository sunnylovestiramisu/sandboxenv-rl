#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Fails when the Go client (clients/go, outside its tests) depends on a
# package of this repository other than proto/ateenv/ (internal/ included),
# on Substrate's modules (go.ate.dev/...) or on Kubernetes (k8s.io/...). An
# external module that imports the client must not pull the control plane's
# dependencies; outside tests the client imports only proto/ateenv/v1alpha
# from this repository (AGENTS.md, "Architecture invariants"). `go list -deps`
# ignores test files, so the tests keep their fixture's imports. Subpackages
# of clients/go are allowed and checked the same way.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# go list runs on its own, so that a failure of it (an import that does not
# resolve, a build error) fails this check instead of reading as no output.
deps="$(go list -deps ./clients/go/...)"

repo='github.com/agent-substrate/env'
bad="$(grep -E '^(github\.com/agent-substrate/env/|go\.ate\.dev/|k8s\.io/)' <<<"$deps" |
  grep -v -E "^${repo//./\\.}/(proto/ateenv/|clients/go(/|$))" || true)"
if [[ -n "$bad" ]]; then
  echo "clients/go depends on packages it must not import (outside tests):" >&2
  echo "  ${bad//$'\n'/$'\n'  }" >&2
  echo "Find the import chain with: go list -deps -f '{{.ImportPath}}: {{join .Imports \" \"}}' ./clients/go/..." >&2
  exit 1
fi
echo "clients/go: no dependency on this repository outside proto/ateenv/, or on go.ate.dev/ or k8s.io/"
