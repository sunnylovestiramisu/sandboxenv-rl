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

.DEFAULT_GOAL := build

GOOGLE_CLOUD_PROJECT ?= $(shell gcloud config get-value project 2>/dev/null)
ATE_ENV_IMAGE_REPO ?= gcr.io/$(GOOGLE_CLOUD_PROJECT)

# Baked into the ate-env CLI by build-cli (release.yaml does the same with the
# published digests): `ate-env --version`, and the defaults of manifest
# --api-image and manifest template --guest-image.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
ATE_ENV_API_IMAGE ?=
ATE_ENV_GUEST_IMAGE ?=
CLI_LDFLAGS := -X main.version=$(VERSION) -X main.defaultAPIImage=$(ATE_ENV_API_IMAGE) -X main.defaultGuestImage=$(ATE_ENV_GUEST_IMAGE)

PRETTIER_VERSION := 3.8.3
PRETTIER ?= npx --yes prettier@$(PRETTIER_VERSION)

.PHONY: build build-cli install test vet clean images python-protos python-test nemo-gym-test verify-boilerplate verify-go-client-deps verify-gofmt verify-protos verify-docs format-docs \
	help ci ci-go ci-python coverage ci-openhands ci-harbor ci-dafny ci-e2e smoke check-substrate-head check-pins update-pins protos lint-actions-locally dev-shell

help: ## Show this help message
	@awk 'BEGIN {FS = ":[^#]*## "} /^[a-zA-Z0-9_-]+:[^#]*## / {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

ci: ## Run all local CI checks (Go and Python suites)
	hack/ci-compat.sh ci

ci-go: ## Run Go build, vet, test (-race, -cover), format, and proto checks
	hack/ci-compat.sh ci-go

ci-python: ## Run Python lint (ruff) and pytest test suites
	hack/ci-compat.sh ci-python

coverage: ## Run Go and Python test suites with coverage and generate reports
	hack/ci-compat.sh ci-coverage

ci-openhands: ## Run OpenHands integration test and lint suite via openhands devShell
	hack/ci-compat.sh --shell .#openhands sh -c 'python -m pytest integrations/openhands/tests -q && ruff check integrations/openhands examples/openhands'

ci-harbor: ## Run Harbor integration test and lint suite via harbor devShell
	hack/ci-compat.sh --shell .#harbor sh -c 'python -m pytest integrations/harbor/tests -q && ruff check integrations/harbor'

ci-dafny: ## Verify the Dafny models in docs/plans/telemetry/verify against their README, via the dafny devShell
	hack/ci-compat.sh --shell .#dafny python3 hack/verify/dafny.py

ci-e2e: ## Run end-to-end tests against local kind cluster
	hack/ci-compat.sh --docker-socket ci-e2e

smoke: ## Run smoke tests against a running cluster
	hack/ci-compat.sh --host-network smoke

check-substrate-head: ## Verify compatibility against substrate@HEAD (or custom REF/DIR via ARGS)
	hack/ci-compat.sh check-substrate-head $(ARGS)

check-pins: ## Verify flake and dependency pin freshness
	hack/ci-compat.sh check-pins

protos: ## Regenerate Go and Python protobuf stubs with the pinned generators (Go plugins in go.mod, grpcio-tools in uv.lock)
	hack/ci-compat.sh sh -c 'make python-protos && go tool buf generate'

update-pins: ## Update flake and dependency pins
	hack/ci-compat.sh update-pins $(ARGS)

lint-actions-locally: ## Lint GitHub Actions workflows locally (zizmor + actionlint)
	hack/ci-compat.sh lint-actions-locally

dev-shell: ## Enter interactive development shell
	hack/ci-compat.sh bash

# -----------------------------------------------------------------------------
# Host-level targets (invoked directly or from within pinned runners)
# -----------------------------------------------------------------------------

build:
	go build ./...

# Build bin/ate-env with the version and, if ATE_ENV_API_IMAGE /
# ATE_ENV_GUEST_IMAGE are set, image defaults baked in.
build-cli:
	go build -trimpath -ldflags "$(CLI_LDFLAGS)" -o bin/ate-env ./cmd/ate-env

# Install ate-env and ate-env-api to $GOBIN (or $GOPATH/bin).
install:
	go install ./cmd/...

test:
	go test ./...

vet:
	go vet ./...

verify-boilerplate:
	hack/verify/boilerplate.sh

# Fail when the Go client depends on internal/, go.ate.dev/ or k8s.io/ packages.
verify-go-client-deps:
	hack/verify/go-client-deps.sh

# Fail when any tracked Go file is not gofmt-formatted; prints the offenders.
verify-gofmt:
	@out=$$(gofmt -l $$(git ls-files '*.go')); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

# Format Markdown documentation files configured in .prettierrc.json.
format-docs:
	$(PRETTIER) --write .

# Fail when any Markdown documentation file is not Prettier-formatted.
verify-docs:
	$(PRETTIER) --check .

# Regenerate the committed Python gRPC stubs (requires grpcio-tools; see
# clients/python/README.md).
python-protos:
	./clients/python/scripts/gen-protos.sh

# Run the Python client tests (requires an environment with the dev extra
# installed: pip install -e 'clients/python[dev]').
python-test:
	cd clients/python && python3 -m pytest

# Run the NeMo Gym provider tests (requires an environment with the client and
# the provider's test extra installed: pip install clients/python -e
# 'integrations/nemo-gym[test]').
nemo-gym-test:
	cd integrations/nemo-gym && python3 -m pytest

# Run the Harbor provider tests (requires an environment with the client and
# the provider's test extra installed: pip install clients/python -e
# 'integrations/harbor[test]'; the trial test also needs a docker daemon).
harbor-test:
	cd integrations/harbor && python3 -m pytest

images:
	@echo "Building and pushing container images to $(ATE_ENV_IMAGE_REPO)..."
	@guest_img=$$(KO_DOCKER_REPO=$(ATE_ENV_IMAGE_REPO)/ate-env-guest ko build --bare ./cmd/ate-env-guest | tail -n 1); \
	api_img=$$(KO_DOCKER_REPO=$(ATE_ENV_IMAGE_REPO)/ate-env-api ko build --bare ./cmd/ate-env-api | tail -n 1); \
	echo "Published:" && \
	echo "  --guest-image $$guest_img" && \
	echo "  --api-image   $$api_img"

clean:
	rm -rf bin

verify-protos:
	hack/verify/protos.sh
