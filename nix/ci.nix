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

{
  pkgs,
  pyproject-nix,
  uv2nix,
  pyproject-build-systems,
}:

let
  # Go comes from go.mod: its `go` directive picks the nixpkgs package with
  # the same major.minor (go 1.27.5 -> pkgs.go_1_27). The patch level is
  # whatever nixpkgs ships; check-pins fails if it is below go.mod's.
  goModVersion =
    let
      lines = pkgs.lib.splitString "\n" (builtins.readFile ../go.mod);
      matches = builtins.filter (m: m != null) (
        map (builtins.match "go ([0-9]+)\\.([0-9]+)(\\.[0-9]+)?[[:space:]]*") lines
      );
    in
    if matches == [ ] then throw "nix/ci.nix: go.mod has no `go` directive" else builtins.head matches;
  goAttr = "go_${builtins.elemAt goModVersion 0}_${builtins.elemAt goModVersion 1}";
  goToolchain =
    pkgs.${goAttr} or (throw ''
      nix/ci.nix: go.mod asks for Go ${builtins.elemAt goModVersion 0}.${builtins.elemAt goModVersion 1}, but the locked nixpkgs has no pkgs.${goAttr}.
      Lower the `go` directive in go.mod to a Go version the locked nixpkgs packages, or wait until nixpkgs packages Go ${builtins.elemAt goModVersion 0}.${builtins.elemAt goModVersion 1}.
      If a newer nixpkgs already has it, run `make update-pins ARGS=nixpkgs` (or `nix flake update nixpkgs`) to move the lock forward.
    '');

  pythonEnvs = import ./python.nix {
    inherit
      pkgs
      pyproject-nix
      uv2nix
      pyproject-build-systems
      ;
  };
  inherit (pythonEnvs)
    pythonEnvFull
    pythonEnvBase
    openhandsEnv
    uniAgentEnv
    harborEnv
    ;

  diffCover = pkgs.runCommand "diff-cover" { } ''
    mkdir -p $out/bin
    ln -s ${
      pythonEnvs.python.pkgs.diff-cover.overridePythonAttrs (_: {
        doCheck = false;
      })
    }/bin/diff-cover $out/bin/diff-cover
  '';

  pythonReleaseTools =
    let
      env = pythonEnvs.python.withPackages (ps: [
        ps.build
        ps.setuptools
        ps.setuptools-scm
        ps.wheel
        ps.twine
      ]);
    in
    pkgs.runCommand "python-release-tools" { } ''
      mkdir -p $out/bin
      ln -s ${env}/bin/pyproject-build $out/bin/pyproject-build
      ln -s ${env}/bin/twine $out/bin/twine
    '';

  ciPackages = with pkgs; [
    # Go toolchain selected from go.mod's `go` directive (see goToolchain)
    goToolchain
    golangci-lint

    # Container, image, signing, and Kubernetes tooling
    ko
    crane
    kind
    kubectl
    docker-client

    # Python, built from uv.lock (nix/python.nix); ruff and pytest come from
    # the client's dev extra. uv to update the lock.
    pythonEnvFull
    uv

    # Prettier for Markdown documentation formatting
    prettier

    # Utilities, build tools, and CI scripting
    stdenv.cc
    cacert
    coreutils
    gnused
    gawk
    findutils
    gnugrep
    curl
    jq
    flake-checker
    shellcheck
    actionlint
    zizmor
    git
    gnumake

    # Coverage and diff-coverage tooling
    gocover-cobertura
    diffCover

    # Python release tools
    pythonReleaseTools
  ];

  devTools = with pkgs; [
    gopls
  ];

  pureEnvSetup = ''
    if [[ -z "''${HOME:-}" || "''${HOME}" == "/homeless-shelter" || ! -w "''${HOME}" ]]; then
      UID_VAL="$(id -u)"
      HOME="''${TMPDIR:-/tmp}/sandboxenv-rl-home-''${UID_VAL}"
      export HOME
      mkdir -p "''${HOME}"
    fi
    XDG_CACHE_HOME="''${XDG_CACHE_HOME:-''${HOME}/.cache}"
    export XDG_CACHE_HOME
    mkdir -p "''${XDG_CACHE_HOME}"
    if [[ -z "''${USER:-}" ]]; then
      USER="$(id -un 2>/dev/null || echo runner)"
      export USER
    fi
    export SSL_CERT_FILE="''${SSL_CERT_FILE:-${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt}"
    export NIX_SSL_CERT_FILE="''${NIX_SSL_CERT_FILE:-''${SSL_CERT_FILE}}"
    export GIT_SSL_CAINFO="''${GIT_SSL_CAINFO:-''${SSL_CERT_FILE}}"
    export GOTOOLCHAIN="local"
    # The Python envs install this repository's projects editable, relative
    # to $REPO_ROOT (nix/python.nix).
    REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
    export REPO_ROOT
    export PYTHONNOUSERSITE=1
    export PYTHONUNBUFFERED=1
    # uv only locks here, with the Nix interpreter (it skips the env's python
    # on PATH, and the host's may not exist); never a downloaded one.
    export UV_PYTHON="''${UV_PYTHON:-${pythonEnvs.python}/bin/python3}"
    export UV_PYTHON_DOWNLOADS=never
  '';

  mkHackScript =
    name: script:
    pkgs.writeShellApplication {
      inherit name;
      runtimeInputs = ciPackages;
      text = ''
        cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
        ${pureEnvSetup}
        exec ${script} "$@"
      '';
    };

  ci-go = pkgs.writeShellApplication {
    name = "ci-go";
    runtimeInputs = ciPackages;
    text = ''
      set -euo pipefail
      cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
      ${pureEnvSetup}

      covdir="$(pwd)/coverage"
      # go-daemon too: coverage-summary.py merges every counter dir it finds,
      # and a ci-python run may have left daemon counters from an older build.
      rm -rf "$covdir/go-unit" "$covdir/go-merged" "$covdir/go-daemon"
      mkdir -p "$covdir/go-unit"

      echo "=== [sandboxenv-rl] Go Build, Vet, Test (-race, -cover), Format & Protos ==="
      echo "--> Go build..."
      go build ./...
      echo "--> Go vet..."
      go vet ./...
      echo "--> Go test (-race, -cover)..."
      go test -race -covermode=atomic -coverpkg=./... -coverprofile="$covdir/go.out" ./... -args -test.gocoverdir="$covdir/go-unit"
      echo "--> Format & boilerplate..."
      make verify-gofmt
      make verify-boilerplate
      make verify-go-client-deps
      make PRETTIER=prettier verify-docs
      echo "--> Protobuf stub verification..."
      make verify-protos
      echo "--> docs/metrics.md is generated from internal/telemetry..."
      metrics_md="$(mktemp)"
      go run ./hack/metrics-docs >"$metrics_md"
      diff -u docs/metrics.md "$metrics_md"
      rm -f "$metrics_md"
      echo "--> Coverage summary & diff-cover..."
      ./hack/ci/coverage-summary.py --covdir="$covdir" --merge-covdata --go --diff-cover
      echo "=== [sandboxenv-rl] Go CI checks passed ==="
    '';
  };
  # Shells for the integrations whose envs the default devShell doesn't
  # carry: `nix develop .#openhands` / `.#uni-agent`.
  mkPythonShell =
    env:
    pkgs.mkShell {
      packages = [
        env
        pkgs.uv
        pkgs.git
        pkgs.coreutils
        pkgs.cacert
      ];
      shellHook = pureEnvSetup;
    };
  # Dafny for the hand-written models in docs/plans/telemetry/verify/
  # (hack/verify/dafny.py). A shell of its own: Dafny brings the .NET SDK and
  # Z3, which no other check needs.
  dafnyShell = pkgs.mkShell {
    packages = [
      pkgs.dafny
      pkgs.python3
      pkgs.git
      pkgs.coreutils
    ];
    shellHook = pureEnvSetup;
  };
  openhandsShell = mkPythonShell openhandsEnv;
  uniAgentShell = mkPythonShell uniAgentEnv;
  harborShell = mkPythonShell harborEnv;

  ci-python = pkgs.writeShellApplication {
    name = "ci-python";
    runtimeInputs = ciPackages;
    text = ''
      set -euo pipefail
      cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
      ${pureEnvSetup}

      covdir="$(pwd)/coverage"
      cov_rc="$(pwd)/.coveragerc"
      export COVERAGE_RCFILE="$cov_rc"
      rm -rf "$covdir/py" "$covdir/go-daemon"
      mkdir -p "$covdir/py" "$covdir/bin" "$covdir/go-daemon"

      echo "=== [sandboxenv-rl] Python Lint & Test Suites ==="
      # The base and uni-agent envs are built here, on first use, not as part
      # of the devShell: nothing else needs them, and uni-agent's is a second
      # Python (3.12), so `nix develop -c ci-go` shouldn't build them. They
      # come from the same flake as the devShell: this checkout.
      echo "--> Building the base and uni-agent Python envs..."
      base_env="$(nix build --no-link --print-out-paths .#python-env-base)"
      uni_agent_env="$(nix build --no-link --print-out-paths .#python-env-uni-agent)"
      echo "--> Python ruff check..."
      ruff check clients/python examples/verl_swebench hack/release
      ruff check --config clients/python/pyproject.toml examples/Qwen3-1.7B-verl
      ruff check benchmarking/telemetry_report
      # The base-env suites only mean something if the base env really lacks
      # the optional extras. A Python package added to the devShell can leak
      # every Python input onto PYTHONPATH (nixpkgs' Python setup hook), which
      # would quietly turn them into full-env runs, so check first.
      echo "--> Checking the base env can't import the tracing/metrics extras..."
      if ! "$base_env/bin/python" -c '
      import importlib.util, sys
      leaked = [m for m in ("opentelemetry", "prometheus_client") if importlib.util.find_spec(m)]
      if leaked:
          print("pythonEnvBase can import " + ", ".join(leaked) + "; PYTHONPATH=" + repr(__import__("os").environ.get("PYTHONPATH", "")))
          sys.exit(1)
      '; then
        echo "ERROR: the base env sees packages it must not have, so the base-env suites would test nothing extra." >&2
        echo "Something in the devShell probably put Python site-packages on PYTHONPATH; check \`nix develop -c sh -c 'echo \$PYTHONPATH'\`." >&2
        exit 1
      fi

      echo "--> Building instrumented guest daemon..."
      go build -cover -covermode=atomic -coverpkg=./... -o "$covdir/bin/guest-daemon-cover" ./examples/guest-daemon
      echo "--> Python pytest suites (in parallel)..."
      # Each suite writes its own log; logs print in a fixed order afterwards so
      # the output doesn't interleave. The base-env client run shares a rootdir
      # with the full one, so it skips the pytest cache to avoid racing on it.
      logs="$(mktemp -d)"
      names=()
      pids=()
      cleanup_all() {
        local p
        for p in "''${pids[@]:-}"; do
          if [[ -n "$p" ]]; then
            kill "$p" 2>/dev/null || true
          fi
        done
        rm -rf "$logs"
      }
      trap cleanup_all EXIT INT TERM
      suite() {
        local name="$1" dir="$2"
        shift 2
        (cd "$dir" && "$@") >"$logs/''${#pids[@]}.log" 2>&1 &
        names+=("$name")
        pids+=("$!")
      }

      # shellcheck disable=SC2329
      run_guest_daemon_suite() {
        local port workspace daemon_pid ready
        port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
        workspace="$(mktemp -d)"
        mkdir -p "$workspace/logs"

        # shellcheck disable=SC2329
        cleanup_daemon() {
          if [[ -n "''${daemon_pid:-}" ]]; then
            kill -TERM "$daemon_pid" 2>/dev/null || true
            wait "$daemon_pid" 2>/dev/null || true
          fi
          rm -rf "$workspace"
        }
        trap cleanup_daemon EXIT INT TERM

        GOCOVERDIR="$covdir/go-daemon" "$covdir/bin/guest-daemon-cover" \
          --listen "127.0.0.1:$port" \
          --workspace "$workspace" \
          --log-dir "$workspace/logs" &
        daemon_pid=$!

        ready=0
        for _ in $(seq 1 50); do
          if python3 -c "import socket; s=socket.create_connection(('127.0.0.1', $port), timeout=0.1); s.close()" 2>/dev/null; then
            ready=1
            break
          fi
          sleep 0.1
        done

        if [[ "$ready" -ne 1 ]]; then
          echo "guest daemon failed to start on 127.0.0.1:$port" >&2
          exit 1
        fi

        ATE_ENV_GUEST_TARGET="127.0.0.1:$port" \
        COVERAGE_FILE="$covdir/py/.coverage.guest-daemon" \
        coverage run --rcfile="$cov_rc" -m pytest clients/python/tests/e2e/test_guest_daemon.py -q

        kill -TERM "$daemon_pid"
        wait "$daemon_pid" 2>/dev/null || true
        daemon_pid=""
        rm -rf "$workspace"
        trap - EXIT INT TERM
      }

      suite "clients/python (full env)" clients/python \
        env COVERAGE_FILE="$covdir/py/.coverage.client" coverage run --rcfile="$cov_rc" -m pytest -q
      suite "clients/python (base env, no tracing/metrics)" clients/python \
        env COVERAGE_FILE="$covdir/py/.coverage.client-base" "$base_env/bin/python" -m coverage run --rcfile="$cov_rc" -m pytest -q -p no:cacheprovider
      suite "examples/Qwen3-1.7B-verl" . \
        env COVERAGE_FILE="$covdir/py/.coverage.qwen" coverage run --rcfile="$cov_rc" -m pytest examples/Qwen3-1.7B-verl/tests -q
      suite "integrations/nemo-gym" integrations/nemo-gym \
        env COVERAGE_FILE="$covdir/py/.coverage.nemo" coverage run --rcfile="$cov_rc" -m pytest -q
      suite "integrations/uni-agent (Python 3.12)" . \
        env PATH="$uni_agent_env/bin:$PATH" \
        sh -c 'COVERAGE_FILE="'"$covdir"'/py/.coverage.uni-agent" python -m coverage run --rcfile="'"$cov_rc"'" -m pytest integrations/uni-agent/tests -q && ruff check integrations/uni-agent'
      suite "swebench" . \
        env COVERAGE_FILE="$covdir/py/.coverage.swebench" coverage run --rcfile="$cov_rc" -m pytest internal/benchmarking/swebench/trajectories benchmarking/swebench -q
      # The report tool runs on both envs, like the client: its end-to-end test
      # traces a fleet and needs the telemetry extras.
      suite "benchmarking/telemetry_report (full env)" . \
        env COVERAGE_FILE="$covdir/py/.coverage.telemetry-full" coverage run --rcfile="$cov_rc" -m pytest benchmarking/telemetry_report -q -p no:cacheprovider
      suite "benchmarking/telemetry_report (base env)" . \
        env COVERAGE_FILE="$covdir/py/.coverage.telemetry-base" "$base_env/bin/python" -m coverage run --rcfile="$cov_rc" -m pytest benchmarking/telemetry_report -q -p no:cacheprovider
      suite "clients/python (guest daemon integration)" . run_guest_daemon_suite

      failed=0
      for i in "''${!pids[@]}"; do
        if wait "''${pids[$i]}"; then status=ok; else status=FAILED; failed=1; fi
        echo "--> ''${names[$i]}: $status"
        cat "$logs/$i.log"
      done
      rm -rf "$logs"
      if [[ "$failed" -ne 0 ]]; then
        echo "=== [sandboxenv-rl] Python CI checks FAILED ===" >&2
        exit 1
      fi

      echo "--> Combining Python coverage..."
      shopt -s nullglob
      cov_files=("$covdir/py/.coverage."*)
      if [[ ''${#cov_files[@]} -gt 0 ]]; then
        COVERAGE_FILE="$covdir/py/.coverage" coverage combine --rcfile="$cov_rc" "''${cov_files[@]}"
        COVERAGE_FILE="$covdir/py/.coverage" coverage xml -o "$covdir/py.xml" --rcfile="$cov_rc"
        COVERAGE_FILE="$covdir/py/.coverage" coverage html -d "$covdir/py-html" --rcfile="$cov_rc"
        COVERAGE_FILE="$covdir/py/.coverage" coverage json -o "$covdir/py.json" --rcfile="$cov_rc"
      fi
      shopt -u nullglob

      echo "--> Coverage summary & diff-cover..."
      ./hack/ci/coverage-summary.py --covdir="$covdir" --python --diff-cover

      echo "=== [sandboxenv-rl] Python CI checks passed ==="
    '';
  };

  ci-coverage = pkgs.writeShellApplication {
    name = "ci-coverage";
    runtimeInputs = ciPackages ++ [
      ci-go
      ci-python
    ];
    text = ''
      set -euo pipefail
      cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
      ${pureEnvSetup}

      covdir="$(pwd)/coverage"
      echo "=============================================="
      echo "Running sandboxenv-rl test suites with coverage"
      echo "=============================================="
      ci-go
      ci-python
      echo "=============================================="
      echo "Generating unified test coverage report"
      echo "=============================================="
      ./hack/ci/coverage-summary.py --covdir="$covdir" --merge-covdata --go --python --diff-cover --print-paths
      echo "=============================================="
      echo "Coverage run complete!"
      echo "=============================================="
    '';
  };

  ci = pkgs.writeShellApplication {
    name = "ci";
    runtimeInputs = ciPackages ++ [
      ci-go
      ci-python
    ];
    text = ''
      set -euo pipefail
      echo "=============================================="
      echo "Running sandboxenv-rl local CI verification"
      echo "=============================================="
      ci-go
      ci-python
      echo "=============================================="
      echo "Generating unified test coverage report"
      echo "=============================================="
      ./hack/ci/coverage-summary.py --covdir=coverage --merge-covdata --go --python --diff-cover
      echo "=============================================="
      echo "All sandboxenv-rl local CI checks passed!"
      echo "=============================================="
    '';
  };

  ci-all = ci;

  ci-release = pkgs.writeShellApplication {
    name = "ci-release";
    runtimeInputs = ciPackages;
    text = ''
      set -euo pipefail
      cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
      ${pureEnvSetup}

      bin_tmp="$(mktemp -d)"
      dist_tmp="$(mktemp -d)"
      # shellcheck disable=SC2329
      cleanup() {
        rm -rf "$bin_tmp" "$dist_tmp"
      }
      trap cleanup EXIT INT TERM

      echo "=== [sandboxenv-rl] Release Distribution & Tooling Validation ==="
      echo "--> Cross-compiling ate-env CLI (linux/darwin x amd64/arm64)..."
      for os in linux darwin; do
        for arch in amd64 arm64; do
          echo "    building ate-env for ''${os}/''${arch}..."
          CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -o "$bin_tmp/ate-env_''${os}_''${arch}" ./cmd/ate-env
        done
      done

      ./hack/release/build-dists.sh "$dist_tmp"

      echo "--> Validating release scripts..."
      shellcheck hack/release/*.sh
      python3 -m py_compile hack/release/create-release.py
      python3 hack/release/create-release.py --help >/dev/null
      pytest hack/release/test_create_release.py -v

      echo "=== [sandboxenv-rl] Release validation passed ==="
    '';
  };

  ensure-substrate = mkHackScript "ensure-substrate" "./hack/ci/ensure-substrate.sh";
  ci-e2e = mkHackScript "ci-e2e" "./hack/ci/e2e.py";
  smoke = mkHackScript "smoke" "pytest hack/ci/test_smoke.py -v";
  check-substrate-head = mkHackScript "check-substrate-head" "./hack/ci/check-substrate-head.sh";
  # Pin consistency (hack/ci/check-pins.py), then DeterminateSystems'
  # flake-checker for nixpkgs freshness (< 30 days), upstream owner and
  # supported branch. flake-checker only reports; it does not fail the run.
  check-pins = pkgs.writeShellApplication {
    name = "check-pins";
    runtimeInputs = ciPackages;
    text = ''
      cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
      ${pureEnvSetup}
      ./hack/ci/check-pins.py "$@"
      flake-checker --no-telemetry
    '';
  };
  # update-pins has its own minimal runtimeInputs independent of goToolchain and
  # Python environments so that it evaluates and runs even when the locked
  # nixpkgs cannot evaluate the devShell (for example, when go.mod requests a Go
  # minor version not yet packaged in nixpkgs, which is precisely when
  # `make update-pins ARGS=nixpkgs` is needed to recover). Its `uv lock
  # --upgrade` step runs uv through `nix shell --inputs-from . nixpkgs#uv`, not
  # the devShell, for the same reason.
  update-pins = pkgs.writeShellApplication {
    name = "update-pins";
    runtimeInputs = [
      pkgs.python3
      pkgs.crane
      pkgs.git
      pkgs.coreutils
    ];
    text = ''
      cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
      ${pureEnvSetup}
      exec ./hack/ci/check-pins.py update "$@"
    '';
  };
  lint-actions-locally = mkHackScript "lint-actions-locally" "./hack/ci/lint-actions-locally.sh";
in
{
  inherit
    ciPackages
    devTools
    pureEnvSetup
    ci
    ci-all
    ci-go
    ci-python
    ci-coverage
    ci-release
    ci-e2e
    smoke
    check-substrate-head
    ensure-substrate
    check-pins
    update-pins
    lint-actions-locally
    dafnyShell
    openhandsShell
    uniAgentShell
    harborShell
    pythonEnvBase
    uniAgentEnv
    harborEnv
    ;
  coverage = ci-coverage;
}
