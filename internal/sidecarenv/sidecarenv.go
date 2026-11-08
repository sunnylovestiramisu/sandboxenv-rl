// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sidecarenv carries environment variables that only the guest's
// sidecars get. `ate-env manifest template --sidecar-env NAME=VALUE` sets
// Prefix+NAME=VALUE on the actor's container. At start the guest takes these
// variables out of its own environment with Take, before it starts anything,
// and adds them back, without the prefix, only to the sidecars' environment.
// Thus the commands that the guest starts for ate-env-api, and their children,
// inherit an environment without them.
package sidecarenv

import (
	"os"
	"strings"
)

// Prefix marks a container environment variable as sidecar-only.
const Prefix = "ATE_SIDECAR_ENV_"

// RequiredFlag is the guest flag that a template with sidecar-only
// variables carries. A guest that does not know Prefix does not know this
// flag either, thus it exits at flag parse and the actor crashes at start.
// Without the flag, such a guest would start the sidecars without the
// variables, and a sidecar that treats a missing key as "no
// authentication" (the OpenHands agent-server does) would serve everyone.
// A guest that knows the flag exits when Take finds nothing.
const RequiredFlag = "sidecar-env-required"

// Take removes each Prefix variable from the process environment and returns
// them as NAME=VALUE with the prefix stripped, in environment order. A
// variable that is the prefix alone is removed and not returned.
//
// Take changes the environment that os.Environ and os/exec give to child
// processes. It does not change the memory that /proc/<pid>/environ shows:
// that is the environment the process was started with.
func Take() []string {
	var out []string
	for _, kv := range os.Environ() {
		key, value, _ := strings.Cut(kv, "=")
		name, ok := strings.CutPrefix(key, Prefix)
		if !ok {
			continue
		}
		os.Unsetenv(key)
		if name != "" {
			out = append(out, name+"="+value)
		}
	}
	return out
}

// Names returns the NAME of each NAME=VALUE that Take returned, for a log
// line that must not show the values.
func Names(env []string) []string {
	names := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		names = append(names, name)
	}
	return names
}
