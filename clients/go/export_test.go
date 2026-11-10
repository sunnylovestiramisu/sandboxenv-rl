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

package env

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/metadata"
)

// SetPollBoundsForTest sets the first and the largest interval between the
// polls of a ShellRequest.Poll run on c, so a test of many polls runs fast.
func SetPollBoundsForTest(c *Client, minPoll, maxPoll time.Duration) {
	c.pollMin, c.pollMax = minPoll, maxPoll
}

// PollRetries is how many consecutive transient poll errors a
// ShellRequest.Poll run tolerates.
const PollRetries = pollRetries

// SetRPCSemconvModeForTest sets the rpc.* convention of rpcAttrs ("new",
// "old" or "dup") for the rest of t, so that a test does not depend on the
// OTEL_SEMCONV_STABILITY_OPT_IN of the process that runs it.
func SetRPCSemconvModeForTest(t testing.TB, mode string) {
	t.Helper()
	prev := rpcSemconvMode
	rpcSemconvMode = mode
	t.Cleanup(func() { rpcSemconvMode = prev })
}

func ImageRefAttrForTest(ref string) string        { return imageRefAttr(ref) }
func ExecutableForTest(line string) string         { return executable(line) }
func ExecutableWordsForTest(words []string) string { return executableWords(words) }
func ParseRPCSemconvModeForTest(val string) string { return parseRPCSemconvMode(val) }
func RPCAttrsForTest(svc, method, target string) []attribute.KeyValue {
	return rpcAttrs(svc, method, target)
}
func FromGRPCErrorForTest(err error) error { return fromGRPCError(err) }
func MarkStartFailureForTest(ctx context.Context, err error, reason string) error {
	md := metadata.Pairs(StartFailureKey, reason)
	return mark(ctx, err, md)
}

var NonEmptyForTest = nonEmpty
