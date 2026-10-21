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

package apiservice

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/telemetry"
	"google.golang.org/grpc/metadata"
)

func TestRequestedAt(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	past := now.Add(-1500 * time.Millisecond)
	cases := []struct {
		name string
		md   metadata.MD
		want time.Time
	}{
		{"no metadata", nil, now},
		{"no header", metadata.Pairs("x-env-id", "a"), now},
		{"valid", metadata.Pairs(telemetry.RequestedAtHeader, past.Format(time.RFC3339Nano)), past},
		{"unparsable", metadata.Pairs(telemetry.RequestedAtHeader, "1500ms ago"), now},
		{"future", metadata.Pairs(telemetry.RequestedAtHeader, now.Add(time.Minute).Format(time.RFC3339Nano)), now},
	}
	for _, tc := range cases {
		ctx := context.Background()
		if tc.md != nil {
			ctx = metadata.NewIncomingContext(ctx, tc.md)
		}
		if got := requestedAt(ctx, now); !got.Equal(tc.want) {
			t.Errorf("%s: requestedAt = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The pending map is bounded: when full, stale entries are dropped and a new
// one is admitted; with no stale entry the new one is not tracked.
func TestTrackCreateIsBounded(t *testing.T) {
	s := &Server{Metrics: telemetry.NewMetrics()}
	old := time.Now().Add(-2 * pendingTTL)
	for i := range maxPending {
		s.trackCreate(DefaultAtespace, string(rune('a'+i%26))+time.Duration(i).String(), old)
	}
	if len(s.pending) != maxPending {
		t.Fatalf("pending = %d, want %d", len(s.pending), maxPending)
	}
	s.trackCreate(DefaultAtespace, "fresh", time.Now())
	if len(s.pending) != 1 {
		t.Errorf("pending after eviction = %d, want 1 (only the fresh entry)", len(s.pending))
	}
	for i := range maxPending - 1 {
		s.trackCreate(DefaultAtespace, "x"+time.Duration(i).String(), time.Now())
	}
	s.trackCreate(DefaultAtespace, "overflow", time.Now())
	if _, ok := s.pending[DefaultAtespace+"/overflow"]; ok {
		t.Error("an entry was admitted past the bound with nothing stale to evict")
	}
}
