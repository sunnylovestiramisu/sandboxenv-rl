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
	"runtime/debug"
	"slices"
	"testing"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
)

func TestGetServerInfoFeaturesFollowConfig(t *testing.T) {
	cases := []struct {
		name string
		info ServerInfo
		want []string
	}{
		{"defaults", ServerInfo{}, []string{FeatureRootfsImage, FeaturePTY, FeatureSuspendMode, FeatureImageStatus, FeaturePlacementDomains, FeatureWarmImage}},
		{"port proxy", ServerInfo{PortProxy: true}, []string{FeatureRootfsImage, FeaturePTY, FeatureSuspendMode, FeatureImageStatus, FeaturePlacementDomains, FeatureWarmImage, FeaturePortProxy}},
	}
	for _, tc := range cases {
		s := New(nil, "", "")
		s.Info = tc.info
		resp, err := s.GetServerInfo(context.Background(), &ateenvv1alpha.GetServerInfoRequest{})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !slices.Equal(resp.GetFeatures(), tc.want) {
			t.Errorf("%s: features = %v, want %v", tc.name, resp.GetFeatures(), tc.want)
		}
		if resp.GetServer() != ServerName {
			t.Errorf("%s: server = %q, want %q", tc.name, resp.GetServer(), ServerName)
		}
		if resp.GetSubstrateVersion() == "" {
			t.Errorf("%s: substrate_version is empty, want a version or \"unknown\"", tc.name)
		}
	}
}

func TestGetServerInfoVersion(t *testing.T) {
	s := New(nil, "", "")
	resp, err := s.GetServerInfo(context.Background(), &ateenvv1alpha.GetServerInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetVersion() != "dev" {
		t.Errorf("version without Info.Version = %q, want \"dev\"", resp.GetVersion())
	}
	s.Info.Version = "v0.3.0"
	resp, _ = s.GetServerInfo(context.Background(), &ateenvv1alpha.GetServerInfoRequest{})
	if resp.GetVersion() != "v0.3.0" {
		t.Errorf("version = %q, want \"v0.3.0\"", resp.GetVersion())
	}
}

func TestSubstrateVersion(t *testing.T) {
	deps := func(mods ...*debug.Module) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) { return &debug.BuildInfo{Deps: mods}, true }
	}
	cases := []struct {
		name string
		read func() (*debug.BuildInfo, bool)
		want string
	}{
		{"pinned", deps(&debug.Module{Path: "go.ate.dev/substrate", Version: "v0.5.1-0.20261018212731-ddb46651d605"}), "v0.5.1-0.20261018212731-ddb46651d605"},
		{"replaced", deps(&debug.Module{Path: "go.ate.dev/substrate", Version: "v0.5.0", Replace: &debug.Module{Path: "../substrate", Version: "v0.6.0"}}), "v0.6.0"},
		{"local replace without version", deps(&debug.Module{Path: "go.ate.dev/substrate", Version: "v0.5.0", Replace: &debug.Module{Path: "../substrate"}}), "v0.5.0+replaced"},
		{"absent", deps(&debug.Module{Path: "google.golang.org/grpc", Version: "v1.84.0"}), "unknown"},
		{"no build info", func() (*debug.BuildInfo, bool) { return nil, false }, "unknown"},
	}
	for _, tc := range cases {
		if got := substrateVersion(tc.read); got != tc.want {
			t.Errorf("%s: substrateVersion = %q, want %q", tc.name, got, tc.want)
		}
	}
}
