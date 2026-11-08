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

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
)

// ServerName is the server implementation GetServerInfo reports. Another
// ate-env-api build that serves the same API under another name lets a client
// tell the two apart.
const ServerName = "sandboxenv-rl"

// Optional features GetServerInfo reports. A client checks for a name before
// it depends on the feature and ignores names it does not know. A name keeps
// its meaning once published, because clients and stored run records use it.
const (
	// FeatureRootfsImage: CreateEnvironment accepts rootfs_image.
	FeatureRootfsImage = "rootfs_image"
	// FeaturePTY: StartProcess accepts tty, and ResizeProcess is served.
	FeaturePTY = "pty"
	// FeatureSuspendMode: SuspendEnvironment accepts mode, so a caller can ask
	// for a local suspend (SUSPEND_MODE_LOCAL).
	FeatureSuspendMode = "suspend_mode"
	// FeaturePortProxy: the listener forwards HTTP to ports in environments.
	FeaturePortProxy = "port_proxy"
	// FeatureImageStatus: GetImageStatus and EnsureImageTemplate are served.
	FeatureImageStatus = "image_status"
	// FeaturePlacementDomains: ListPlacementDomains is served (whether it is
	// allowed depends on the access policy).
	FeaturePlacementDomains = "placement_domains"
	// FeatureWarmImage: WarmImage is served.
	FeatureWarmImage = "warm_image"
)

const substrateModule = "go.ate.dev/substrate"

// ServerInfo is what the binary tells the Server about its build and
// configuration, for GetServerInfo.
type ServerInfo struct {
	// Version is the build version of the binary; empty reports "dev".
	Version string
	// PortProxy is true when the listener serves the port proxy.
	PortProxy bool
}

// GetServerInfo reports the server implementation, its version, the Substrate
// module it was built against and its optional features.
func (s *Server) GetServerInfo(ctx context.Context, req *ateenvv1alpha.GetServerInfoRequest) (*ateenvv1alpha.GetServerInfoResponse, error) {
	version := s.Info.Version
	if version == "" {
		version = "dev"
	}
	return &ateenvv1alpha.GetServerInfoResponse{
		Server:           ServerName,
		Version:          version,
		SubstrateVersion: substrateVersion(debug.ReadBuildInfo),
		Features:         s.features(),
	}, nil
}

// features lists the optional features this server serves, in a fixed order.
func (s *Server) features() []string {
	f := []string{FeatureRootfsImage, FeaturePTY, FeatureSuspendMode, FeatureImageStatus, FeaturePlacementDomains, FeatureWarmImage}
	if s.Info.PortProxy {
		f = append(f, FeaturePortProxy)
	}
	return f
}

// substrateVersion returns the version of the Substrate module in the binary's
// build information, or "unknown". This is the module the binary was built
// against, not the version of the control plane it talks to. A replacement
// with a version reports that version; a local replacement without one
// reports the go.mod version with "+replaced", because the binary does not
// contain that version.
func substrateVersion(read func() (*debug.BuildInfo, bool)) string {
	bi, ok := read()
	if !ok {
		return "unknown"
	}
	for _, dep := range bi.Deps {
		if dep.Path != substrateModule {
			continue
		}
		if dep.Replace != nil {
			if dep.Replace.Version != "" {
				return dep.Replace.Version
			}
			if dep.Version != "" {
				return dep.Version + "+replaced"
			}
			return "unknown+replaced"
		}
		if dep.Version != "" {
			return dep.Version
		}
	}
	return "unknown"
}
