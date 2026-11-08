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
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
)

// GuestMountPath is where the ate-env-guest image is mounted inside an actor
// whose container image is a rootfs image rather than the guest itself. The
// guest binary is then at GuestMountPath + "/ko-app/ate-env-guest".
const GuestMountPath = "/ate"

// GuestVolumeName is the name of the image volume carrying the guest.
const GuestVolumeName = "guest"

// defaultGuestBinary is the guest's path inside its own image (ko layout).
const defaultGuestBinary = "/ko-app/ate-env-guest"

// guestBinaryName is what a re-rootable command must start with: only the
// guest's own path moves under the mount, never a shell or another binary.
const guestBinaryName = "ate-env-guest"

// imageTemplateDigestLen is how many hex digits of the digest go into a
// derived template's name: enough to make collisions a non-concern and short
// enough to keep the name a valid k8s short name next to any base name.
const imageTemplateDigestLen = 12

var digestRef = regexp.MustCompile(`@sha256:([0-9a-f]{64})$`)

// shortName is the shape substrate accepts for a resource name (k8s short
// name): lowercase alphanumerics and '-', at most 63 characters.
var shortName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const maxShortNameLen = 63

// ImageDigest returns the hex digest of a digest-pinned image reference, or an
// error when the reference is not pinned. Substrate requires pinned images in
// ActorTemplates because changing the image invalidates snapshots.
func ImageDigest(image string) (string, error) {
	m := digestRef.FindStringSubmatch(image)
	if m == nil {
		return "", fmt.Errorf("image %q is not pinned by digest (want repo@sha256:<64 hex>)", image)
	}
	return m[1], nil
}

// ImageTemplateName is the name of the ActorTemplate derived from base for
// image: "<base>-<first 12 hex digits of the digest>".
func ImageTemplateName(base, image string) (string, error) {
	digest, err := ImageDigest(image)
	if err != nil {
		return "", err
	}
	name := base + "-" + digest[:imageTemplateDigestLen]
	if len(name) > maxShortNameLen {
		return "", fmt.Errorf("derived template name %q is %d characters, the limit is %d; use a shorter base template name",
			name, len(name), maxShortNameLen)
	}
	if !shortName.MatchString(name) {
		return "", fmt.Errorf("derived template name %q is not a valid resource name (lowercase alphanumerics and '-')", name)
	}
	return name, nil
}

// DeriveImageTemplate returns a copy of base, named name, whose container runs
// image unmodified with the ate-env-guest mounted into it as a read-only image
// volume at GuestMountPath.
//
// base can be either kind of template: one whose container image is the
// guest itself (the shape `ate-env manifest template` writes), in which case
// that image becomes the guest volume and the command is re-rooted under the
// mount; or one that already mounts the guest as an image volume (named
// GuestVolumeName), in which case only the container image changes.
// Either way the guest image must be pinned by digest, and the result must be
// a template Substrate accepts, with the guest volume alone at GuestMountPath, so a base that already claims the guest's
// volume name or mount path, or that mounts the guest anywhere else, is
// refused with the reason. A base that already mounts the guest must also
// start it from the mount: its command, which carries over, must begin with
// the guest binary's absolute path under GuestMountPath; no command, or one
// outside the mount, is refused.
// Everything else carries over: worker selector, snapshots, sandbox config,
// resources, env, readiness, and any other volumes, such as extra runtime
// layers, together with the command-line flags that start them.
func DeriveImageTemplate(base *ateapipb.ActorTemplate, name, image string) (*ateapipb.ActorTemplate, error) {
	if base == nil {
		return nil, errors.New("base template is required")
	}
	if name == "" {
		return nil, errors.New("derived template name is required")
	}
	if _, err := ImageDigest(image); err != nil {
		return nil, err
	}
	if len(base.GetContainers()) == 0 {
		return nil, fmt.Errorf("base template %q has no containers", base.GetMetadata().GetName())
	}

	tmpl := proto.Clone(base).(*ateapipb.ActorTemplate)
	tmpl.Metadata = &ateapipb.ResourceMetadata{
		Atespace: base.GetMetadata().GetAtespace(),
		Name:     name,
	}
	tmpl.Status = nil

	c := tmpl.Containers[0]
	if guestVolume(tmpl) == nil {
		// The base runs the guest as its container image: that image becomes
		// the guest volume, and the command moves under the mount.
		guestImage := c.GetImage()
		if _, err := ImageDigest(guestImage); err != nil {
			return nil, fmt.Errorf("base template %q guest image: %w", base.GetMetadata().GetName(), err)
		}
		// Substrate requires unique volume names and mounts that do not
		// nest, so a base that already uses the guest's volume name or mount
		// path would derive a template Substrate rejects. Say why here.
		if err := checkGuestSlotFree(tmpl, c); err != nil {
			return nil, fmt.Errorf("base template %q: %w", base.GetMetadata().GetName(), err)
		}
		tmpl.Volumes = append(tmpl.Volumes, &ateapipb.Volume{
			Name:  GuestVolumeName,
			Image: &ateapipb.ImageVolumeSource{Reference: guestImage},
		})
		c.VolumeMounts = append(c.VolumeMounts, &ateapipb.VolumeMount{
			Name:      GuestVolumeName,
			MountPath: GuestMountPath,
		})
		command, err := rerootCommand(c.GetCommand())
		if err != nil {
			return nil, fmt.Errorf("base template %q: %w", base.GetMetadata().GetName(), err)
		}
		c.Command = command
	} else {
		// The base already mounts the guest. It carries over as is, so it
		// must already be what Substrate accepts: a guest image pinned by
		// digest, as for the other base shape, the guest alone at
		// GuestMountPath, and a command that starts the guest from there.
		// Say why here.
		if _, err := ImageDigest(guestVolume(tmpl).GetImage().GetReference()); err != nil {
			return nil, fmt.Errorf("base template %q guest image: %w", base.GetMetadata().GetName(), err)
		}
		if err := checkGuestMounted(tmpl, c); err != nil {
			return nil, fmt.Errorf("base template %q: %w", base.GetMetadata().GetName(), err)
		}
	}
	c.Image = image
	return tmpl, nil
}

// checkGuestSlotFree reports an error when tmpl already has a volume named
// GuestVolumeName (that is not the guest image volume) or c mounts a volume
// at or under GuestMountPath, where the guest volume goes.
func checkGuestSlotFree(tmpl *ateapipb.ActorTemplate, c *ateapipb.Container) error {
	for _, v := range tmpl.GetVolumes() {
		if v.GetName() == GuestVolumeName {
			return fmt.Errorf("volume %q is not an image volume, but the guest image volume needs that name; rename it", GuestVolumeName)
		}
	}
	for _, m := range c.GetVolumeMounts() {
		if p := m.GetMountPath(); p == GuestMountPath || strings.HasPrefix(p, GuestMountPath+"/") {
			return fmt.Errorf("volume %q is mounted at %s, which overlaps the guest mount at %s; mount it elsewhere", m.GetName(), p, GuestMountPath)
		}
	}
	return nil
}

// checkGuestMounted reports an error unless a base that already has the guest
// image volume has exactly one volume named GuestVolumeName, mounted by c
// exactly once and at GuestMountPath, with nothing else mounted at or under
// GuestMountPath, and c's command starts the guest from that mount
// (checkGuestCommand).
func checkGuestMounted(tmpl *ateapipb.ActorTemplate, c *ateapipb.Container) error {
	named := 0
	for _, v := range tmpl.GetVolumes() {
		if v.GetName() == GuestVolumeName {
			named++
		}
	}
	if named > 1 {
		return fmt.Errorf("%d volumes are named %q, but volume names must be unique; rename the extra ones", named, GuestVolumeName)
	}
	mounted := 0
	for _, m := range c.GetVolumeMounts() {
		p := m.GetMountPath()
		switch {
		case m.GetName() == GuestVolumeName && p == GuestMountPath:
			mounted++
		case m.GetName() == GuestVolumeName:
			return fmt.Errorf("guest volume %q is mounted at %s, but the guest must be mounted at %s; mount it there only", GuestVolumeName, p, GuestMountPath)
		case p == GuestMountPath || strings.HasPrefix(p, GuestMountPath+"/"):
			return fmt.Errorf("volume %q is mounted at %s, which overlaps the guest mount at %s; mount it elsewhere", m.GetName(), p, GuestMountPath)
		}
	}
	if mounted != 1 {
		return fmt.Errorf("guest volume %q is mounted %d times in container %q, want once at %s", GuestVolumeName, mounted, c.GetName(), GuestMountPath)
	}
	return checkGuestCommand(c.GetCommand())
}

// checkGuestCommand reports an error unless command, carried over unchanged
// from a base that already mounts the guest, starts the guest from its
// mount: command[0] must be an absolute path that names the guest binary and
// resolves under GuestMountPath, cleaned as rerootCommand cleans it (as a
// lookup from "/" resolves it, so "/ate/../ko-app/ate-env-guest" is outside
// the mount). An empty command is refused too: the container image is a
// rootfs image, so its own entrypoint would run and the guest never starts.
// This is the rule rerootCommand applies to a guest-as-image base, checked on
// a command that is already re-rooted.
func checkGuestCommand(command []string) error {
	want := path.Join(GuestMountPath, defaultGuestBinary)
	if len(command) == 0 {
		return fmt.Errorf("the container has no command, but the guest must be started from its mount (%s)", want)
	}
	cmd0 := command[0]
	if !strings.HasPrefix(cmd0, "/") || path.Base(cmd0) != guestBinaryName ||
		!strings.HasPrefix(path.Clean(cmd0), GuestMountPath+"/") {
		return fmt.Errorf("command %q does not start the guest from its mount: it must start with the absolute path of the guest binary under %s (.../%s), such as %s",
			strings.Join(command, " "), GuestMountPath, guestBinaryName, want)
	}
	return nil
}

// guestVolume returns the template's guest image volume, or nil when the
// guest is the container image. Other image volumes (extra runtime layers)
// do not count: they can be present in either kind of base.
func guestVolume(tmpl *ateapipb.ActorTemplate) *ateapipb.Volume {
	for _, v := range tmpl.GetVolumes() {
		if v.GetName() == GuestVolumeName && v.GetImage() != nil {
			return v
		}
	}
	return nil
}

// rerootCommand moves a guest command that pointed into the guest image's
// root under GuestMountPath. An empty command means the guest's default path.
// A command that does not start with an absolute path cannot be re-rooted
// (a shell wrapper, for instance), so it is an error rather than a template
// whose guest is silently not found.
func rerootCommand(command []string) ([]string, error) {
	if len(command) == 0 {
		return []string{path.Join(GuestMountPath, defaultGuestBinary)}, nil
	}
	if !strings.HasPrefix(command[0], "/") || path.Base(command[0]) != guestBinaryName {
		return nil, fmt.Errorf("cannot re-root command %q under %s: it must start with the absolute path of the guest binary (.../%s)",
			strings.Join(command, " "), GuestMountPath, guestBinaryName)
	}
	out := append([]string(nil), command...)
	// Clean first: path.Join would let a leading ".." cancel GuestMountPath
	// ("/../ko-app/ate-env-guest" would become "/ko-app/ate-env-guest").
	out[0] = path.Join(GuestMountPath, path.Clean(out[0]))
	return out, nil
}
