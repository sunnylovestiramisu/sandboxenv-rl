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
	"strings"
	"testing"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

var hexDigit = rapid.SampledFrom([]rune("0123456789abcdef"))

func genDigest() *rapid.Generator[string] {
	return rapid.StringOfN(hexDigit, 64, 64, -1)
}

func genRepo() *rapid.Generator[string] {
	seg := rapid.StringMatching(`[a-z0-9]([a-z0-9._-]{0,6}[a-z0-9])?`)
	return rapid.Custom(func(t *rapid.T) string {
		host := rapid.SampledFrom([]string{"docker.io", "example.com", "gcr.io", "localhost:5000", "us-docker.pkg.dev"}).Draw(t, "host")
		n := rapid.IntRange(1, 4).Draw(t, "segs")
		parts := []string{host}
		for i := range n {
			parts = append(parts, seg.Draw(t, "seg"+string(rune('0'+i))))
		}
		ref := strings.Join(parts, "/")
		if rapid.Bool().Draw(t, "tag") {
			ref += ":" + rapid.StringMatching(`[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,8}`).Draw(t, "tagv")
		}
		return ref
	})
}

// genDigestLike draws a digest equal to d, one sharing d's first
// imageTemplateDigestLen hex digits, or an unrelated one. Independent draws
// almost never collide (16^12 prefixes), so a property about equal or
// colliding digests needs them drawn on purpose.
func genDigestLike(d string) *rapid.Generator[string] {
	return rapid.OneOf(
		rapid.Just(d),
		rapid.Custom(func(t *rapid.T) string {
			return d[:imageTemplateDigestLen] + genDigest().Draw(t, "tail")[imageTemplateDigestLen:]
		}),
		genDigest(),
	)
}

func genShortName(maxLen int) *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		n := rapid.IntRange(1, maxLen).Draw(t, "len")
		if n == 1 {
			return rapid.StringOfN(rapid.SampledFrom([]rune("abcz09")), 1, 1, -1).Draw(t, "c")
		}
		first := rapid.StringOfN(rapid.SampledFrom([]rune("abcz09")), 1, 1, -1).Draw(t, "first")
		mid := rapid.StringOfN(rapid.SampledFrom([]rune("abz09-")), n-2, n-2, -1).Draw(t, "mid")
		last := rapid.StringOfN(rapid.SampledFrom([]rune("abz09")), 1, 1, -1).Draw(t, "last")
		return first + mid + last
	})
}

// AE-3 (docs/plans/ate-env-api/acceptance-criteria.md). Rule (ImageTemplateName doc, CreateEnvironment doc, AGENTS.md): the derived
// name is "<base>-<first 12 hex of the digest>": a function of base and digest
// only (repository path and tag do not matter), and for a valid base short
// enough it is a valid k8s short name; distinct (base, digest12) never
// collide.
func TestPropImageTemplateNameIdentity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		base := genShortName(50).Draw(t, "base")
		d := genDigest().Draw(t, "digest")
		r1, r2 := genRepo().Draw(t, "repo1"), genRepo().Draw(t, "repo2")
		n1, err1 := ImageTemplateName(base, r1+"@sha256:"+d)
		n2, err2 := ImageTemplateName(base, r2+"@sha256:"+d)
		if err1 != nil || err2 != nil {
			t.Fatalf("valid inputs rejected: %v / %v", err1, err2)
		}
		if n1 != n2 {
			t.Fatalf("same digest, different names: %q vs %q", n1, n2)
		}
		if n1 != base+"-"+d[:12] || len(n1) > 63 || !shortName.MatchString(n1) {
			t.Fatalf("bad derived name %q", n1)
		}
		// Injectivity over (base, digest12). Draw base2 and d2 equal to, or
		// colliding with, base and d on purpose: drawn independently, the
		// "same name" side of the check below never happens.
		base2 := rapid.OneOf(rapid.Just(base), genShortName(50)).Draw(t, "base2")
		d2 := genDigestLike(d).Draw(t, "digest2")
		m, err := ImageTemplateName(base2, r1+"@sha256:"+d2)
		if err != nil {
			t.Fatal(err)
		}
		if (m == n1) != (base2 == base && d2[:12] == d[:12]) {
			t.Fatalf("collision: (%q,%q)->%q and (%q,%q)->%q", base, d, n1, base2, d2, m)
		}
	})
}

// AE-3. Rule: ImageTemplateName never panics and never returns an invalid name,
// whatever the input.
func TestPropImageTemplateNameNeverInvalid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		base := rapid.String().Draw(t, "base")
		image := rapid.OneOf(rapid.String(), rapid.Custom(func(t *rapid.T) string {
			return rapid.String().Draw(t, "p") + "@sha256:" + genDigest().Draw(t, "d")
		})).Draw(t, "image")
		name, err := ImageTemplateName(base, image)
		if err == nil && (len(name) > 63 || !shortName.MatchString(name)) {
			t.Fatalf("invalid name %q accepted", name)
		}
	})
}

// AE-2 (docs/plans/ate-env-api/acceptance-criteria.md). Rule (rerootCommand doc): a guest command that pointed into the guest
// image's root moves under GuestMountPath; one that cannot is an error, never
// "a template whose guest is silently not found". Thus every accepted
// re-rooted command[0] names, under GuestMountPath, the same file command[0]
// named in the guest image's root.
func TestPropRerootStaysUnderMount(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		segs := rapid.SliceOfN(rapid.SampledFrom([]string{"ko-app", "usr", "bin", "..", ".", "", "x"}), 0, 5).Draw(t, "segs")
		// Mostly the guest binary, sometimes another file, sometimes a
		// relative path (a shell wrapper's "sh -c ..." is the real case).
		last := rapid.SampledFrom([]string{guestBinaryName, guestBinaryName, guestBinaryName, "sh", guestBinaryName + "2"}).Draw(t, "last")
		cmd0 := strings.Join(append(segs, last), "/")
		if rapid.IntRange(0, 4).Draw(t, "rooted") > 0 {
			cmd0 = "/" + cmd0
		}
		rooted := strings.HasPrefix(cmd0, "/") // an empty first segment roots it too
		out, err := rerootCommand([]string{cmd0, "-flag"})
		// Accepted exactly when command[0] is an absolute path to the guest
		// binary (rerootCommand doc). Checked both ways: a regression that
		// rejected every command must fail here, not skip.
		if wantOK := rooted && last == guestBinaryName; (err == nil) != wantOK {
			t.Fatalf("rerootCommand(%q): err = %v, want accepted = %v", cmd0, err, wantOK)
		}
		if err != nil {
			return
		}
		// Resolve as the kernel would: in the guest image's root, ".." at
		// "/" stays at "/".
		want := GuestMountPath + cleanRooted(cmd0)
		if out[0] != want {
			t.Fatalf("rerootCommand(%q)[0] = %q, want %q (the same file under the mount)", cmd0, out[0], want)
		}
	})
}

// cleanRooted resolves a rooted path lexically, as a lookup from "/" does.
func cleanRooted(p string) string {
	var stack []string
	for _, s := range strings.Split(p, "/") {
		switch s {
		case "", ".":
		case "..":
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		default:
			stack = append(stack, s)
		}
	}
	return "/" + strings.Join(stack, "/")
}

// The shrunk counterexample of TestPropRerootStaysUnderMount, pinned.
func TestRerootKeepsDotDotUnderMount(t *testing.T) {
	out, err := rerootCommand([]string{"/../ko-app/ate-env-guest"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out[0] != GuestMountPath+"/ko-app/ate-env-guest" {
		t.Errorf("rerootCommand(/../ko-app/ate-env-guest) = %q: outside %s, the guest is looked up in the rootfs image", out[0], GuestMountPath)
	}
}

// AE-4 (docs/plans/ate-env-api/acceptance-criteria.md). Rule (DeriveImageTemplate doc): deriving from an already-derived template
// changes only the container image (and the name); thus
// Derive(Derive(base, n1, i1), n2, i2) == Derive(base, n2, i2). The base is
// never changed, and a derived template is one Substrate can run with the
// guest found (checkDerivedTemplate). A base that already claims the guest's
// volume name or mount path, or that mounts the guest anywhere but alone at
// GuestMountPath, or that mounts the guest but does not start it from there,
// is refused instead (claimsGuestSlot); every other base derives.
func TestPropDeriveComposes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		base := genBaseTemplate().Draw(t, "base")
		i1 := genRepo().Draw(t, "r1") + "@sha256:" + genDigest().Draw(t, "d1")
		i2 := genRepo().Draw(t, "r2") + "@sha256:" + genDigest().Draw(t, "d2")
		before := proto.Clone(base)
		once, err := DeriveImageTemplate(base, "n2", i2)
		if !proto.Equal(base, before) {
			t.Fatal("base mutated")
		}
		// Checked both ways: a base whose guest slot is taken must be
		// refused (accepting it carries a broken mount layout over), and an
		// ordinary base must derive.
		if claims := claimsGuestSlot(base); (err != nil) != claims {
			t.Fatalf("DeriveImageTemplate: err = %v, want refused = %v; volumes = %v, mounts = %v, command = %q",
				err, claims, base.GetVolumes(), base.GetContainers()[0].GetVolumeMounts(), base.GetContainers()[0].GetCommand())
		}
		if err != nil {
			return
		}
		checkDerivedTemplate(t, once, i2)
		d1, err := DeriveImageTemplate(base, "n1", i1)
		if err != nil {
			t.Fatalf("derive n1 failed after n2 succeeded: %v", err)
		}
		twice, err := DeriveImageTemplate(d1, "n2", i2)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(twice, once) {
			t.Fatalf("derive does not compose:\n twice=%v\n once=%v", twice, once)
		}
	})
}

// genBaseTemplate draws a base template of either documented shape. Half
// are guest-as-image bases (the shape `ate-env manifest template` writes):
// modeABase. Half already mount the guest (a template derived before, or
// written by hand that way): modeABase running a rootfs image, with a guest
// image volume mounted at GuestMountPath, mostly, or elsewhere, twice, or not
// at all, and a command that starts the guest under the mount, mostly, or
// outside it (the guest's own path, or one ".." leads out of the mount), a
// shell under the mount, or no command. Either kind gets one to three containers, zero to three extra
// volumes with distinct names, of image or durable-dir kind, mounted into
// the first container at distinct paths; a guest-as-image base maybe has no
// command (the guest's default path). Names and mount paths include the guest's own
// (GuestVolumeName, GuestMountPath and under it): those are the inputs where
// the guest can end up not alone at GuestMountPath, since Substrate requires
// unique volume names and mounts that do not nest. A sibling such as
// "/ate-cache" shares the prefix but does not nest, so it must not be
// refused.
func genBaseTemplate() *rapid.Generator[*ateapipb.ActorTemplate] {
	return rapid.Custom(func(t *rapid.T) *ateapipb.ActorTemplate {
		base := modeABase()
		paths := map[string]bool{}
		c := base.Containers[0]
		injected := rapid.Bool().Draw(t, "injected")
		if injected {
			c.Image = "example.com/rootfs@sha256:" + genDigest().Draw(t, "rd")
			c.Command = []string{GuestMountPath + defaultGuestBinary, "-workspace", "/workspace"}
			base.Volumes = append(base.Volumes, &ateapipb.Volume{Name: GuestVolumeName, Image: &ateapipb.ImageVolumeSource{Reference: testGuestImage}})
			var guestPaths []string
			switch rapid.SampledFrom([]string{"alone", "alone", "elsewhere", "twice", "none"}).Draw(t, "guestmount") {
			case "alone":
				guestPaths = []string{GuestMountPath}
			case "elsewhere":
				guestPaths = []string{"/opt/" + GuestVolumeName}
			case "twice":
				guestPaths = []string{GuestMountPath, "/opt/" + GuestVolumeName}
			}
			for _, p := range guestPaths {
				paths[p] = true
				c.VolumeMounts = append(c.VolumeMounts, &ateapipb.VolumeMount{Name: GuestVolumeName, MountPath: p})
			}
		}
		for i := range rapid.IntRange(0, 2).Draw(t, "sidecars") {
			base.Containers = append(base.Containers, &ateapipb.Container{
				Name: "side" + string(rune('a'+i)), Image: "example.com/side@sha256:" + genDigest().Draw(t, "sd"),
			})
		}
		names := rapid.SliceOfNDistinct(rapid.SampledFrom([]string{"execd", "py", "tools", "cache", GuestVolumeName}), 0, 3, rapid.ID[string]).Draw(t, "volumes")
		for _, name := range names {
			v := &ateapipb.Volume{Name: name}
			// An extra volume named GuestVolumeName is not an image: an
			// image one is the guest itself, drawn above.
			if name != GuestVolumeName && rapid.Bool().Draw(t, "image") {
				v.Image = &ateapipb.ImageVolumeSource{Reference: "example.com/l@sha256:" + genDigest().Draw(t, "ld")}
			} else {
				v.DurableDir = &ateapipb.DurableDirVolumeSource{}
			}
			base.Volumes = append(base.Volumes, v)
			p := rapid.SampledFrom([]string{"/opt/" + name, GuestMountPath, GuestMountPath + "/sub", GuestMountPath + "-" + name}).Draw(t, "mount")
			if paths[p] {
				p = "/opt/" + name
			}
			if paths[p] {
				p = "/srv/" + name // "/opt/guest" taken by the guest mount
			}
			paths[p] = true
			c.VolumeMounts = append(c.VolumeMounts, &ateapipb.VolumeMount{Name: name, MountPath: p})
		}
		if injected {
			// The command carries over, so draw one under the mount (the
			// documented shape), one outside it (the guest's path in its
			// own image, or one that ".." leads out of the mount), another
			// binary under the mount (a shell), or none.
			switch rapid.SampledFrom([]string{"under", "under", "under", "outside", "escape", "other", "none"}).Draw(t, "guestcmd") {
			case "other":
				c.Command = []string{GuestMountPath + "/bin/sh", "-c", "exec " + GuestMountPath + defaultGuestBinary}
			case "outside":
				c.Command[0] = defaultGuestBinary
			case "escape":
				c.Command[0] = GuestMountPath + "/.." + defaultGuestBinary
			case "none":
				c.Command = nil
			}
		} else if rapid.Bool().Draw(t, "nocmd") {
			c.Command = nil
		}
		return base
	})
}

// claimsGuestSlot reports whether base must be refused because the derived
// template could not have the guest alone at GuestMountPath. A base whose
// only volume named GuestVolumeName is an image volume already carries the
// guest: it is refused unless its first container mounts that volume exactly
// once, at GuestMountPath, and nothing else at or under it, and its command
// (which carries over) starts the guest from the mount (startsGuestFromMount):
// a command under the mount derives; one outside it, another binary under
// it, or none is refused. Any
// other base gets the guest volume added and its command re-rooted (none
// means the guest's default path): it is refused when it already has a
// volume named GuestVolumeName or a first-container mount at or under
// GuestMountPath.
func claimsGuestSlot(base *ateapipb.ActorTemplate) bool {
	var guestVols, guestImageVols int
	for _, v := range base.GetVolumes() {
		if v.GetName() == GuestVolumeName {
			guestVols++
			if v.GetImage() != nil {
				guestImageVols++
			}
		}
	}
	var guestAtMount, others int
	for _, m := range base.GetContainers()[0].GetVolumeMounts() {
		p := m.GetMountPath()
		atMount := p == GuestMountPath || strings.HasPrefix(p, GuestMountPath+"/")
		switch {
		case m.GetName() == GuestVolumeName && p == GuestMountPath:
			guestAtMount++
		case m.GetName() == GuestVolumeName || atMount:
			others++
		}
	}
	if guestImageVols > 0 {
		return guestVols != 1 || guestAtMount != 1 || others != 0 ||
			!startsGuestFromMount(base.GetContainers()[0].GetCommand())
	}
	return guestVols > 0 || guestAtMount > 0 || others > 0
}

// startsGuestFromMount reports whether command starts the guest binary from
// GuestMountPath: command[0] is rooted, names the guest binary, and resolved
// from "/" (cleanRooted) lies under GuestMountPath. No command does not: the
// rootfs image's own entrypoint would run instead.
func startsGuestFromMount(command []string) bool {
	if len(command) == 0 || !strings.HasPrefix(command[0], "/") {
		return false
	}
	resolved := cleanRooted(command[0])
	return strings.HasPrefix(resolved, GuestMountPath+"/") && strings.HasSuffix(resolved, "/"+guestBinaryName)
}

// checkDerivedTemplate asserts that a derived template is one Substrate can
// run with the guest found: unique volume names, every mount naming a
// volume, unique mount paths per container, the first container running
// image, exactly one image volume named GuestVolumeName mounted at
// GuestMountPath in the first container and nothing else mounted at or under
// it (Substrate rejects nested mounts), and a first-container command that
// starts the guest binary from GuestMountPath (startsGuestFromMount).
func checkDerivedTemplate(t *rapid.T, tmpl *ateapipb.ActorTemplate, image string) {
	vols := map[string]*ateapipb.Volume{}
	for _, v := range tmpl.GetVolumes() {
		if vols[v.GetName()] != nil {
			t.Fatalf("duplicate volume %q in %v", v.GetName(), tmpl.GetVolumes())
		}
		vols[v.GetName()] = v
	}
	for _, c := range tmpl.GetContainers() {
		paths := map[string]bool{}
		for _, m := range c.GetVolumeMounts() {
			if vols[m.GetName()] == nil {
				t.Fatalf("container %q mounts missing volume %q", c.GetName(), m.GetName())
			}
			if paths[m.GetMountPath()] {
				t.Fatalf("container %q mounts two volumes at %q: %v", c.GetName(), m.GetMountPath(), c.GetVolumeMounts())
			}
			paths[m.GetMountPath()] = true
		}
	}
	c := tmpl.GetContainers()[0]
	if c.GetImage() != image {
		t.Fatalf("first container runs %q, want %q", c.GetImage(), image)
	}
	if g := vols[GuestVolumeName]; g == nil || g.GetImage() == nil {
		t.Fatalf("no guest image volume %q in %v", GuestVolumeName, tmpl.GetVolumes())
	}
	guestMounts := 0
	for _, m := range c.GetVolumeMounts() {
		if m.GetName() == GuestVolumeName {
			guestMounts++
		}
		atMount := m.GetMountPath() == GuestMountPath || strings.HasPrefix(m.GetMountPath(), GuestMountPath+"/")
		if (m.GetName() == GuestVolumeName) != (m.GetMountPath() == GuestMountPath) || (atMount && m.GetName() != GuestVolumeName) {
			t.Fatalf("guest not alone at %s: %v", GuestMountPath, c.GetVolumeMounts())
		}
	}
	if guestMounts != 1 {
		t.Fatalf("guest volume mounted %d times, want once at %s: %v", guestMounts, GuestMountPath, c.GetVolumeMounts())
	}
	// For either base shape the guest must be found at boot: the command
	// starts the guest binary from the mount (re-rooted for a guest-as-image
	// base, carried over and checked for one that already mounts the guest).
	if !startsGuestFromMount(c.GetCommand()) {
		t.Fatalf("command %q does not start the guest from %s", c.GetCommand(), GuestMountPath)
	}
}

// AE-1 (supporting). Rule (sameImageDigest doc): two pinned references name the same image iff
// their digests are equal, whatever the repository path.
func TestPropSameImageDigest(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// d2 is often d1 itself: "same digest, other repository path" is the
		// case sameImageDigest exists for, and independent draws never hit it.
		d1 := genDigest().Draw(t, "d1")
		d2 := genDigestLike(d1).Draw(t, "d2")
		a := genRepo().Draw(t, "a") + "@sha256:" + d1
		b := genRepo().Draw(t, "b") + "@sha256:" + d2
		if sameImageDigest(a, b) != (d1 == d2) || !sameImageDigest(a, a) || sameImageDigest(a, b) != sameImageDigest(b, a) {
			t.Fatalf("sameImageDigest(%q,%q) wrong", a, b)
		}
	})
}
