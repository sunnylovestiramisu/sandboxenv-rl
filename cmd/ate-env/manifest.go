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

package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/hostcheck"
	"github.com/agent-substrate/env/internal/routertls"
	"github.com/agent-substrate/env/internal/sidecarenv"
	"github.com/spf13/cobra"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"
)

// apiName is the name of the API service Deployment and Service.
const apiName = "ate-env-api"

// defaultAuthTokenSecret is the Secret, key token, that holds the bearer
// token ate-env-api requires from its callers unless --insecure-no-auth is
// given. "ate-env manifest auth-secret" prints one.
const (
	defaultAuthTokenSecret = "ate-env-api-token"
	authTokenSecretKey     = "token"
	authTokenMountPath     = "/var/run/secrets/ate-env-auth"
)

// authTokenBytes is the size of a generated caller token before hex encoding.
const authTokenBytes = 32

// Where the API deployment mounts the credentials for the router's TLS
// listeners: the pod identity credential bundle and the servicedns trust
// bundle, the same paths the router itself uses.
const (
	routerCredentialBundlePath = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
	routerTrustBundlePath      = "/run/servicedns-ca/trust-bundle.pem"
)

// workerPoolLabel labels the WorkerPool with its own name, and templates
// select their pool by it. Substrate copies a pool's labels to its workers and
// matches a template's workerSelector against them. Since substrate#2780 the
// template names no sandbox class, thus without a selector an actor that
// boots without a snapshot can land on any pool of any class in the cluster.
const workerPoolLabel = "ate-env/workerpool"

// defaultWorkerPool is the pool name `ate-env manifest` gives its WorkerPool,
// <template>-workerpool, unless --workerpool says otherwise.
func defaultWorkerPool(template string) string { return template + "-workerpool" }

// defaultTemplatePool is the pool a template selects unless --workerpool says
// otherwise: the one `ate-env manifest` creates with its defaults. A cluster
// usually runs one pool and registers many templates on it, thus the default
// does not follow the template's own name.
var defaultTemplatePool = defaultWorkerPool(apiservice.DefaultTemplate)

// maxLabelValueLen is the Kubernetes limit on a label value. The pool name is
// the value of workerPoolLabel, thus it must fit, while a WorkerPool name
// itself can be longer.
const maxLabelValueLen = 63

func checkWorkerPoolName(pool string) error {
	if len(pool) > maxLabelValueLen {
		return fmt.Errorf("worker pool name %q has %d characters; it is the value of the %s label, "+
			"which allows %d: pass a shorter --workerpool", pool, len(pool), workerPoolLabel, maxLabelValueLen)
	}
	return nil
}

// sandboxConfigName is the Substrate SandboxConfig the WorkerPool's gvisor
// sandbox class refers to. `ate-setup deploy ate-system` installs exactly this
// one, so a pool declared with it is schedulable on a stock install. Since
// substrate#2780 the ActorTemplate names no sandbox: the worker pool decides.
const sandboxConfigName = "gvisor-default"

// Default compute limits for each actor. Substrate requires both on every
// ActorTemplate since substrate#2285: the scheduler books them against worker
// capacity. Two CPUs and 4 GiB match the Python client's ResourceLimits
// default; size them for the workload with --cpu and --memory.
const (
	defaultActorCPU    = "2"
	defaultActorMemory = "4Gi"
)

// Wakeup-probe timeouts. Substrate fails an actor start or resume whose
// probe has not answered 200 within the template's timeout_seconds (1 to
// 3600; 30 when unset). The guest's /readyz waits for each --sidecar-readyz
// URL, so a template with one inherits its sidecar's boot time: an OpenHands
// agent-server takes 20 to 26 s on a quiet node and 22 to 29 s with 3 to 4
// boots per node (p50 27.9 s), and 7 boots per node exceed 30 s. RL groups
// start this way, many rollouts of one new task image at once, thus such a
// template gets four times Substrate's 30 s default: about twice the 60 to
// 65 s that 8 boots on a 16-CPU node extrapolate to.
const (
	substrateWakeupTimeout = 30 * time.Second
	sidecarWakeupTimeout   = 120 * time.Second
	maxWakeupTimeout       = 3600 * time.Second
)

// manifestConfig holds configuration for generating Kubernetes deployment manifests.
type manifestConfig struct {
	namespace          string
	template           string
	workerPool         string
	workerImage        string
	replicas           int32
	apiImage           string
	apiReplicas        int32
	apiPort            int32
	authTokenSecret    string
	insecureNoAuth     bool
	portProxyAtespaces string
	// allowedHosts are host names, next to the Service's in-cluster names,
	// that HTTP callers may send to ate-env-api (its -allowed-hosts).
	allowedHosts []string
	// routerTLS mounts the pod identity certificate and the servicedns trust
	// bundle into ate-env-api and points it at the router's TLS listeners,
	// the only ones Substrate serves since v0.6.0. Off only for the test
	// fakes, which serve the plaintext listeners.
	routerTLS bool

	// Telemetry of ate-env-api. enableTracing renders --enable-tracing;
	// otlpEndpoint and otlpInsecure set OTEL_EXPORTER_OTLP_ENDPOINT and
	// OTEL_EXPORTER_OTLP_INSECURE for its exporter. metricsPort, when not
	// zero, renders --metrics-listen-addr=:<port> and exposes the port as
	// "metrics" on the container and the Service, with prometheus.io scrape
	// annotations on the pod; zero leaves metrics off (the server's
	// listener is off unless --metrics-listen-addr is set).
	enableTracing bool
	otlpEndpoint  string
	otlpInsecure  bool
	metricsPort   int32
}

// resolveAuth applies --insecure-no-auth and rejects an empty --auth-token-secret.
func (c *manifestConfig) resolveAuth(secretChanged bool) error {
	if c.insecureNoAuth {
		if secretChanged {
			return errors.New("--auth-token-secret and --insecure-no-auth cannot be used together")
		}
		c.authTokenSecret = ""
		return nil
	}
	c.authTokenSecret = strings.TrimSpace(c.authTokenSecret)
	if c.authTokenSecret == "" {
		return errors.New("--auth-token-secret is empty; pass --insecure-no-auth to deploy ate-env-api without a caller token")
	}
	return nil
}

// metricsPortName names the ate-env-api metrics port on the container and
// the Service.
const metricsPortName = "metrics"

func (c *manifestConfig) resolveImages() error {
	if c.workerImage == "" {
		return errors.New(`--worker-image is required: the worker image is
ateworker-gvisor from the Substrate repo and must match the Substrate version
deployed on your cluster. From a Substrate checkout at that version run
"go run ./cmd/ate-setup publish worker-images": it pushes the images and
prints the digest-pinned refs to use. Pin by digest: a registry's :latest
is whatever was built last, and a stale worker image registers workers
with no capacity`)
	}
	if c.apiImage == "" {
		return errors.New(`--api-image is required when ate-env is built from source; release
binaries default it to the ate-env-api image published with the release
(see docs/release.md), or build and push your own with make images`)
	}
	return nil
}

// validateTelemetry checks the telemetry flags of the API deployment.
func (c *manifestConfig) validateTelemetry() error {
	if c.metricsPort < 0 || c.metricsPort > 65535 {
		return fmt.Errorf("--metrics-port %d is not a port number", c.metricsPort)
	}
	if c.metricsPort != 0 && c.metricsPort == c.apiPort {
		return fmt.Errorf("--metrics-port %d is the API port; metrics are served on a listener of their own", c.metricsPort)
	}
	return nil
}

// templateConfig holds configuration for generating a Substrate ActorTemplate manifest.
type templateConfig struct {
	template string
	// workerPool is the WorkerPool the template's actors run on; the template
	// selects it by workerPoolLabel.
	workerPool      string
	atespace        string
	guestImage      string
	guestCommand    []string
	snapshotsBucket string
	// rootfsImage, when set, is the digest-pinned image the actor runs; the
	// guest is then mounted into it as an image volume instead of being the
	// container image itself.
	rootfsImage string
	// workspace, when set, is passed to the guest as -workspace.
	workspace string
	// layers are extra runtimes mounted as read-only image volumes, each
	// "name=image@sha256:...=/mount/path".
	layers []string
	// sidecars are command lines the guest starts and supervises next to
	// itself (absolute binary path first), one per -sidecar flag.
	sidecars []string
	// sidecarReadyz are URLs the guest's /readyz waits for.
	sidecarReadyz []string
	// cpu and memory are the actor's compute limits as Kubernetes quantities.
	// Empty means defaultActorCPU and defaultActorMemory.
	cpu    string
	memory string
	// wakeupTimeout is the wakeup probe's timeout_seconds as a duration.
	// Zero means sidecarWakeupTimeout with --sidecar-readyz and
	// substrateWakeupTimeout without.
	wakeupTimeout time.Duration
	// env are extra container environment variables, each NAME=VALUE. The
	// guest passes its environment to sidecars, so this is how a runtime
	// layer's daemon is configured. Commands that the guest starts inherit
	// them too.
	env []string
	// sidecarEnv are environment variables, each NAME=VALUE, that only the
	// sidecars get: the container carries them as sidecarenv.Prefix+NAME,
	// and the guest removes them from its own environment at start.
	sidecarEnv []string
}

// wakeupTimeoutSeconds returns the probe timeout the template carries.
func (c *templateConfig) wakeupTimeoutSeconds() int32 {
	d := c.wakeupTimeout
	if d == 0 {
		d = substrateWakeupTimeout
		if len(c.sidecarReadyz) > 0 {
			d = sidecarWakeupTimeout
		}
	}
	return int32(d / time.Second)
}

// validateWakeupTimeout checks --wakeup-timeout against Substrate's bounds:
// whole seconds from 1 s to 1 h. Zero means the default.
func (c *templateConfig) validateWakeupTimeout() error {
	d := c.wakeupTimeout
	if d == 0 {
		return nil
	}
	if d < time.Second || d > maxWakeupTimeout || d%time.Second != 0 {
		return fmt.Errorf("--wakeup-timeout %s: want whole seconds from 1s to %s", d, maxWakeupTimeout)
	}
	return nil
}

// runtimeLayer is a parsed --layer value.
type runtimeLayer struct {
	name, image, mountPath string
}

// parseLayer parses "name=image@sha256:...=/mount/path". The image reference
// carries no '=' and the mount path is absolute, so splitting on the first two
// '=' is unambiguous.
func parseLayer(v string) (runtimeLayer, error) {
	parts := strings.SplitN(v, "=", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return runtimeLayer{}, fmt.Errorf("--layer %q: want name=image@sha256:...=/mount/path", v)
	}
	l := runtimeLayer{name: parts[0], image: parts[1], mountPath: parts[2]}
	if l.name == apiservice.GuestVolumeName {
		return runtimeLayer{}, fmt.Errorf("--layer %q: the name %q is reserved for the guest", v, apiservice.GuestVolumeName)
	}
	if _, err := apiservice.ImageDigest(l.image); err != nil {
		return runtimeLayer{}, fmt.Errorf("--layer %q: %w", l.name, err)
	}
	if !strings.HasPrefix(l.mountPath, "/") || l.mountPath == apiservice.GuestMountPath {
		return runtimeLayer{}, fmt.Errorf("--layer %q: mount path must be absolute and not %s", l.name, apiservice.GuestMountPath)
	}
	return l, nil
}

// parseLayers parses and cross-checks every --layer value.
func parseLayers(values []string) ([]runtimeLayer, error) {
	var layers []runtimeLayer
	names, mounts := map[string]bool{}, map[string]bool{}
	for _, v := range values {
		l, err := parseLayer(v)
		if err != nil {
			return nil, err
		}
		if names[l.name] || mounts[l.mountPath] {
			return nil, fmt.Errorf("--layer %q: duplicate name or mount path", v)
		}
		names[l.name], mounts[l.mountPath] = true, true
		layers = append(layers, l)
	}
	return layers, nil
}

// Bounds on --cpu and --memory. Substrate v0.6.0 carries the limits as
// integer millicores and bytes and requires only that each is non-zero, so
// these are env's own: the minimum keeps a typo such as 512m (millibytes)
// from rendering as one byte, and the maximum keeps the integers in range.
var (
	minCPULimit    = resource.MustParse("1m")
	maxCPULimit    = resource.MustParse("999.999")
	minMemoryLimit = resource.MustParse("1Mi")
	maxMemoryLimit = resource.MustParse("9223372036854775807") // an int64 byte count
)

// validateResources checks --cpu and --memory the way Substrate validates
// resources.limits. An empty value means the default.
func (c *templateConfig) validateResources() error {
	for _, l := range []struct {
		flag, value string
		min, max    resource.Quantity
	}{{"--cpu", c.cpu, minCPULimit, maxCPULimit}, {"--memory", c.memory, minMemoryLimit, maxMemoryLimit}} {
		if l.value == "" {
			continue
		}
		q, err := resource.ParseQuantity(l.value)
		if err != nil {
			return fmt.Errorf("%s %q is not a Kubernetes quantity (e.g. 2, 500m, 4Gi): %w", l.flag, l.value, err)
		}
		if q.Cmp(l.min) < 0 {
			return fmt.Errorf("%s %q is below %s, the smallest limit", l.flag, l.value, l.min.String())
		}
		if q.Cmp(l.max) > 0 {
			return fmt.Errorf("%s %q is above the maximum of %s", l.flag, l.value, l.max.String())
		}
	}
	return nil
}

// validateEnv checks the --env and --sidecar-env flags before anything is
// built.
func (c *templateConfig) validateEnv() error {
	_, err := c.containerEnv()
	return err
}

// containerEnv returns the container env vars of --env and --sidecar-env.
// A --sidecar-env NAME becomes sidecarenv.Prefix+NAME. A name in both flags
// is an error, because the sidecar would get two values; so is --env with the
// prefix, because the guest would take it from the commands it starts, and
// --sidecar-env without --sidecar, because nothing would get it.
func (c *templateConfig) containerEnv() ([]*ateapipb.EnvVar, error) {
	env, err := parseEnv("--env", false, c.env)
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for _, e := range env {
		if strings.HasPrefix(e.GetName(), sidecarenv.Prefix) {
			return nil, fmt.Errorf("--env %s: the prefix %s is reserved for --sidecar-env", e.GetName(), sidecarenv.Prefix)
		}
		names[e.GetName()] = true
	}
	sidecarEnv, err := parseEnv("--sidecar-env", true, c.sidecarEnv)
	if err != nil {
		return nil, err
	}
	if len(sidecarEnv) > 0 && len(c.sidecars) == 0 {
		return nil, errors.New("--sidecar-env needs a --sidecar to give it to")
	}
	for _, e := range sidecarEnv {
		if names[e.GetName()] {
			return nil, fmt.Errorf("%s is given with both --env and --sidecar-env", e.GetName())
		}
		env = append(env, &ateapipb.EnvVar{Name: sidecarenv.Prefix + e.GetName(), Value: e.GetValue()})
	}
	return env, nil
}

func (c *templateConfig) resolveImages() error {
	if c.guestImage == "" {
		return errors.New(`--guest-image is required when ate-env is built from source; release
binaries default it to the ate-env-guest image published with the release
(see docs/release.md), or build and push your own with make images`)
	}
	if c.rootfsImage != "" {
		if _, err := apiservice.ImageDigest(c.rootfsImage); err != nil {
			return fmt.Errorf("--rootfs-image: %w", err)
		}
		if _, err := apiservice.ImageDigest(c.guestImage); err != nil {
			return fmt.Errorf("--guest-image must be digest-pinned to be mounted as an image volume: %w", err)
		}
	}
	if _, err := parseLayers(c.layers); err != nil {
		return err
	}
	for _, s := range c.sidecars {
		argv := strings.Fields(s)
		if len(argv) == 0 || !strings.HasPrefix(argv[0], "/") {
			return fmt.Errorf("--sidecar %q: want an absolute binary path followed by its arguments", s)
		}
	}
	for _, u := range c.sidecarReadyz {
		if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
			return fmt.Errorf("--sidecar-readyz %q: want an http(s) URL", u)
		}
	}
	return nil
}

func newManifestCommand() *cobra.Command {
	mCfg := manifestConfig{}

	cmd := &cobra.Command{
		Use:   "manifest",
		Short: "Generate Kubernetes manifests to deploy the system",
		Long: `Manifest generates Kubernetes manifests for everything environments need on
a cluster that already runs the Agent Substrate system: the target
namespace, a WorkerPool of pre-warmed workers, and the ate-env-api service.
It prints YAML to stdout without touching the cluster; apply it with kubectl.

ate-env-api requires a bearer token from its callers, read from the Secret
--auth-token-secret names (key token). Create that Secret once, in the
namespace this manifest creates, with
"ate-env manifest auth-secret | kubectl create -f -"; ate-env-api starts when
the Secret exists. --insecure-no-auth deploys without the token.

To generate the Substrate ActorTemplate manifest (for kubectl-ate), use
the "template" subcommand: ate-env manifest template`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := mCfg.resolveAuth(cmd.Flags().Changed("auth-token-secret")); err != nil {
				return err
			}
			if mCfg.template == "" {
				mCfg.template = apiservice.DefaultTemplate
			}
			if mCfg.workerPool == "" {
				mCfg.workerPool = defaultWorkerPool(mCfg.template)
			}
			if err := mCfg.validateTelemetry(); err != nil {
				return err
			}
			if err := checkWorkerPoolName(mCfg.workerPool); err != nil {
				return err
			}
			if err := mCfg.resolveImages(); err != nil {
				return err
			}
			if _, err := hostcheck.New(mCfg.allowedHosts); err != nil {
				return fmt.Errorf("--api-allowed-hosts: %w", err)
			}
			if err := writeManifests(cmd.OutOrStdout(), buildManifests(mCfg)); err != nil {
				return err
			}
			if mCfg.routerTLS {
				fmt.Fprintf(cmd.ErrOrStderr(), "note: ate-env-api presents %s to the atenet router; install Substrate with that ID in the router's ingress allow-list (ate-setup: ATE_INGRESS_ALLOWED_SPIFFE_IDS)\n",
					routertls.SPIFFEID(mCfg.namespace, apiName))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&mCfg.namespace, "namespace", apiservice.DefaultNamespace, "Kubernetes namespace to deploy into")
	cmd.Flags().StringVar(&mCfg.template, "template", apiservice.DefaultTemplate, "ActorTemplate name")
	cmd.Flags().StringVar(&mCfg.workerImage, "worker-image", "", "digest-pinned worker image for the worker pool, e.g. ateworker-gvisor built from the Substrate repo")
	cmd.Flags().StringVar(&mCfg.workerImage, "ateom-image", "", "alias for --worker-image, kept from before Substrate renamed ateom to ateworker")
	cmd.Flags().StringVar(&mCfg.apiImage, "api-image", defaultAPIImage, imageDefaultHelp("digest-pinned ate-env-api image for the API service", defaultAPIImage))
	cmd.Flags().Int32Var(&mCfg.apiReplicas, "api-replicas", 1, "number of API service replicas")
	cmd.Flags().Int32Var(&mCfg.apiPort, "api-port", 7777, "port the ate-env-api service listens on")
	cmd.Flags().StringVar(&mCfg.authTokenSecret, "auth-token-secret", defaultAuthTokenSecret, "Kubernetes Secret (key 'token') mounted into ate-env-api; its value is the bearer token ate-env-api requires from callers. Create it with 'ate-env manifest auth-secret'")
	cmd.Flags().BoolVar(&mCfg.insecureNoAuth, "insecure-no-auth", false, "deploy ate-env-api without a caller token: anyone who can reach its Service can create environments, run commands in them and reach their ports")
	cmd.Flags().StringVar(&mCfg.portProxyAtespaces, "port-proxy-atespaces", "", "comma-separated atespaces whose environments ate-env-api's port proxy forwards to, or * for any (defaults to ate-env)")
	cmd.Flags().StringSliceVar(&mCfg.allowedHosts, "api-allowed-hosts", nil, "extra host names that HTTP clients may use to reach ate-env-api, e.g. an internal load balancer name (a leading *. allows the names below it); the Service's in-cluster names, in the cluster's own DNS domain, IP addresses and localhost are always allowed")
	cmd.Flags().BoolVar(&mCfg.routerTLS, "api-router-tls", true, "mount the pod identity certificate and the servicedns trust bundle into ate-env-api and dial the atenet router's TLS listeners, the only ones Substrate serves since v0.6.0; the API's SPIFFE ID must then be in the router's --ingress-allowed-spiffe-ids. Off, the API dials the plaintext addresses, which only the test fakes serve")
	cmd.Flags().StringVar(&mCfg.workerPool, "workerpool", "", "WorkerPool name (defaults to <template>-workerpool)")
	cmd.Flags().Int32Var(&mCfg.replicas, "replicas", 5, "number of pre-warmed worker pods")
	cmd.Flags().BoolVar(&mCfg.enableTracing, "enable-tracing", false, "run ate-env-api with --enable-tracing, exporting OpenTelemetry traces over OTLP/gRPC")
	cmd.Flags().StringVar(&mCfg.otlpEndpoint, "otlp-endpoint", "", "OTLP/gRPC collector endpoint for ate-env-api's traces, set as OTEL_EXPORTER_OTLP_ENDPOINT, e.g. http://otel-collector.observability:4317")
	cmd.Flags().BoolVar(&mCfg.otlpInsecure, "otlp-insecure", false, "set OTEL_EXPORTER_OTLP_INSECURE=true: talk to the collector in plaintext")
	cmd.Flags().Int32Var(&mCfg.metricsPort, "metrics-port", 0, "port ate-env-api serves Prometheus metrics on (--metrics-listen-addr), exposed on the container and the Service as \"metrics\" with prometheus.io scrape annotations; 0 leaves metrics off. The metrics listener has no authentication and no Host check")

	cmd.AddCommand(newManifestTemplateCommand())
	cmd.AddCommand(newManifestAccessPolicyCommand())
	cmd.AddCommand(newManifestAuthSecretCommand())

	return cmd
}

func newManifestAuthSecretCommand() *cobra.Command {
	namespace := apiservice.DefaultNamespace
	name := defaultAuthTokenSecret
	cmd := &cobra.Command{
		Use:   "auth-secret",
		Short: "Generate the Secret with the bearer token ate-env-api requires from callers",
		Long: `Auth-secret prints a Kubernetes Secret whose key token holds a new random
bearer token (32 bytes, hex). ate-env manifest mounts it into ate-env-api, which
then refuses gRPC, MCP and port-proxy calls without it. Each run prints a new
token, thus create the Secret once and do not apply it again:

  ate-env manifest auth-secret | kubectl create -f -

Callers read the token back from the Secret:

  export SUBSTRATE_ENV_AUTH_TOKEN="$(kubectl -n ate-env get secret ate-env-api-token -o jsonpath='{.data.token}' | base64 -d)"

The ate-env CLI and swebench-loader read SUBSTRATE_ENV_AUTH_TOKEN (or
--auth-token-file); the Python client takes Client(auth_token=...) or
FleetConfig(auth_token=...). To rotate the token, replace the Secret's value;
ate-env-api reads the new value once the kubelet updates the mounted file.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace = strings.TrimSpace(namespace)
			name = strings.TrimSpace(name)
			if namespace == "" {
				return errors.New("--namespace is empty")
			}
			if name == "" {
				return errors.New("--name is empty")
			}
			token, err := newAuthToken()
			if err != nil {
				return err
			}
			return writeManifests(cmd.OutOrStdout(), []any{buildAuthSecret(namespace, name, token)})
		},
	}
	cmd.Flags().StringVar(&namespace, "namespace", namespace, "Kubernetes namespace ate-env-api runs in")
	cmd.Flags().StringVar(&name, "name", name, "Secret name; pass the same value to ate-env manifest --auth-token-secret")
	return cmd
}

// newAuthToken returns a random caller token: authTokenBytes from
// crypto/rand, hex encoded.
func newAuthToken() (string, error) {
	b := make([]byte, authTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating the token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// buildAuthSecret returns the Secret that holds ate-env-api's caller token.
func buildAuthSecret(namespace, name, token string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": apiName},
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{authTokenSecretKey: token},
	}
}

func newManifestAccessPolicyCommand() *cobra.Command {
	namespace := apiservice.DefaultNamespace
	var global bool
	cmd := &cobra.Command{
		Use:   "access-policy",
		Short: "Generate the Substrate access policy that lets ate-env-api manage environments",
		Long: `Access-policy prints a Substrate access policy, in the form kubectl-ate reads,
that grants ate-env-api's ServiceAccount the editor role on an atespace.

Substrate enforces authorization by default: ate-env-api then needs editor on
each atespace it creates environments in, and the atespace must exist, because
creating one takes a global owner. As an owner of the atespace (the default
kubectl-ate identity on a stock install), run once for each atespace:

  kubectl ate create atespace ate-env
  ate-env manifest access-policy | kubectl ate create access-policy -a ate-env -f -

An atespace has one policy. If it already has one, add the binding to it with
"kubectl ate get access-policy" and "kubectl ate update access-policy".

With --global the policy grants the global viewer role instead, which
ListPlacementDomains needs to read the workers (and WarmImage to count a
domain's nodes when the request leaves nodes 0). The global policy is one
object for the whole control plane and it holds the owners: merge this binding
into it with "kubectl ate get access-policy" and "kubectl ate update
access-policy"; do not create it over an existing one.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if global {
				return writeProtoYAML(cmd.OutOrStdout(), buildGlobalAccessPolicy(namespace), "global access policy")
			}
			return writeProtoYAML(cmd.OutOrStdout(), buildAccessPolicy(namespace), "access policy")
		},
	}
	cmd.Flags().StringVar(&namespace, "namespace", namespace, "Kubernetes namespace ate-env-api runs in (its ServiceAccount's namespace)")
	cmd.Flags().BoolVar(&global, "global", false, "print the global policy binding (viewer) that lets ate-env-api list workers for ListPlacementDomains")
	return cmd
}

func newManifestTemplateCommand() *cobra.Command {
	tCfg := templateConfig{}

	cmd := &cobra.Command{
		Use:   "template",
		Short: "Generate Substrate ActorTemplate manifest",
		Long: `Generate Substrate ActorTemplate manifest to register with kubectl-ate.
It prints YAML to stdout without touching the cluster.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if tCfg.template == "" {
				tCfg.template = apiservice.DefaultTemplate
			}
			if tCfg.workerPool == "" {
				tCfg.workerPool = defaultTemplatePool
			}
			if err := checkWorkerPoolName(tCfg.workerPool); err != nil {
				return err
			}
			if err := tCfg.resolveImages(); err != nil {
				return err
			}
			// An empty --cpu or --memory would silently mean the default.
			for _, f := range []string{"cpu", "memory"} {
				if cmd.Flags().Changed(f) && cmd.Flags().Lookup(f).Value.String() == "" {
					return fmt.Errorf("--%s must not be empty; omit it for the default", f)
				}
			}
			if err := tCfg.validateResources(); err != nil {
				return err
			}
			if err := tCfg.validateWakeupTimeout(); err != nil {
				return err
			}
			if err := tCfg.validateEnv(); err != nil {
				return err
			}
			tmpl, err := buildTemplate(tCfg)
			if err != nil {
				return err
			}
			return writeActorTemplate(cmd.OutOrStdout(), tmpl)
		},
	}

	cmd.Flags().StringVar(&tCfg.template, "template", apiservice.DefaultTemplate, "ActorTemplate name")
	cmd.Flags().StringVar(&tCfg.workerPool, "workerpool", "", "WorkerPool the actors run on, selected by its "+workerPoolLabel+" label (defaults to "+defaultTemplatePool+", the pool ate-env manifest creates by default)")
	cmd.Flags().StringVar(&tCfg.atespace, "atespace", apiservice.DefaultAtespace, "Substrate atespace for the ActorTemplate")
	cmd.Flags().StringVar(&tCfg.guestImage, "guest-image", defaultGuestImage, imageDefaultHelp("digest-pinned ate-env-guest image (repo@sha256:...)", defaultGuestImage))
	cmd.Flags().StringSliceVar(&tCfg.guestCommand, "guest-command", []string{"/ko-app/ate-env-guest"}, "guest container entrypoint")
	cmd.Flags().StringVar(&tCfg.snapshotsBucket, "snapshots-bucket", "", "object-storage location for this template's snapshots, e.g. gs://bucket/prefix/; empty (the default) stores them under the cluster's snapshot location. A value needs Substrate's PerTemplateSnapshotLocation preview gate and kubectl-ate --preview to create the template")
	cmd.Flags().StringVar(&tCfg.rootfsImage, "rootfs-image", "", "digest-pinned image to run unmodified; the guest is mounted into it as a read-only image volume at "+apiservice.GuestMountPath)
	cmd.Flags().StringVar(&tCfg.workspace, "workspace", "", "workspace root inside the actor, passed to the guest as -workspace (default: the guest's default)")
	cmd.Flags().StringArrayVar(&tCfg.layers, "layer", nil, "extra runtime mounted as a read-only image volume, as name=image@sha256:...=/mount/path (repeatable)")
	cmd.Flags().StringArrayVar(&tCfg.sidecars, "sidecar", nil, "command the guest starts and keeps running next to itself, an absolute binary path plus arguments, e.g. '/opt/opensandbox/execd --port 44772' (repeatable)")
	cmd.Flags().StringArrayVar(&tCfg.sidecarReadyz, "sidecar-readyz", nil, "URL the actor's readiness waits for, e.g. http://127.0.0.1:44772/ready (repeatable)")
	cmd.Flags().StringVar(&tCfg.cpu, "cpu", defaultActorCPU, "CPU limit of each actor, a Kubernetes quantity such as 2 or 500m, at least 1m, rendered as whole millicores (rounded up); Substrate requires one")
	cmd.Flags().StringVar(&tCfg.memory, "memory", defaultActorMemory, "memory limit of each actor, a Kubernetes quantity such as 4Gi, at least 1Mi, rendered as whole bytes (rounded up); Substrate requires one")
	cmd.Flags().DurationVar(&tCfg.wakeupTimeout, "wakeup-timeout", 0, fmt.Sprintf("how long Substrate polls the guest's /readyz at each actor start and resume before it fails the actor, whole seconds from 1s to %s; 0 or unset means the default: %s with --sidecar-readyz, else Substrate's %s", maxWakeupTimeout, sidecarWakeupTimeout, substrateWakeupTimeout))
	cmd.Flags().StringArrayVar(&tCfg.env, "env", nil, "environment variable for the actor's container as NAME=VALUE (repeatable); the guest passes it to sidecars and to each command it starts, e.g. a layer daemon's log level; use --sidecar-env for a secret")
	cmd.Flags().StringArrayVar(&tCfg.sidecarEnv, "sidecar-env", nil, fmt.Sprintf("environment variable that only the --sidecar processes get, as NAME=VALUE (repeatable), e.g. a layer daemon's API key; the container carries it as %sNAME and the guest strips it before it starts anything. Limits: docs/security/threat-model.md", sidecarenv.Prefix))

	return cmd
}

// buildManifests returns the Kubernetes objects that make up a deployment,
// in apply order.
func buildManifests(cfg manifestConfig) []any {
	return []any{
		buildNamespace(cfg),
		buildWorkerPool(cfg),
		buildAPIServiceAccount(cfg),
		buildAPIDeployment(cfg),
		buildAPIService(cfg),
	}
}

// apiPrincipal is the identity ate-env-api presents to the Substrate control
// plane: its own ServiceAccount, through a projected token. Access policies
// name it with a "user:" prefix (see the access-policy subcommand).
func apiPrincipal(namespace string) string {
	return "system:serviceaccount:" + namespace + ":" + apiName
}

// buildAPIServiceAccount returns the ServiceAccount ate-env-api runs as. A
// dedicated account gives the control plane a stable principal to authorize;
// the namespace's default account would be shared with anything else there.
// ate-env-api never calls the Kubernetes API, thus no API token is mounted.
func buildAPIServiceAccount(cfg manifestConfig) *corev1.ServiceAccount {
	automount := false
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      apiName,
			Namespace: cfg.namespace,
			Labels:    map[string]string{"app": apiName},
		},
		AutomountServiceAccountToken: &automount,
	}
}

// buildAccessPolicy returns the Substrate access policy that lets ate-env-api
// manage environments in one atespace: the editor role creates, suspends and
// deletes actors, and creates the per-image templates it derives.
func buildAccessPolicy(namespace string) *ateapipb.AccessPolicy {
	return &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{
			Role:    "editor",
			Members: []string{"user:" + apiPrincipal(namespace)},
		}},
	}
}

// buildGlobalAccessPolicy returns the global Substrate access policy binding
// that lets ate-env-api list workers (ListPlacementDomains, and WarmImage
// with nodes 0): the viewer role at the global scope, whose policy has no
// atespace.
func buildGlobalAccessPolicy(namespace string) *ateapipb.AccessPolicy {
	return &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{
			Role:    "viewer",
			Members: []string{"user:" + apiPrincipal(namespace)},
		}},
	}
}

// writeActorTemplate writes a single Substrate ActorTemplate as protojson-shaped YAML.
func writeActorTemplate(w io.Writer, tmpl *ateapipb.ActorTemplate) error {
	return writeProtoYAML(w, tmpl, "actor template")
}

// writeProtoYAML writes a Substrate control-plane message as protojson-shaped
// YAML, the form kubectl-ate reads, without its server-managed status.
func writeProtoYAML(w io.Writer, msg proto.Message, what string) error {
	opts := protojson.MarshalOptions{
		UseProtoNames: false,
	}
	jsonBytes, err := opts.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encoding %s json: %w", what, err)
	}
	var m map[string]any
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return fmt.Errorf("decoding %s json: %w", what, err)
	}
	delete(m, "status")
	prune(m)
	data, err := yaml.Marshal(m)
	if err != nil {
		return fmt.Errorf("encoding %s yaml: %w", what, err)
	}
	_, err = w.Write(data)
	return err
}

// writeManifests writes the objects as a multi-document YAML stream.
func writeManifests(w io.Writer, objs []any) error {
	for i, obj := range objs {
		var jsonBytes []byte
		var err error
		if m, ok := obj.(proto.Message); ok {
			jsonBytes, err = protojson.Marshal(m)
		} else {
			jsonBytes, err = json.Marshal(obj)
		}
		if err != nil {
			return fmt.Errorf("encoding json: %w", err)
		}
		var m map[string]any
		if err := json.Unmarshal(jsonBytes, &m); err != nil {
			return fmt.Errorf("decoding json: %w", err)
		}
		delete(m, "status")
		prune(m)
		data, err := yaml.Marshal(m)
		if err != nil {
			return fmt.Errorf("encoding manifest: %w", err)
		}
		if i > 0 {
			if _, err := io.WriteString(w, "---\n"); err != nil {
				return err
			}
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func prune(v any) any {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			cleaned := prune(child)
			if cleaned == nil {
				delete(val, k)
				continue
			}
			if m, ok := cleaned.(map[string]any); ok && len(m) == 0 {
				delete(val, k)
				continue
			}
			val[k] = cleaned
		}
		if len(val) == 0 {
			return nil
		}
		return val
	case []any:
		for i, elem := range val {
			val[i] = prune(elem)
		}
		return val
	default:
		return val
	}
}

// apiAccessLabel is the namespace label Substrate's network policy requires of
// in-cluster API clients: ate-system denies ingress by default and admits
// non-worker pods from namespaces carrying it (substrate #2522). Without it
// ate-env-api cannot reach ateapi and crash-loops on its startup atespace check.
const apiAccessLabel = "ate.dev/api-access"

func buildNamespace(cfg manifestConfig) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name:   cfg.namespace,
			Labels: map[string]string{apiAccessLabel: "true"},
		},
	}
}

func buildWorkerPool(cfg manifestConfig) *atev1.WorkerPool {
	return &atev1.WorkerPool{
		TypeMeta: metav1.TypeMeta{
			APIVersion: atev1.GroupVersion.String(),
			Kind:       "WorkerPool",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.workerPool,
			Namespace: cfg.namespace,
			// Copied to the pool's workers; templates select the pool by it.
			Labels: map[string]string{workerPoolLabel: cfg.workerPool},
		},
		Spec: atev1.WorkerPoolSpec{
			Replicas:    cfg.replicas,
			WorkerImage: cfg.workerImage,
			// The CRD requires exactly one sandbox class per pool. Since
			// substrate#2780 the pool alone decides the sandbox: the
			// ActorTemplate names none, and an actor restores onto a worker of
			// its snapshot's class. The configRef is the SandboxConfig the
			// workers run.
			SandboxClasses: []atev1.WorkerPoolSandboxClass{{
				Name:      atev1.SandboxClassGvisor,
				ConfigRef: &atev1.SandboxConfigReference{Name: sandboxConfigName},
			}},
		},
	}
}

func buildActorTemplate(cfg templateConfig) *ateapipb.ActorTemplate {
	atespace := cfg.atespace
	if atespace == "" {
		atespace = apiservice.DefaultAtespace
	}
	pool := cfg.workerPool
	if pool == "" {
		pool = defaultTemplatePool
	}
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Name:     cfg.template,
			Atespace: atespace,
		},
		// Pin the actors to their pool, thus to its sandbox class: the
		// template names no class since substrate#2780.
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{workerPoolLabel: pool}},
		Containers: []*ateapipb.Container{{
			Name:    "guest",
			Image:   cfg.guestImage,
			Command: cfg.guestCommand,
			Env: []*ateapipb.EnvVar{{
				Name:  "PORT",
				Value: "80",
			}},
			WakeupProbe: &ateapipb.WakeupProbe{
				HttpGet: &ateapipb.HttpGetAction{
					Path: "/readyz",
					Port: 80,
				},
				TimeoutSeconds: cfg.wakeupTimeoutSeconds(),
			},
		}},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			// Empty means the cluster's --snapshot-location. Substrate rejects a
			// value unless its PerTemplateSnapshotLocation preview gate is on.
			StorageLocation: cfg.snapshotsBucket,
			// Environments must come back from suspend with their process
			// state intact. MEMORY is also the server default, but the API
			// schema marks the field required, so say it explicitly.
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		Resources: resourceLimits(orDefault(cfg.cpu, defaultActorCPU), orDefault(cfg.memory, defaultActorMemory)),
	}
}

// resourceLimits renders the cpu and memory quantities as the integer limits
// Substrate carries since v0.6.0: millicores and bytes, rounded up, so a
// fraction of either becomes one. The flags were checked by
// validateResources, and the defaults are constants, so the parse cannot
// fail here.
func resourceLimits(cpu, memory string) *ateapipb.ResourceRequirements {
	c, m := resource.MustParse(cpu), resource.MustParse(memory)
	return &ateapipb.ResourceRequirements{Limits: &ateapipb.Resources{
		CpuMillicores: c.MilliValue(),
		MemoryBytes:   m.Value(),
	}}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// buildTemplate returns the ActorTemplate for cfg: the guest template from
// buildActorTemplate with the workspace flag applied and, when a rootfs image is
// set, that image as the container with the guest mounted into it.
func buildTemplate(cfg templateConfig) (*ateapipb.ActorTemplate, error) {
	tmpl := buildActorTemplate(cfg)
	c := tmpl.Containers[0]
	env, err := cfg.containerEnv()
	if err != nil {
		return nil, err
	}
	c.Env = append(c.Env, env...)
	command := append([]string(nil), c.Command...)
	if cfg.workspace != "" {
		command = append(command, "-workspace", cfg.workspace)
	}
	// Extra runtimes: each layer is an image volume, each sidecar a flag the
	// guest acts on. They are added before any rootfs-image derivation so the
	// derived template inherits them, and so do environments created from
	// it with an image of their own.
	layers, err := parseLayers(cfg.layers)
	if err != nil {
		return nil, err
	}
	for _, l := range layers {
		tmpl.Volumes = append(tmpl.Volumes, &ateapipb.Volume{
			Name:  l.name,
			Image: &ateapipb.ImageVolumeSource{Reference: l.image},
		})
		c.VolumeMounts = append(c.VolumeMounts, &ateapipb.VolumeMount{Name: l.name, MountPath: l.mountPath})
	}
	for _, s := range cfg.sidecars {
		command = append(command, "-sidecar", s)
	}
	for _, u := range cfg.sidecarReadyz {
		command = append(command, "-sidecar-readyz", u)
	}
	// An older guest does not know the sidecar-only prefix and would start
	// the sidecars without those values. This flag makes it exit instead.
	if len(cfg.sidecarEnv) > 0 {
		command = append(command, "-"+sidecarenv.RequiredFlag)
	}
	c.Command = command
	if cfg.rootfsImage == "" {
		return tmpl, nil
	}
	return apiservice.DeriveImageTemplate(tmpl, cfg.template, cfg.rootfsImage)
}

// buildAPIDeployment returns the ate-env-api Deployment, pointed at the
// in-cluster Substrate endpoints.
func buildAPIDeployment(cfg manifestConfig) *appsv1.Deployment {
	automountAPIToken := false
	labels := map[string]string{"app": apiName}
	replicas := cfg.apiReplicas
	tokenExpiration := int64(7200)
	args := []string{
		"-listen", fmt.Sprintf("0.0.0.0:%d", cfg.apiPort),
		"-ateapi-token-file=/var/run/secrets/ateapi/token",
		// ate-env-api allows the Service's in-cluster names itself: it reads
		// the namespace and the cluster domain from the pod's resolv.conf,
		// else the namespace from POD_NAMESPACE below.
		"-service-name=" + apiName,
	}
	if len(cfg.allowedHosts) > 0 {
		args = append(args, "-allowed-hosts="+strings.Join(cfg.allowedHosts, ","))
	}
	mounts := []corev1.VolumeMount{{
		Name:      "ateapi-token",
		MountPath: "/var/run/secrets/ateapi",
		ReadOnly:  true,
	}}
	volumes := []corev1.Volume{{
		Name: "ateapi-token",
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
						Audience:          "api.ate-system.svc",
						ExpirationSeconds: &tokenExpiration,
						Path:              "token",
					},
				}},
			},
		},
	}}
	if cfg.routerTLS {
		// The router's TLS listeners need the pod's identity certificate and
		// the CA of the router's own certificate; both are projected by the
		// kubelet and rotated in place (see internal/routertls).
		args = append(args,
			"-atenet-client-cert="+routerCredentialBundlePath,
			"-atenet-ca-file="+routerTrustBundlePath)
		mounts = append(mounts,
			corev1.VolumeMount{Name: "podidentity", MountPath: "/run/podidentity.podcert.ate.dev", ReadOnly: true},
			corev1.VolumeMount{Name: "servicedns-ca", MountPath: "/run/servicedns-ca", ReadOnly: true})
		servicednsSigner := "servicedns.podcert.ate.dev/identity"
		volumes = append(volumes,
			corev1.Volume{Name: "podidentity", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{PodCertificate: &corev1.PodCertificateProjection{
					SignerName:           "podidentity.podcert.ate.dev/identity",
					KeyType:              "ECDSAP256",
					CredentialBundlePath: "credential-bundle.pem",
				}}},
			}}},
			corev1.Volume{Name: "servicedns-ca", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{ClusterTrustBundle: &corev1.ClusterTrustBundleProjection{
					SignerName:    &servicednsSigner,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"podcert.ate.dev/canarying": "live"}},
					Path:          "trust-bundle.pem",
				}}},
			}}})
	}
	if cfg.authTokenSecret != "" {
		args = append(args, "-auth-token-file="+authTokenMountPath+"/"+authTokenSecretKey)
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "ate-env-auth",
			MountPath: authTokenMountPath,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "ate-env-auth",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cfg.authTokenSecret,
				},
			},
		})
	}
	if a := strings.TrimSpace(cfg.portProxyAtespaces); a != "" {
		args = append(args, "-port-proxy-atespaces="+a)
	}
	ports := []corev1.ContainerPort{{ContainerPort: cfg.apiPort}}
	var podAnnotations map[string]string
	if cfg.enableTracing {
		args = append(args, "-enable-tracing")
	}
	if cfg.metricsPort != 0 {
		args = append(args, fmt.Sprintf("-metrics-listen-addr=:%d", cfg.metricsPort))
		ports = append(ports, corev1.ContainerPort{Name: metricsPortName, ContainerPort: cfg.metricsPort})
		podAnnotations = map[string]string{
			"prometheus.io/scrape": "true",
			"prometheus.io/port":   fmt.Sprint(cfg.metricsPort),
			"prometheus.io/path":   "/metrics",
		}
	}
	// POD_NAMESPACE lets ate-env-api name its own Service for the Host
	// check when /etc/resolv.conf has no search list.
	env := []corev1.EnvVar{{
		Name: "POD_NAMESPACE",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		},
	}}
	if cfg.otlpEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: cfg.otlpEndpoint})
	}
	if cfg.otlpInsecure {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_INSECURE", Value: "true"})
	}
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      apiName,
			Namespace: cfg.namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					// The ServiceAccount is the principal the control plane
					// authorizes (buildAccessPolicy); the projected ateapi token
					// below is minted for it.
					ServiceAccountName:           apiName,
					AutomountServiceAccountToken: &automountAPIToken,
					Containers: []corev1.Container{{
						Name:  apiName,
						Image: cfg.apiImage,
						// ateapi/atenet default to the in-cluster
						// Substrate service addresses.
						Args:         args,
						Env:          env,
						Ports:        ports,
						VolumeMounts: mounts,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt32(cfg.apiPort),
								},
							},
						},
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

func buildAPIService(cfg manifestConfig) *corev1.Service {
	labels := map[string]string{"app": apiName}
	ports := []corev1.ServicePort{{
		Port:       cfg.apiPort,
		TargetPort: intstr.FromInt32(cfg.apiPort),
	}}
	if cfg.metricsPort != 0 {
		// A Service with several ports must name each.
		ports[0].Name = "api"
		ports = append(ports, corev1.ServicePort{
			Name:       metricsPortName,
			Port:       cfg.metricsPort,
			TargetPort: intstr.FromString(metricsPortName),
		})
	}
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      apiName,
			Namespace: cfg.namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    ports,
		},
	}
}

// envName is a POSIX environment variable name.
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// parseEnv turns the NAME=VALUE values of flagName into env vars. PORT is the
// guest's own and cannot be overridden; a name given twice is an error. With
// secret, a malformed value is named by its position, not by its text, because
// the value can be the secret itself.
func parseEnv(flagName string, secret bool, flags []string) ([]*ateapipb.EnvVar, error) {
	var out []*ateapipb.EnvVar
	seen := map[string]bool{}
	for i, f := range flags {
		name, value, ok := strings.Cut(f, "=")
		if !ok || !envName.MatchString(name) {
			if secret {
				return nil, fmt.Errorf("%s value %d: want NAME=VALUE with NAME of letters, digits and '_', not starting with a digit", flagName, i+1)
			}
			return nil, fmt.Errorf("%s %q: want NAME=VALUE with NAME of letters, digits and '_', not starting with a digit", flagName, f)
		}
		if name == "PORT" {
			return nil, fmt.Errorf("%s PORT: PORT is the guest's listen port and is set by the template", flagName)
		}
		if seen[name] {
			return nil, fmt.Errorf("%s %s is given twice", flagName, name)
		}
		seen[name] = true
		out = append(out, &ateapipb.EnvVar{Name: name, Value: value})
	}
	return out, nil
}
