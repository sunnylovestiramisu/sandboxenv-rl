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

// Command ate-env is a CLI for the environment service. Environment commands go
// through the ate-env-api service using the env SDK; manifest generates
// Kubernetes manifests for setting up the system on a cluster.
//
// The API endpoint can be set with the --api flag or the
// SUBSTRATE_ENV_API environment variable.
package main

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/internal/apiservice"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// authFlags are the caller-token flags of the commands that call
// ate-env-api.
type authFlags struct {
	token string
	file  string
}

func (a *authFlags) register(cmd *cobra.Command, persistent bool) {
	fs := cmd.Flags()
	if persistent {
		fs = cmd.PersistentFlags()
	}
	// The default stays empty: cobra prints a flag default in --help, and a
	// default read from SUBSTRATE_ENV_AUTH_TOKEN would print the token.
	// resolve reads the variable at run time.
	fs.StringVar(&a.token, "auth-token", "", "bearer token for ate-env-api (default: $"+authTokenEnv+"); the value shows in ps output and shell history, thus prefer "+authTokenEnv+" or --auth-token-file")
	fs.StringVar(&a.file, "auth-token-file", "", "file that holds the bearer token for ate-env-api; surrounding whitespace is ignored")
}

// authTokenEnv holds the caller token when neither flag is set.
const authTokenEnv = "SUBSTRATE_ENV_AUTH_TOKEN"

// resolve returns the token from --auth-token-file when it is set, otherwise
// from --auth-token, otherwise from SUBSTRATE_ENV_AUTH_TOKEN. The two flags
// exclude each other; each flag takes precedence over the environment
// variable.
func (a *authFlags) resolve(cmd *cobra.Command) (string, error) {
	tokenSet := cmd.Flags().Changed("auth-token")
	if a.file == "" {
		if tokenSet {
			return a.token, nil
		}
		return os.Getenv(authTokenEnv), nil
	}
	if tokenSet {
		return "", errors.New("--auth-token and --auth-token-file cannot be used together")
	}
	b, err := os.ReadFile(a.file)
	if err != nil {
		return "", fmt.Errorf("reading --auth-token-file: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("--auth-token-file %s is empty", a.file)
	}
	return token, nil
}

func newGuestCommand(id string) *cobra.Command {
	var (
		endpoint string
		atespace string
		auth     authFlags
		client   *env.Client
	)

	guestCmd := &cobra.Command{
		Use:   id,
		Short: fmt.Sprintf("Manage environment %s", id),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err = env.NewClient(env.ClientOptions{
				Endpoint:  endpoint,
				AuthToken: token,
			})
			return err
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			if client != nil {
				client.Close()
			}
		},
	}
	guestCmd.PersistentFlags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	guestCmd.PersistentFlags().StringVar(&atespace, "atespace", apiservice.DefaultAtespace, "Substrate atespace")
	auth.register(guestCmd, true)

	guestCmd.AddCommand(&cobra.Command{
		Use:   "read <path>",
		Short: "Print an environment file to stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rc, err := client.Env(atespace, id).ReadFile(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			defer rc.Close()
			_, err = io.Copy(os.Stdout, rc)
			return err
		},
	})

	guestCmd.AddCommand(&cobra.Command{
		Use:   "write <path>",
		Short: "Write stdin to an environment file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return client.Env(atespace, id).WriteFile(cmd.Context(), args[0], os.Stdin, 0o644)
		},
	})

	var (
		shellStdin   bool
		shellTimeout time.Duration
	)
	shellCmd := &cobra.Command{
		Use:     "shell <cmdline>",
		Aliases: []string{"cmd"},
		Short:   "Run a shell command line in the environment",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := env.ShellRequest{Command: strings.Join(args, " "), Timeout: shellTimeout}
			if shellStdin {
				data, err := io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("reading stdin: %w", err)
				}
				req.Stdin = data
			}
			res, err := client.Env(atespace, id).Run(cmd.Context(), req)
			if err != nil {
				return err
			}
			if out := strings.Trim(res.Stdout, "\r\n"); out != "" {
				fmt.Println(out)
			}
			if errOut := strings.Trim(res.Stderr, "\r\n"); errOut != "" {
				fmt.Fprintln(os.Stderr, errOut)
			}
			if res.ExitCode != 0 {
				os.Exit(res.ExitCode)
			}
			return nil
		},
	}
	shellCmd.Flags().BoolVarP(&shellStdin, "stdin", "i", false, "feed this process's stdin to the command")
	shellCmd.Flags().DurationVar(&shellTimeout, "timeout", 0, "kill the command after this duration (default: server default)")
	guestCmd.AddCommand(shellCmd)

	return guestCmd
}

func newCreateCommand() *cobra.Command {
	var (
		endpoint               string
		atespace               string
		auth                   authFlags
		createTemplate         string
		createTemplateAtespace string
		rootfsImage            string
		workerSelector         []string
	)
	cmd := &cobra.Command{
		Use:   "create <id>",
		Short: "Create and start an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkNotSubcommand(cmd, args[0]); err != nil {
				return err
			}
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{
				Endpoint:  endpoint,
				AuthToken: token,
			})
			if err != nil {
				return err
			}
			defer client.Close()

			selector, err := parseWorkerSelector(workerSelector)
			if err != nil {
				return err
			}
			req := &ateenvv1alpha.CreateEnvironmentRequest{
				Id:             args[0],
				Atespace:       atespace,
				RootfsImage:    rootfsImage,
				WorkerSelector: selector,
			}
			if createTemplate != "" || createTemplateAtespace != "" {
				tmplAtespace := createTemplateAtespace
				if tmplAtespace == "" {
					tmplAtespace = atespace
				}
				req.Template = &ateenvv1alpha.Template{
					Name:     createTemplate,
					Atespace: tmplAtespace,
				}
			}
			_, err = client.Create(cmd.Context(), req)
			return err
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	cmd.Flags().StringVar(&atespace, "atespace", apiservice.DefaultAtespace, "Substrate atespace")
	auth.register(cmd, false)
	cmd.Flags().StringVar(&createTemplate, "template", "", "ActorTemplate name (defaults to server default)")
	cmd.Flags().StringVar(&createTemplateAtespace, "template-atespace", "", "Substrate atespace of the ActorTemplate (defaults to environment atespace)")
	cmd.Flags().StringVar(&rootfsImage, "rootfs-image", "", "digest-pinned rootfs image to run unmodified (repo@sha256:...); the template becomes the base the guest is taken from")
	cmd.Flags().StringSliceVar(&workerSelector, "worker-selector", nil, "label a worker pool must carry, key=value; repeat the flag or separate with commas. ANDed with the template's own selector, thus it narrows the pools the environment can run on")
	return cmd
}

// parseWorkerSelector turns key=value items into the labels a worker pool
// must carry. A key given twice is an error, whatever the values: a
// selector has one value per key, and a repeat is a typo to surface.
func parseWorkerSelector(items []string) (map[string]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	selector := make(map[string]string, len(items))
	for _, item := range items {
		key, value, ok := strings.Cut(item, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("--worker-selector %q: want key=value", item)
		}
		if prev, dup := selector[key]; dup {
			return nil, fmt.Errorf("--worker-selector %s is given twice (%q and %q)", key, prev, value)
		}
		selector[key] = value
	}
	return selector, nil
}

func newGetCommand() *cobra.Command {
	var (
		endpoint string
		atespace string
		auth     authFlags
		asJSON   bool
	)
	cmd := &cobra.Command{
		Use:   "get <id>",
		Short: "Show an environment: template, status, node and worker selector",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{
				Endpoint:  endpoint,
				AuthToken: token,
			})
			if err != nil {
				return err
			}
			defer client.Close()
			e, err := client.Get(cmd.Context(), atespace, args[0])
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(e)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(out, "%s\n", b)
				return err
			}
			fmt.Fprintf(out, "id: %s\natespace: %s\ntemplate: %s/%s\nstatus: %s\nnode: %s\n",
				e.GetId(), e.GetAtespace(), e.GetTemplate().GetAtespace(), e.GetTemplate().GetName(),
				strings.ToLower(strings.TrimPrefix(e.GetStatus().String(), "ENVIRONMENT_STATUS_")), e.GetNode())
			if sel := e.GetWorkerSelector(); len(sel) > 0 {
				keys := slices.Sorted(maps.Keys(sel))
				items := make([]string, 0, len(keys))
				for _, k := range keys {
					items = append(items, k+"="+sel[k])
				}
				fmt.Fprintf(out, "worker selector: %s\n", strings.Join(items, ","))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	cmd.Flags().StringVar(&atespace, "atespace", apiservice.DefaultAtespace, "Substrate atespace")
	auth.register(cmd, false)
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the environment as JSON")
	return cmd
}

func newImageStatusCommand() *cobra.Command {
	var (
		endpoint         string
		atespace         string
		auth             authFlags
		template         string
		templateAtespace string
		ensure           bool
		asJSON           bool
	)
	cmd := &cobra.Command{
		Use:   "image-status <rootfs-image>",
		Short: "Show the template derived for a rootfs image and whether its golden snapshot is ready",
		Long: "Show the template derived for a digest-pinned rootfs image and the state of its golden snapshot:\n" +
			"none (no template yet), pending (baking; environments boot from the image), ready, or failed.\n" +
			"With --ensure the template is created when missing, which starts the bake; no environment is created.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{
				Endpoint:  endpoint,
				AuthToken: token,
			})
			if err != nil {
				return err
			}
			defer client.Close()
			var tmpl *ateenvv1alpha.Template
			if template != "" || templateAtespace != "" {
				tmpl = &ateenvv1alpha.Template{Name: template, Atespace: templateAtespace}
			}
			var st *ateenvv1alpha.ImageStatus
			if ensure {
				st, err = client.EnsureImageTemplate(cmd.Context(), &ateenvv1alpha.EnsureImageTemplateRequest{RootfsImage: args[0], Template: tmpl, Atespace: atespace})
			} else {
				st, err = client.ImageStatus(cmd.Context(), &ateenvv1alpha.GetImageStatusRequest{RootfsImage: args[0], Template: tmpl, Atespace: atespace})
			}
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(st)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(out, "%s\n", b)
				return err
			}
			fmt.Fprintf(out, "image: %s\ntemplate: %s/%s (%s)\ngolden: %s\n",
				st.GetRootfsImage(), st.GetTemplate().GetAtespace(), st.GetTemplate().GetName(),
				map[bool]string{true: "exists", false: "missing"}[st.GetTemplateExists()],
				strings.ToLower(strings.TrimPrefix(st.GetGolden().String(), "GOLDEN_SNAPSHOT_STATE_")))
			if st.GetGoldenTag() != "" {
				fmt.Fprintf(out, "golden tag: %s\n", st.GetGoldenTag())
			}
			if st.GetGoldenError() != "" {
				fmt.Fprintf(out, "golden error: %s\n", st.GetGoldenError())
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	cmd.Flags().StringVar(&atespace, "atespace", "", "Substrate atespace of the templates (defaults to --template-atespace, else "+apiservice.DefaultAtespace+")")
	auth.register(cmd, false)
	cmd.Flags().StringVar(&template, "template", "", "base ActorTemplate name (defaults to server default)")
	cmd.Flags().StringVar(&templateAtespace, "template-atespace", "", "Substrate atespace of the base ActorTemplate (defaults to --atespace)")
	cmd.Flags().BoolVar(&ensure, "ensure", false, "create the derived template when missing, which starts its golden snapshot bake")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the status as JSON")
	return cmd
}

// checkNotSubcommand rejects an environment id that equals an ate-env
// subcommand name (create, delete, help, ...): `ate-env <id> read|write|shell`
// cannot reach such an environment, because the subcommand takes the name.
func checkNotSubcommand(cmd *cobra.Command, id string) error {
	for _, c := range cmd.Root().Commands() {
		if c.Name() == id || c.HasAlias(id) {
			return fmt.Errorf("environment id %q is reserved: it is an ate-env subcommand, thus `ate-env %s read|write|shell` cannot reach the environment", id, id)
		}
	}
	return nil
}

func newSuspendCommand() *cobra.Command {
	var (
		endpoint string
		atespace string
		mode     string
		auth     authFlags
	)
	cmd := &cobra.Command{
		Use:   "suspend <id>",
		Short: "Suspend an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if mode != "durable" && mode != "local" {
				return fmt.Errorf("--mode must be durable or local, got %q", mode)
			}
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{
				Endpoint:  endpoint,
				AuthToken: token,
			})
			if err != nil {
				return err
			}
			defer client.Close()

			if mode == "local" {
				return client.SuspendLocal(cmd.Context(), atespace, args[0])
			}
			return client.Suspend(cmd.Context(), atespace, args[0])
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	cmd.Flags().StringVar(&atespace, "atespace", apiservice.DefaultAtespace, "Substrate atespace")
	cmd.Flags().StringVar(&mode, "mode", "durable", "durable: return once the snapshot is in object storage; local: return once it is on the node, with the upload in the background (the environment resumes on that node until it lands and shows PAUSED meanwhile). The mode applies to a running environment; a suspended or paused one is left as it is")
	auth.register(cmd, false)
	return cmd
}

func newDeleteCommand() *cobra.Command {
	var (
		endpoint string
		atespace string
		auth     authFlags
	)
	cmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{
				Endpoint:  endpoint,
				AuthToken: token,
			})
			if err != nil {
				return err
			}
			defer client.Close()

			return client.Delete(cmd.Context(), atespace, args[0])
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	cmd.Flags().StringVar(&atespace, "atespace", apiservice.DefaultAtespace, "Substrate atespace")
	auth.register(cmd, false)
	return cmd
}

func newServerInfoCommand() *cobra.Command {
	var (
		endpoint string
		auth     authFlags
		asJSON   bool
	)
	cmd := &cobra.Command{
		Use:   "server-info",
		Short: "Show the ate-env-api server's version and features",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{
				Endpoint:  endpoint,
				AuthToken: token,
			})
			if err != nil {
				return err
			}
			defer client.Close()

			info, err := client.ServerInfo(cmd.Context())
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(info)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(out, "%s\n", b)
				return err
			}
			fmt.Fprintf(out, "server: %s\n", info.GetServer())
			fmt.Fprintf(out, "version: %s\n", info.GetVersion())
			fmt.Fprintf(out, "substrate_version: %s\n", info.GetSubstrateVersion())
			fmt.Fprintf(out, "features: %s\n", strings.Join(info.GetFeatures(), ", "))
			return nil
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print one JSON object with the fields server, version, substrate_version and features")
	auth.register(cmd, false)
	return cmd
}

func newRootCommand(args []string) *cobra.Command {
	root := &cobra.Command{
		Use:   "ate-env",
		Short: "Manage environments on Agent Substrate",
		Long: `Manage environments on Agent Substrate.

Common environment commands:
  ate-env <id> read <path>       Print an environment file to stdout
  ate-env <id> write <path>      Write stdin to an environment file
  ate-env <id> shell <cmdline>   Run a shell command line in the environment`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}

	root.AddCommand(newManifestCommand())
	root.AddCommand(newCreateCommand())
	root.AddCommand(newGetCommand())
	root.AddCommand(newImageStatusCommand())
	root.AddCommand(newPlacementDomainsCommand())
	root.AddCommand(newWarmImageCommand())
	root.AddCommand(newSuspendCommand())
	root.AddCommand(newDeleteCommand())
	root.AddCommand(newServerInfoCommand())

	if len(args) > 0 {
		cmdName := args[0]
		if _, _, err := root.Find([]string{cmdName}); err != nil {
			root.AddCommand(newGuestCommand(cmdName))
		}
	}

	return root
}

func main() {
	root := newRootCommand(os.Args[1:])
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "ate-env:", err)
		os.Exit(1)
	}
}
