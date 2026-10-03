package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/nextlevelbuilder/goclaw/internal/acpserver"
)

type acpOptions struct {
	Agent         string
	Owner         string
	Tenant        string
	WorkspaceRoot string
	MCPExecRoots  []string
	APIKey        string
}

type resolvedACPOptions struct {
	acpOptions
	GatewayURL string
}

type acpRuntime interface {
	Run(context.Context, io.Reader, io.Writer) error
	Close() error
}

var (
	runACPCommand  = runACP
	startACPServer = newACPServer
)

func acpCmd() *cobra.Command {
	var options acpOptions
	command := &cobra.Command{
		Use:          "acp",
		Short:        "Run the ACP v1 stdio adapter",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(command *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(command.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return runACPCommand(ctx, options, command.InOrStdin(), command.OutOrStdout(), command.ErrOrStderr())
		},
	}
	command.Flags().StringVar(&options.Agent, "agent", "", "fixed agent key or ID")
	command.Flags().StringVar(&options.Owner, "owner", "", "expected Gateway owner user ID")
	command.Flags().StringVar(&options.Tenant, "tenant", "", "expected Gateway tenant UUID")
	command.Flags().StringVar(&options.WorkspaceRoot, "workspace-root", "", "allowed workspace root")
	command.Flags().StringSliceVar(&options.MCPExecRoots, "mcp-exec-root", nil, "allowed MCP executable root (repeatable)")
	command.Flags().StringVar(&options.APIKey, "api-key", "", "ACP-specific Gateway API key")
	for _, name := range []string{"agent", "owner", "tenant", "workspace-root", "mcp-exec-root"} {
		if err := command.MarkFlagRequired(name); err != nil {
			panic(err)
		}
	}
	return command
}

func resolveACPAPIKey(flagValue string) (string, error) {
	if value := strings.TrimSpace(flagValue); value != "" {
		return value, nil
	}
	if value := strings.TrimSpace(os.Getenv("GOCLAW_ACP_API_KEY")); value != "" {
		return value, nil
	}
	return "", errors.New("ACP API key is required through --api-key or GOCLAW_ACP_API_KEY")
}

func runACP(ctx context.Context, options acpOptions, input io.Reader, output, _ io.Writer) error {
	resolved, err := resolveACPOptions(options)
	if err != nil {
		return err
	}
	runtime, err := startACPServer(ctx, resolved)
	if err != nil {
		return err
	}
	runErr := runtime.Run(ctx, input, output)
	closeErr := runtime.Close()
	if runErr != nil {
		return errors.Join(runErr, closeErr)
	}
	return closeErr
}

func resolveACPOptions(options acpOptions) (resolvedACPOptions, error) {
	options.Agent = strings.TrimSpace(options.Agent)
	options.Owner = strings.TrimSpace(options.Owner)
	if options.Agent == "" || options.Owner == "" {
		return resolvedACPOptions{}, errors.New("ACP agent and owner are required")
	}
	tenantID, err := uuid.Parse(strings.TrimSpace(options.Tenant))
	if err != nil || tenantID == uuid.Nil {
		return resolvedACPOptions{}, errors.New("ACP tenant must be a non-zero UUID")
	}
	options.Tenant = tenantID.String()
	options.APIKey, err = resolveACPAPIKey(options.APIKey)
	if err != nil {
		return resolvedACPOptions{}, err
	}
	options.WorkspaceRoot, err = canonicalACPDirectory("workspace root", options.WorkspaceRoot)
	if err != nil {
		return resolvedACPOptions{}, err
	}
	if len(options.MCPExecRoots) == 0 {
		return resolvedACPOptions{}, errors.New("at least one MCP executable root is required")
	}
	for index, root := range options.MCPExecRoots {
		options.MCPExecRoots[index], err = canonicalACPDirectory("MCP executable root", root)
		if err != nil {
			return resolvedACPOptions{}, err
		}
	}
	return resolvedACPOptions{acpOptions: options, GatewayURL: resolveACPGatewayURL()}, nil
}

func resolveACPGatewayURL() string {
	if value := firstNonEmpty(gatewayServerOverride, os.Getenv("GOCLAW_SERVER"), os.Getenv("GOCLAW_GATEWAY_URL")); value != "" {
		return strings.TrimRight(value, "/")
	}
	return resolveGatewayBaseURL()
}

func canonicalACPDirectory(label, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return "", fmt.Errorf("%s must be an absolute directory", label)
	}
	canonical, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", label, err)
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", label, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", label)
	}
	return canonical, nil
}

func newACPServer(ctx context.Context, options resolvedACPOptions) (acpRuntime, error) {
	gateway := acpserver.NewGatewayClient(acpserver.GatewayClientConfig{
		URL: options.GatewayURL, Token: options.APIKey, UserID: options.Owner,
	})
	if err := gateway.Connect(ctx); err != nil {
		return nil, err
	}
	if err := validateACPIdentity(gateway.Identity(), options); err != nil {
		return nil, errors.Join(err, gateway.Close())
	}
	server, err := acpserver.NewServer(acpserver.ServerConfig{
		Gateway: gateway, AgentID: options.Agent, WorkspaceRoot: options.WorkspaceRoot,
		MCPExecRoots: options.MCPExecRoots, Version: Version,
	})
	if err != nil {
		return nil, errors.Join(err, gateway.Close())
	}
	return server, nil
}

func validateACPIdentity(identity acpserver.GatewayIdentity, options resolvedACPOptions) error {
	if identity.Role != "operator" || identity.IsOwner || identity.IsMasterScope {
		return errors.New("ACP API key must authenticate as a non-owner operator without master scope")
	}
	if identity.UserID != options.Owner {
		return errors.New("Gateway owner does not match --owner")
	}
	tenantID, err := uuid.Parse(identity.TenantID)
	if err != nil || tenantID.String() != options.Tenant {
		return errors.New("Gateway tenant does not match --tenant")
	}
	return nil
}
