package cmd

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/acpserver"
)

func TestACPCommandRequiresAgent(t *testing.T) {
	cmd := acpCmd()
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--owner", "user-1",
		"--tenant", "00000000-0000-0000-0000-000000000001",
		"--workspace-root", t.TempDir(),
		"--mcp-exec-root", t.TempDir(),
	})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "agent") {
		t.Fatalf("Execute() error = %v, want required agent error", err)
	}
}

func TestACPCommandRejectsPositionalArguments(t *testing.T) {
	cmd := acpCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"unexpected"})

	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute() error = nil, want positional argument rejection")
	}
}

func TestRootRegistersACPCommand(t *testing.T) {
	command, _, err := rootCmd.Find([]string{"acp"})
	if err != nil || command == rootCmd || command.Name() != "acp" {
		t.Fatalf("rootCmd.Find(acp) = %v, %v", command, err)
	}
}

func TestResolveACPAPIKeyDoesNotUseGatewayCredentials(t *testing.T) {
	t.Setenv("GOCLAW_ACP_API_KEY", "")
	t.Setenv("GOCLAW_GATEWAY_TOKEN", "gateway-secret")
	oldToken := gatewayTokenOverride
	gatewayTokenOverride = "flag-secret"
	t.Cleanup(func() { gatewayTokenOverride = oldToken })

	if _, err := resolveACPAPIKey(""); err == nil {
		t.Fatal("resolveACPAPIKey() accepted a Gateway credential fallback")
	}
	if got, err := resolveACPAPIKey("acp-secret"); err != nil || got != "acp-secret" {
		t.Fatalf("resolveACPAPIKey() = %q, %v", got, err)
	}
}

func TestACPCommandPassesRequiredOptions(t *testing.T) {
	oldRun := runACPCommand
	t.Cleanup(func() { runACPCommand = oldRun })
	var got acpOptions
	runACPCommand = func(_ context.Context, options acpOptions, _ io.Reader, _, _ io.Writer) error {
		got = options
		return nil
	}

	workspace := t.TempDir()
	execRoot := t.TempDir()
	cmd := acpCmd()
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--agent", "coding-agent",
		"--owner", "user-1",
		"--tenant", "00000000-0000-0000-0000-000000000001",
		"--workspace-root", workspace,
		"--mcp-exec-root", execRoot,
		"--api-key", "acp-secret",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got.Agent != "coding-agent" || got.Owner != "user-1" || got.WorkspaceRoot != workspace || len(got.MCPExecRoots) != 1 || got.MCPExecRoots[0] != execRoot {
		t.Fatalf("options = %#v", got)
	}
}

func TestRunACPCanonicalizesRootsAndKeepsStdoutProtocolOnly(t *testing.T) {
	oldStart := startACPServer
	t.Cleanup(func() { startACPServer = oldStart })

	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	execRoot := filepath.Join(root, "bin")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(execRoot, 0o755); err != nil {
		t.Fatal(err)
	}

	var got resolvedACPOptions
	runtime := &fakeACPRuntime{}
	startACPServer = func(_ context.Context, options resolvedACPOptions) (acpRuntime, error) {
		got = options
		return runtime, nil
	}

	t.Setenv("GOCLAW_ACP_API_KEY", "acp-secret")
	oldServer := gatewayServerOverride
	gatewayServerOverride = "ws://127.0.0.1:18790"
	t.Cleanup(func() { gatewayServerOverride = oldServer })

	var stdout strings.Builder
	var stderr strings.Builder
	err := runACP(context.Background(), acpOptions{
		Agent: "coding-agent", Owner: "user-1",
		Tenant:        "00000000-0000-0000-0000-000000000001",
		WorkspaceRoot: workspace + string(os.PathSeparator) + ".",
		MCPExecRoots:  []string{execRoot + string(os.PathSeparator) + "."},
	}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("runACP() error = %v", err)
	}
	if stdout.String() != "" {
		t.Fatalf("stdout = %q, want protocol-only empty output", stdout.String())
	}
	canonicalWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	canonicalExecRoot, err := filepath.EvalSymlinks(execRoot)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkspaceRoot != canonicalWorkspace || len(got.MCPExecRoots) != 1 || got.MCPExecRoots[0] != canonicalExecRoot {
		t.Fatalf("resolved options = %#v", got)
	}
	if got.APIKey != "acp-secret" || got.GatewayURL != "ws://127.0.0.1:18790" {
		t.Fatalf("resolved credentials = %#v", got)
	}
	if runtime.runInput == nil || runtime.runOutput != &stdout || !runtime.closed {
		t.Fatalf("runtime lifecycle = %#v", runtime)
	}
}

func TestRunACPCancelsAndCloses(t *testing.T) {
	oldStart := startACPServer
	t.Cleanup(func() { startACPServer = oldStart })
	runtime := &fakeACPRuntime{waitForCancel: true}
	startACPServer = func(_ context.Context, _ resolvedACPOptions) (acpRuntime, error) {
		return runtime, nil
	}
	t.Setenv("GOCLAW_ACP_API_KEY", "acp-secret")
	oldServer := gatewayServerOverride
	gatewayServerOverride = "ws://127.0.0.1:18790"
	t.Cleanup(func() { gatewayServerOverride = oldServer })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runACP(ctx, validACPOptions(t), strings.NewReader(""), io.Discard, io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runACP() error = %v, want context cancellation", err)
	}
	if !runtime.closed {
		t.Fatal("runtime was not closed after cancellation")
	}
}

func TestValidateACPIdentity(t *testing.T) {
	want := resolvedACPOptions{acpOptions: acpOptions{Owner: "user-1", Tenant: "00000000-0000-0000-0000-000000000001"}}
	valid := acpserver.GatewayIdentity{UserID: want.Owner, TenantID: want.Tenant, Role: "operator"}
	if err := validateACPIdentity(valid, want); err != nil {
		t.Fatalf("validateACPIdentity() error = %v", err)
	}
	for name, identity := range map[string]acpserver.GatewayIdentity{
		"admin role":      {UserID: want.Owner, TenantID: want.Tenant, Role: "admin"},
		"owner role":      {UserID: want.Owner, TenantID: want.Tenant, Role: "owner"},
		"owner flag":      {UserID: want.Owner, TenantID: want.Tenant, Role: "operator", IsOwner: true},
		"master scope":    {UserID: want.Owner, TenantID: want.Tenant, Role: "operator", IsMasterScope: true},
		"different owner": {UserID: "user-2", TenantID: want.Tenant, Role: "operator"},
		"different tenant": {UserID: want.Owner,
			TenantID: "00000000-0000-0000-0000-000000000002", Role: "operator"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateACPIdentity(identity, want); err == nil {
				t.Fatalf("validateACPIdentity(%+v) error = nil", identity)
			}
		})
	}
}

func TestRunACPRejectsMissingAndNonCanonicalRoots(t *testing.T) {
	t.Setenv("GOCLAW_ACP_API_KEY", "acp-secret")
	oldServer := gatewayServerOverride
	gatewayServerOverride = "ws://127.0.0.1:18790"
	t.Cleanup(func() { gatewayServerOverride = oldServer })

	options := validACPOptions(t)
	options.MCPExecRoots = nil
	if err := runACP(context.Background(), options, strings.NewReader(""), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "MCP executable root") {
		t.Fatalf("runACP() error = %v, want MCP root validation", err)
	}

	options = validACPOptions(t)
	options.WorkspaceRoot = filepath.Join(t.TempDir(), "missing")
	if err := runACP(context.Background(), options, strings.NewReader(""), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "workspace root") {
		t.Fatalf("runACP() error = %v, want workspace validation", err)
	}
}

type fakeACPRuntime struct {
	waitForCancel bool
	runInput      io.Reader
	runOutput     io.Writer
	closed        bool
}

func (f *fakeACPRuntime) Run(ctx context.Context, input io.Reader, output io.Writer) error {
	f.runInput = input
	f.runOutput = output
	if f.waitForCancel {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (f *fakeACPRuntime) Close() error {
	f.closed = true
	return nil
}

func validACPOptions(t *testing.T) acpOptions {
	t.Helper()
	return acpOptions{
		Agent: "coding-agent", Owner: "user-1",
		Tenant:        "00000000-0000-0000-0000-000000000001",
		WorkspaceRoot: t.TempDir(), MCPExecRoots: []string{t.TempDir()},
	}
}
