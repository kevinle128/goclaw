package mcp

import (
	"testing"

	"github.com/nextlevelbuilder/goclaw/internal/security"
)

func TestValidateCommand_Injection_Rejected(t *testing.T) {
	tests := []struct {
		name    string
		command string
		wantErr bool
	}{
		{"shell injection semicolon", "python; rm -rf /", true},
		{"shell injection pipe", "node | cat /etc/passwd", true},
		{"shell injection backtick", "ruby `whoami`", true},
		{"shell injection subshell", "go $(curl x.com)", true},
		{"shell injection ampersand", "node & echo pwned", true},
		{"path traversal", "../../../bin/sh", true},
		{"newline injection", "node\nrm", true},
		{"empty whitespace", "   ", true},
		{"not in allowlist", "sh", true},
		{"not in allowlist bash", "bash", true},
		{"valid node", "node", false},
		{"valid npx", "npx", false},
		{"valid python", "python", false},
		{"valid python3", "python3", false},
		{"valid uvx", "uvx", false},
		{"valid deno", "deno", false},
		{"valid bun", "bun", false},
		{"valid absolute path", "/usr/local/bin/node", false},
		{"empty command", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCommand(tt.command)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateCommand(%q) = nil, want error", tt.command)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateCommand(%q) = %v, want nil", tt.command, err)
			}
		})
	}
}

func TestValidateArgs_DangerousPatterns_Rejected(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"eval flag", []string{"--eval", "process.exit(1)"}, true},
		{"short eval flag", []string{"-e", "code"}, true},
		{"c flag", []string{"-c", "import os"}, true},
		{"require flag", []string{"--require", "malicious"}, true},
		{"import flag", []string{"--import", "bad-module"}, true},
		{"exec in arg", []string{"exec('rm -rf /')"}, true},
		{"eval in arg", []string{"eval('bad')"}, true},
		{"python import", []string{"__import__('os')"}, true},
		{"child_process", []string{"require('child_process')"}, true},
		{"subprocess", []string{"subprocess.run"}, true},
		{"shell metachar", []string{"file.js; rm -rf /"}, true},
		{"valid args", []string{"server.js", "--port", "3000"}, false},
		{"valid path args", []string{"/path/to/script.js"}, false},
		{"empty args", []string{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateArgs(tt.args)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateArgs(%v) = nil, want error", tt.args)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateArgs(%v) = %v, want nil", tt.args, err)
			}
		})
	}
}

func TestValidateURL_SSRF_Rejected(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"localhost", "http://localhost:8080/mcp", false},
		{"HTTPS localhost", "https://localhost:8080/mcp", false},
		{"127.0.0.1", "http://127.0.0.1/mcp", false},
		{"loopback range", "http://127.0.0.2/mcp", false},
		{"public IPv4", "https://93.184.216.34/mcp", false},
		{"public IPv6", "https://[2606:4700:4700::1111]/mcp", false},
		{"AWS metadata", "http://169.254.169.254/latest/meta-data", true},
		{"private 10.x", "http://10.0.0.1/mcp", true},
		{"private 172.16.x", "http://172.16.0.1/mcp", true},
		{"private 192.168.x", "http://192.168.1.1/mcp", true},
		{"IPv6 localhost", "http://[::1]/mcp", false},
		{"IPv4-mapped localhost", "http://[::ffff:127.0.0.1]/mcp", false},
		{"IPv6 private", "http://[fd00::1]/mcp", true},
		{"IPv4-mapped private", "http://[::ffff:10.0.0.1]/mcp", true},
		{"IPv6 link-local", "http://[fe80::1]/mcp", true},
		{"multicast", "http://224.0.0.1/mcp", true},
		{"unspecified", "http://0.0.0.0/mcp", true},
		{"IPv6 unspecified", "http://[::]/mcp", true},
		{"missing host", "http:///mcp", true},
		{"invalid URL", "://", true},
		{"invalid port", "http://localhost:bad/mcp", true},
		{"missing scheme", "localhost:8080/mcp", true},
		{"unresolvable host", "http://mcp-server.invalid/mcp", true},
		{"file scheme", "file:///etc/passwd", true},
		{"empty url", "", false},
		// Note: external URLs may fail DNS resolution in tests, but that's expected
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateURL(tt.url)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateURL(%q) = nil, want error", tt.url)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateURL(%q) = %v, want nil", tt.url, err)
			}
		})
	}
}

func TestValidateURL_WebhookPolicyRemainsSeparate(t *testing.T) {
	for _, rawURL := range []string{"http://localhost:8080/mcp", "http://127.0.0.1/mcp", "http://[::1]/mcp"} {
		if err := ValidateURL(rawURL); err != nil {
			t.Fatalf("MCP must allow loopback: %v", err)
		}
		if _, _, err := security.Validate(rawURL); err == nil {
			t.Fatalf("webhook must still reject %q", rawURL)
		}
	}
}

func TestValidateAndResolveEnvVar_Allowlist(t *testing.T) {
	// Set test env vars
	t.Setenv("HOME", "/home/test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("DATABASE_PASSWORD", "db-pass")

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"AWS secret", "env:AWS_SECRET_ACCESS_KEY", true},
		{"DB password", "env:DATABASE_PASSWORD", true},
		{"generic secret", "env:MY_SECRET_KEY", true},
		{"API token", "env:API_TOKEN", true},
		{"allowed HOME", "env:HOME", false},
		{"allowed PATH", "env:PATH", false},
		{"allowed USER", "env:USER", false},
		{"allowed NODE_ENV", "env:NODE_ENV", false},
		{"allowed DEBUG", "env:DEBUG", false},
		{"plain value", "Bearer xyz", false},
		{"not env prefix", "envHOME", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateAndResolveEnvVar(tt.value)
			if tt.wantErr && err == nil {
				t.Errorf("ValidateAndResolveEnvVar(%q) = nil, want error", tt.value)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateAndResolveEnvVar(%q) = %v, want nil", tt.value, err)
			}
		})
	}
}

func TestValidateAndResolveEnvVar_ResolvesValue(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")

	val, err := ValidateAndResolveEnvVar("env:HOME")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "/home/testuser" {
		t.Errorf("got %q, want /home/testuser", val)
	}
}

func TestValidateServerConfig_Combined(t *testing.T) {
	tests := []struct {
		name      string
		transport string
		command   string
		args      []string
		url       string
		wantErr   bool
	}{
		{
			name:      "valid stdio",
			transport: "stdio",
			command:   "node",
			args:      []string{"server.js"},
			wantErr:   false,
		},
		{
			name:      "stdio with injection",
			transport: "stdio",
			command:   "node; rm -rf /",
			wantErr:   true,
		},
		{
			name:      "stdio with bad args",
			transport: "stdio",
			command:   "node",
			args:      []string{"--eval", "process.exit()"},
			wantErr:   true,
		},
		{
			name:      "sse with private IP",
			transport: "sse",
			url:       "http://192.168.1.1/mcp",
			wantErr:   true,
		},
		{
			name:      "streamable-http with localhost",
			transport: "streamable-http",
			url:       "http://localhost/mcp",
			wantErr:   false,
		},
		{
			name:      "sse with loopback",
			transport: "sse",
			url:       "http://127.0.0.1/mcp",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateServerConfig(tt.transport, tt.command, tt.args, tt.url)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateHeaders_EnvVarCheck(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		wantErr bool
	}{
		{
			name:    "plain values",
			headers: map[string]string{"Authorization": "Bearer token123"},
			wantErr: false,
		},
		{
			name:    "allowed env var",
			headers: map[string]string{"X-User": "env:USER"},
			wantErr: false,
		},
		{
			name:    "sensitive env var",
			headers: map[string]string{"Authorization": "env:AWS_SECRET_ACCESS_KEY"},
			wantErr: true,
		},
		{
			name:    "mixed with sensitive",
			headers: map[string]string{"X-User": "env:USER", "X-Secret": "env:API_TOKEN"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateHeaders(tt.headers)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
