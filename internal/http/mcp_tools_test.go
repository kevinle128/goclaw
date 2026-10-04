package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMCPTestConnectionRejectsInvalidConfig(t *testing.T) {
	setupTestToken(t, "mcp-test")
	setupTestNoAuthFallback(t, false)
	mux := http.NewServeMux()
	NewMCPHandler(nil, nil, nil).RegisterRoutes(mux)

	for _, tt := range []struct {
		body, wantErrorPrefix string
	}{
		{`{"transport":"streamable-http","url":"http://169.254.169.254/mcp"}`, "invalid URL:"},
		{`{"transport":"sse","url":"http://10.0.0.1/mcp"}`, "invalid URL:"},
		{`{"transport":"streamable-http","url":"file:///etc/passwd"}`, "invalid URL:"},
		{`{"transport":"stdio","command":"sh"}`, "invalid command:"},
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/mcp/servers/test", strings.NewReader(tt.body))
		req.Header.Set("Authorization", "Bearer mcp-test")
		req.Header.Set("X-GoClaw-User-Id", "system")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
		var result struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Success || !strings.HasPrefix(result.Error, tt.wantErrorPrefix) {
			t.Fatalf("invalid config must fail before connecting: %s", rec.Body.String())
		}
	}
}
