package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
)

const (
	secretAPIKey = "sk-real-api-key-value"
	secretToken  = "glpat-real-header-token"
	secretEnvVal = "real-env-secret"
)

// secretMCPStore returns one server that holds decrypted secrets, as the
// real store does, and records update calls.
type secretMCPStore struct {
	store.MCPServerStore
	server  store.MCPServerData
	updates map[string]any
}

func newSecretMCPStore() *secretMCPStore {
	return &secretMCPStore{server: store.MCPServerData{
		BaseModel: store.BaseModel{ID: uuid.New()},
		Name:      "gitlab-mcp",
		Transport: "streamable-http",
		URL:       "http://127.0.0.1:9/mcp",
		APIKey:    secretAPIKey,
		Headers:   json.RawMessage(`{"Authorization":"Bearer ` + secretToken + `","X-Plain":"p"}`),
		Env:       json.RawMessage(`{"TOKEN":"` + secretEnvVal + `"}`),
	}}
}

func (s *secretMCPStore) ListServers(context.Context) ([]store.MCPServerData, error) {
	return []store.MCPServerData{s.server}, nil
}
func (s *secretMCPStore) CountAgentGrantsByServer(context.Context) (map[uuid.UUID]int, error) {
	return map[uuid.UUID]int{}, nil
}
func (s *secretMCPStore) GetServer(context.Context, uuid.UUID) (*store.MCPServerData, error) {
	cp := s.server
	return &cp, nil
}
func (s *secretMCPStore) CreateServer(_ context.Context, srv *store.MCPServerData) error {
	srv.ID = uuid.New()
	return nil
}
func (s *secretMCPStore) UpdateServer(_ context.Context, _ uuid.UUID, updates map[string]any) error {
	s.updates = updates
	return nil
}

func newSecretTestMux(t *testing.T) (*http.ServeMux, *secretMCPStore) {
	t.Helper()
	setupTestToken(t, "mcp-test")
	setupTestNoAuthFallback(t, false)
	st := newSecretMCPStore()
	mux := http.NewServeMux()
	NewMCPHandler(st, nil, nil).RegisterRoutes(mux)
	return mux, st
}

func assertNoSecrets(t *testing.T, label, body string) {
	t.Helper()
	for _, secret := range []string{secretAPIKey, secretToken, secretEnvVal} {
		if strings.Contains(body, secret) {
			t.Fatalf("%s leaks %q: %s", label, secret, body)
		}
	}
}

func TestMCPReadEndpointsMaskSecrets(t *testing.T) {
	mux, st := newSecretTestMux(t)
	path := "/v1/mcp/servers/" + st.server.ID.String()

	list := serveMCPRequest(mux, http.MethodGet, "/v1/mcp/servers", "")
	get := serveMCPRequest(mux, http.MethodGet, path, "")
	create := serveMCPRequest(mux, http.MethodPost, "/v1/mcp/servers",
		`{"name":"new-mcp","transport":"streamable-http","url":"http://127.0.0.1:9/mcp",`+
			`"api_key":"`+secretAPIKey+`","headers":{"Authorization":"Bearer `+secretToken+`"},"env":{"TOKEN":"`+secretEnvVal+`"}}`)

	for label, rec := range map[string]*httptest.ResponseRecorder{"list": list, "get": get, "create": create} {
		if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
			t.Fatalf("%s status = %d: %s", label, rec.Code, rec.Body)
		}
		assertNoSecrets(t, label, rec.Body.String())
	}

	var got store.MCPServerData
	if err := json.Unmarshal(get.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.APIKey != "***" {
		t.Fatalf("api_key = %q, want marker", got.APIKey)
	}
	assertSameJSON(t, "headers", got.Headers, []byte(`{"Authorization":"***","X-Plain":"***"}`))
	assertSameJSON(t, "env", got.Env, []byte(`{"TOKEN":"***"}`))
	if got.URL != st.server.URL || got.Transport != "streamable-http" {
		t.Fatalf("non-secret fields changed: %+v", got)
	}
	if st.server.APIKey != secretAPIKey {
		t.Fatal("masking must not change the stored server")
	}
}

func TestMCPUpdateKeepsSecretsWhenMarkerSentBack(t *testing.T) {
	mux, st := newSecretTestMux(t)
	body := `{"api_key":"***","headers":{"Authorization":"***","X-Plain":"***"},"env":{"TOKEN":"***"},"timeout_sec":30}`
	rec := serveMCPRequest(mux, http.MethodPut, "/v1/mcp/servers/"+st.server.ID.String(), body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	if _, ok := st.updates["api_key"]; ok {
		t.Fatalf("marker api_key must not be written: %v", st.updates)
	}
	hdr, _ := json.Marshal(st.updates["headers"])
	assertSameJSON(t, "headers", hdr, []byte(`{"Authorization":"Bearer `+secretToken+`","X-Plain":"p"}`))
	env, _ := json.Marshal(st.updates["env"])
	assertSameJSON(t, "env", env, []byte(`{"TOKEN":"`+secretEnvVal+`"}`))
}

func TestMCPUpdateReplacesWithNewValues(t *testing.T) {
	mux, st := newSecretTestMux(t)
	body := `{"api_key":"new-key","headers":{"Authorization":"Bearer new","Added":"***"},"env":{"TOKEN":"new-env"}}`
	rec := serveMCPRequest(mux, http.MethodPut, "/v1/mcp/servers/"+st.server.ID.String(), body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	if st.updates["api_key"] != "new-key" {
		t.Fatalf("api_key = %v, want new-key", st.updates["api_key"])
	}
	hdr, _ := json.Marshal(st.updates["headers"])
	// A marker for a key without a stored value is dropped, not stored.
	assertSameJSON(t, "headers", hdr, []byte(`{"Authorization":"Bearer new"}`))
	env, _ := json.Marshal(st.updates["env"])
	assertSameJSON(t, "env", env, []byte(`{"TOKEN":"new-env"}`))
}

func TestRestoreMaskedMCPStrings(t *testing.T) {
	stored := json.RawMessage(`{"A":"real"}`)
	got := restoreMaskedMCPStrings(map[string]string{"A": "***", "B": "new", "C": "***"}, stored)
	if len(got) != 2 || got["A"] != "real" || got["B"] != "new" {
		t.Fatalf("got %v", got)
	}
	if restoreMaskedMCPStrings(nil, stored) != nil {
		t.Fatal("nil input must stay nil")
	}
}
