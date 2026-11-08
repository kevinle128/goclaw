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

// settingsMCPStore records the server writes of the MCP handler. The
// methods that the tests do not call stay nil and panic when called.
type settingsMCPStore struct {
	store.MCPServerStore
	created *store.MCPServerData
	updates map[string]any
}

func (s *settingsMCPStore) CreateServer(_ context.Context, srv *store.MCPServerData) error {
	srv.ID = uuid.New()
	cp := *srv
	s.created = &cp
	return nil
}

func (s *settingsMCPStore) GetServer(_ context.Context, id uuid.UUID) (*store.MCPServerData, error) {
	return &store.MCPServerData{
		BaseModel: store.BaseModel{ID: id},
		Name:      "archon-mcp",
		Transport: "streamable-http",
		URL:       "http://127.0.0.1:9/mcp",
	}, nil
}

func (s *settingsMCPStore) UpdateServer(_ context.Context, _ uuid.UUID, updates map[string]any) error {
	s.updates = updates
	return nil
}

func newSettingsTestMux(t *testing.T) (*http.ServeMux, *settingsMCPStore) {
	t.Helper()
	setupTestToken(t, "mcp-test")
	setupTestNoAuthFallback(t, false)
	st := &settingsMCPStore{}
	mux := http.NewServeMux()
	NewMCPHandler(st, nil, nil).RegisterRoutes(mux)
	return mux, st
}

func serveMCPRequest(mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer mcp-test")
	req.Header.Set("X-GoClaw-User-Id", "system")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

const validTaskSettings = `{"protocol":"auto","tool_hints":{"global":"hint"},` +
	`"task_events":{"event":"workflow_run.status_changed","task_argument":"run_id",` +
	`"bootstrap_arguments":{"workflow":"mcp-smoke"},"heartbeat_sec":30}}`

func createServerBody(settings string) string {
	return `{"name":"archon-mcp","transport":"streamable-http","url":"http://127.0.0.1:9/mcp",` +
		`"timeout_sec":60,"enabled":true,"settings":` + settings + `}`
}

func assertSameJSON(t *testing.T, label string, got, want []byte) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("%s: bad JSON %s: %v", label, got, err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("%s: bad JSON %s: %v", label, want, err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("%s = %s, want %s", label, gb, wb)
	}
}

func TestMCPCreateServerRejectsBadTaskEvents(t *testing.T) {
	for name, settings := range map[string]string{
		"missing task_argument": `{"task_events":{"event":"workflow_run.status_changed"}}`,
		"heartbeat too low":     `{"task_events":{"event":"e","task_argument":"run_id","heartbeat_sec":1}}`,
		"bootstrap not object":  `{"task_events":{"event":"e","task_argument":"run_id","bootstrap_arguments":[1]}}`,
		"unknown protocol":      `{"protocol":"modern"}`,
	} {
		t.Run(name, func(t *testing.T) {
			mux, st := newSettingsTestMux(t)
			rec := serveMCPRequest(mux, http.MethodPost, "/v1/mcp/servers", createServerBody(settings))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "invalid request: settings.") {
				t.Fatalf("error must name the bad setting: %s", rec.Body.String())
			}
			if st.created != nil {
				t.Fatal("the server must not be stored")
			}
		})
	}
}

func TestMCPCreateServerStoresValidTaskEvents(t *testing.T) {
	mux, st := newSettingsTestMux(t)
	rec := serveMCPRequest(mux, http.MethodPost, "/v1/mcp/servers", createServerBody(validTaskSettings))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	if st.created == nil {
		t.Fatal("the server was not stored")
	}
	assertSameJSON(t, "stored settings", st.created.Settings, []byte(validTaskSettings))

	var resp store.MCPServerData
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	assertSameJSON(t, "response settings", resp.Settings, []byte(validTaskSettings))
}

func TestMCPUpdateServerValidatesSettings(t *testing.T) {
	path := "/v1/mcp/servers/" + uuid.NewString()

	t.Run("bad task_events", func(t *testing.T) {
		mux, st := newSettingsTestMux(t)
		rec := serveMCPRequest(mux, http.MethodPut, path,
			`{"settings":{"task_events":{"event":"e","task_argument":"run_id","safety_poll_sec":10}}}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}
		if st.updates != nil {
			t.Fatal("the update must not be stored")
		}
	})

	t.Run("valid task_events", func(t *testing.T) {
		mux, st := newSettingsTestMux(t)
		rec := serveMCPRequest(mux, http.MethodPut, path, `{"settings":`+validTaskSettings+`}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
		got, err := json.Marshal(st.updates["settings"])
		if err != nil {
			t.Fatal(err)
		}
		assertSameJSON(t, "updated settings", got, []byte(validTaskSettings))
	})

	t.Run("no settings key", func(t *testing.T) {
		mux, st := newSettingsTestMux(t)
		rec := serveMCPRequest(mux, http.MethodPut, path, `{"timeout_sec":90}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
		if _, ok := st.updates["settings"]; ok {
			t.Fatal("settings must stay unchanged")
		}
	})

	t.Run("null settings", func(t *testing.T) {
		mux, _ := newSettingsTestMux(t)
		rec := serveMCPRequest(mux, http.MethodPut, path, `{"settings":null}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
	})
}
