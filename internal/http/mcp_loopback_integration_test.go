//go:build integration && !sqliteonly

package http

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nextlevelbuilder/goclaw/internal/mcp"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/store/pg"
	"github.com/nextlevelbuilder/goclaw/internal/testutil"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

func TestMCPLoopbackServerLifecycle(t *testing.T) {
	db := testutil.TestDB(t, "../../migrations")
	pg.InitSqlx(db)
	ctx := store.WithTenantID(context.Background(), store.MasterTenantID)
	mcpStore := pg.NewPGMCPServerStore(db, "")
	h := NewMCPHandler(mcpStore, nil, nil)
	h.db = db
	setupTestToken(t, "mcp-test")
	setupTestNoAuthFallback(t, false)

	remote := httptest.NewServer(mcp.NewBridgeServer(tools.NewRegistry(), "test", nil))
	defer remote.Close()
	localURL := strings.Replace(remote.URL, "127.0.0.1", "localhost", 1)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	gateway := httptest.NewServer(mux)
	defer gateway.Close()

	call := func(method, path string, body any, wantStatus int) []byte {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(method, gateway.URL+path, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer mcp-test")
		req.Header.Set("X-GoClaw-User-Id", "system")
		req.Header.Set("Content-Type", "application/json")
		res, err := gateway.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err = io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != wantStatus {
			t.Fatalf("%s %s: status %d, want %d; body=%s", method, path, res.StatusCode, wantStatus, data)
		}
		return data
	}

	srv := store.MCPServerData{
		Name: "loopback-" + uuid.NewString()[:8], Transport: "streamable-http", URL: localURL, Enabled: true,
	}
	data := call(http.MethodPost, "/v1/mcp/servers/test", srv, http.StatusOK)
	var discovery struct {
		Success bool `json:"success"`
	}
	if err := json.Unmarshal(data, &discovery); err != nil {
		t.Fatal(err)
	}
	if !discovery.Success {
		t.Fatalf("loopback discovery failed: %s", data)
	}

	data = call(http.MethodPost, "/v1/mcp/servers", srv, http.StatusCreated)
	if err := json.Unmarshal(data, &srv); err != nil {
		t.Fatal(err)
	}
	if srv.ID == uuid.Nil {
		t.Fatal("created server has no ID")
	}
	t.Cleanup(func() {
		if err := mcpStore.DeleteServer(ctx, srv.ID); err != nil {
			t.Error(err)
		}
	})
	path := "/v1/mcp/servers/" + srv.ID.String()
	call(http.MethodPut, path, map[string]string{"url": remote.URL}, http.StatusOK)
	data = call(http.MethodGet, path, nil, http.StatusOK)
	var saved store.MCPServerData
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.URL != remote.URL {
		t.Fatalf("updated URL = %q, want %q", saved.URL, remote.URL)
	}
	call(http.MethodPut, path, map[string]string{"url": "http://169.254.169.254/mcp"}, http.StatusBadRequest)

	importName := "import-loopback-" + uuid.NewString()[:8]
	exported, err := pg.ExportMCPServers(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(exported, func(entry pg.MCPServerExport) bool { return entry.Name == srv.Name })
	if index < 0 {
		t.Fatalf("server %q missing from export", srv.Name)
	}
	importServer := exported[index]
	importServer.Name = importName
	importServer.Transport = "sse"
	importServer.URL = localURL
	entry, err := json.Marshal(importServer)
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "servers.jsonl", Mode: 0600, Size: int64(len(entry))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(entry); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	summary, err := h.doMCPImport(ctx, &archive, "system", nil)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ServersImported != 1 {
		t.Fatalf("import summary = %+v", summary)
	}
	imported, err := mcpStore.GetServerByName(ctx, importName)
	if err != nil {
		t.Fatal(err)
	}
	if imported.URL != localURL {
		t.Fatalf("imported URL = %q, want %q", imported.URL, localURL)
	}
	if err := mcpStore.DeleteServer(ctx, imported.ID); err != nil {
		t.Fatal(err)
	}
}
