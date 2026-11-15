//go:build sqlite || sqliteonly

package sqlitestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
)

// spillSessionDir returns the spill directory of a session in a workspace.
func spillSessionDir(workspace string, tenant uuid.UUID, key string) string {
	return filepath.Join(workspace, tools.ToolResultSpillDirName, "session-"+tools.ToolResultSessionHash(tenant, key))
}

// TestSessionDeleteAndReset_RemoveSpillFiles wires the spill store the way the
// gateway does and checks that deleting or resetting a session removes that
// session's spill directory, and only that one: the same key in another tenant
// and another key in the same tenant keep their files.
func TestSessionDeleteAndReset_RemoveSpillFiles(t *testing.T) {
	db := openTestDB(t)
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	sessions := NewSQLiteSessionStore(db)
	spill := tools.NewToolResultSpillStore(t.TempDir())
	var hooks store.SessionLifecycleHooks = sessions
	hooks.SetSessionHooks(
		func(ctx context.Context, key string) { spill.DeleteSession(ctx, key) },
		func(ctx context.Context, key string) { spill.DeleteSession(ctx, key) },
	)

	workspace := t.TempDir()
	tenantA, tenantB := uuid.New(), uuid.New()
	save := func(tenant uuid.UUID, key string) {
		t.Helper()
		if _, err := spill.Save(workspace, tenant, key, "exec", "result of "+key); err != nil {
			t.Fatalf("Save(%s): %v", key, err)
		}
	}
	for _, key := range []string{"sess-delete", "sess-reset", "sess-other"} {
		save(tenantA, key)
	}
	save(tenantB, "sess-delete")
	save(tenantB, "sess-reset")

	// The delete and reset contexts carry the tenant the way the gateway handlers do.
	ctxA := store.WithTenantID(context.Background(), tenantA)
	sessions.GetOrCreate(ctxA, "sess-delete")
	if err := sessions.Delete(ctxA, "sess-delete"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	sessions.Reset(ctxA, "sess-reset")

	exists := func(tenant uuid.UUID, key string) bool {
		_, err := os.Stat(spillSessionDir(workspace, tenant, key))
		return err == nil
	}
	if exists(tenantA, "sess-delete") {
		t.Error("session delete left the spill directory")
	}
	if exists(tenantA, "sess-reset") {
		t.Error("session reset left the spill directory")
	}
	if !exists(tenantA, "sess-other") {
		t.Error("another session of the same tenant lost its spill directory")
	}
	if !exists(tenantB, "sess-delete") || !exists(tenantB, "sess-reset") {
		t.Error("the same keys in another tenant lost their spill directories")
	}
}
