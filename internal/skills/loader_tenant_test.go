package skills

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type tenantKey struct{}

func writeManagedSkill(t *testing.T, dir, slug, version, desc string) {
	t.Helper()
	d := filepath.Join(dir, slug, version)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: " + slug + "\ndescription: " + desc + "\n---\n\nbody of " + desc + " in {baseDir}\n"
	if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A skill uploaded by a tenant is visible to that tenant only, wins over a master skill with the same
// slug, and does not leak into the master view through the shared cache.
func TestTenantManagedSkillsAreScopedToTheirTenant(t *testing.T) {
	root := t.TempDir()
	master := filepath.Join(root, "skills-store")
	tenantA := filepath.Join(root, "tenants", "a", "skills-store")
	tenantB := filepath.Join(root, "tenants", "b", "skills-store")
	writeManagedSkill(t, master, "shared", "1", "master shared")
	writeManagedSkill(t, tenantA, "shared", "2", "tenant shared")
	writeManagedSkill(t, tenantA, "only-a", "1", "only a")
	writeManagedSkill(t, tenantA, "only-a", "3", "only a v3")

	l := NewLoader("", "", "")
	l.SetManagedDir(master)
	l.SetTenantManagedDir(func(ctx context.Context) string {
		switch ctx.Value(tenantKey{}) {
		case "a":
			return tenantA
		case "b":
			return tenantB
		}
		return ""
	})
	ctxA := context.WithValue(context.Background(), tenantKey{}, "a")
	ctxB := context.WithValue(context.Background(), tenantKey{}, "b")
	ctxMaster := context.Background()

	slugs := func(ctx context.Context) map[string]Info {
		m := map[string]Info{}
		for _, s := range l.ListSkills(ctx) {
			m[s.Slug] = s
		}
		return m
	}

	a := slugs(ctxA)
	if a["only-a"].Description != "only a v3" || a["only-a"].BaseDir != filepath.Join(tenantA, "only-a", "3") {
		t.Fatalf("tenant a latest version not listed: %+v", a["only-a"])
	}
	if a["shared"].Description != "tenant shared" {
		t.Fatalf("tenant skill must win over master skill, got %q", a["shared"].Description)
	}
	if got := l.BuildSummary(ctxA, []string{"only-a"}); got == "" {
		t.Fatal("tenant skill missing from the prompt summary")
	}

	for name, ctx := range map[string]context.Context{"master": ctxMaster, "tenant b": ctxB} {
		v := slugs(ctx)
		if _, ok := v["only-a"]; ok {
			t.Fatalf("%s sees tenant a's skill", name)
		}
		if v["shared"].Description != "master shared" {
			t.Fatalf("%s must see the master skill, got %q", name, v["shared"].Description)
		}
		if _, ok := l.GetSkill(ctx, "only-a"); ok {
			t.Fatalf("%s GetSkill finds tenant a's skill", name)
		}
		if _, ok := l.LoadSkill(ctx, "only-a"); ok {
			t.Fatalf("%s LoadSkill reads tenant a's skill", name)
		}
	}

	if info, ok := l.GetSkill(ctxA, "only-a"); !ok || info.Description != "only a v3" {
		t.Fatalf("tenant a GetSkill: %+v %v", info, ok)
	}
	content, ok := l.LoadSkill(ctxA, "shared")
	if !ok || content == "" || !strings.Contains(content, "tenant shared") || !strings.Contains(content, filepath.Join(tenantA, "shared", "2")) {
		t.Fatalf("tenant a LoadSkill: %q", content)
	}
}
