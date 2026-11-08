package mcp

import "slices"

// IsToolAllowed evaluates whether a tool (by original MCP name) passes the
// allow/deny filter from a grant. Used at registration time so non-allowed
// tools never reach the LLM, and at runtime by the grant checker.
//
// Semantics (matching ListAccessible / GrantChecker.checkAccess):
//   - Empty allow + empty deny → allowed (no filter configured)
//   - Tool in deny → denied (deny takes priority)
//   - Non-empty allow + tool NOT in allow → denied
//   - Otherwise → allowed
func IsToolAllowed(toolName string, allow, deny []string) bool {
	if slices.Contains(deny, toolName) {
		return false
	}
	if len(allow) == 0 {
		return true
	}
	return slices.Contains(allow, toolName)
}
