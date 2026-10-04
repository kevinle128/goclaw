package mcp

import (
	"fmt"
	"net"
	"net/url"

	"github.com/nextlevelbuilder/goclaw/internal/security"
)

// ValidateURL applies the MCP URL policy. Local MCP servers can use loopback;
// other blocked IP ranges still use the shared address classification.
func ValidateURL(rawURL string) error {
	if rawURL == "" {
		return nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("URL validation failed: ssrf: parse url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("URL validation failed: ssrf: scheme %q not allowed (only http/https)", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("URL validation failed: ssrf: empty host")
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("URL validation failed: ssrf: resolve %q: %w", host, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("URL validation failed: ssrf: %q resolved to no addresses", host)
	}
	for _, ip := range ips {
		if !ip.IsLoopback() && security.IsBlocked(ip) {
			return fmt.Errorf("URL validation failed: ssrf: %q resolved to blocked IP %s", host, ip)
		}
	}
	return nil
}
