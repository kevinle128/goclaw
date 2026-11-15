package tools

import (
	"strings"
	"testing"
)

const (
	testHexA = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	testHexB = "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"
)

func TestScrubCredentials_NostrIDsStayVisible(t *testing.T) {
	cases := map[string]string{
		"event_id plain":    `{"accepted":true,"event_id":"` + testHexA + `"}`,
		"id and pubkey":     `{"id": "` + testHexA + `", "pubkey": "` + testHexB + `"}`,
		"reply and thread":  `{"reply_to":"` + testHexA + `","thread_root":"` + testHexB + `","root":"` + testHexA + `"}`,
		"escaped in string": `{"stdout":"{\"event_id\":\"` + testHexA + `\",\"pubkey\":\"` + testHexB + `\"}"}`,
		"double escaped":    `"{\"stdout\":\"{\\\"event_id\\\":\\\"` + testHexA + `\\\"}\"}"`,
		"mention array":     `{"mention_pubkeys":["` + testHexA + `", "` + testHexB + `"]}`,
		"mention escaped":   `{\"mention_pubkeys\":[\"` + testHexA + `\",\"` + testHexB + `\"]}`,
		"e and p tags":      `{"tags":[["e","` + testHexA + `","wss://r","root"],["p","` + testHexB + `"]]}`,
		"tags escaped":      `{\"tags\":[[\"e\",\"` + testHexA + `\"],[\"p\",\"` + testHexB + `\"]]}`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ScrubCredentials(in); got != in {
				t.Errorf("id-like value was altered\n in: %s\ngot: %s", in, got)
			}
			if got := ScrubCredentialsCtx(t.Context(), in); got != in {
				t.Errorf("ctx variant altered id-like value\n in: %s\ngot: %s", in, got)
			}
		})
	}
}

func TestScrubCredentials_HexStillRedactedOutsideIDKeys(t *testing.T) {
	cases := map[string]string{
		"private_key":    `{"private_key":"` + testHexA + `"}`,
		"secret":         `{"secret":"` + testHexA + `"}`,
		"nsec":           `{"nsec":"` + testHexA + `"}`,
		"seckey":         `{"seckey":"` + testHexA + `"}`,
		"key":            `{"key":"` + testHexA + `"}`,
		"escaped secret": `{\"private_key\":\"` + testHexA + `\"}`,
		"free text":      `the digest is ` + testHexA + ` ok`,
		"user_id suffix": `{"user_id":"` + testHexA + `"}`,
		"other tag":      `{"tags":[["x","` + testHexA + `"]]}`,
		"longer than 64": `{"event_id":"` + testHexA + `ab"}`,
		"non-id array":   `{"keys":["` + testHexA + `"]}`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ScrubCredentials(in); strings.Contains(got, testHexA[:32]) {
				t.Errorf("hex leaked: %s", got)
			}
			if got := ScrubCredentialsCtx(t.Context(), in); strings.Contains(got, testHexA[:32]) {
				t.Errorf("ctx variant leaked hex: %s", got)
			}
		})
	}
}

func TestScrubCredentials_MixedIDAndSecretInOneDocument(t *testing.T) {
	in := `{"event_id":"` + testHexA + `","private_key":"` + testHexB + `"}`
	got := ScrubCredentials(in)
	if !strings.Contains(got, testHexA) {
		t.Errorf("event_id was redacted: %s", got)
	}
	if strings.Contains(got, testHexB) {
		t.Errorf("private_key leaked: %s", got)
	}
}
