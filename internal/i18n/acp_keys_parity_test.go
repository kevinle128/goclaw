package i18n

import "testing"

func TestI18nCatalogs_HaveACPRelayKeys(t *testing.T) {
	keys := []string{
		MsgACPRelayUnavailable,
		MsgACPInvalidParams,
		MsgACPAgentNotAllowed,
		MsgACPSubscribeLimit,
		MsgACPRequestNotFound,
		MsgACPRequestNotOwner,
	}
	for _, locale := range []string{LocaleEN, LocaleVI, LocaleZH} {
		for _, key := range keys {
			if msg := lookup(locale, key); msg == "" || msg == key {
				t.Errorf("locale=%s key=%s has no translation", locale, key)
			}
		}
	}
}
