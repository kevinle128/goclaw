package i18n

import "testing"

func TestMsgOutputTruncated_HasTextInEveryLocale(t *testing.T) {
	t.Parallel()
	for _, locale := range []string{LocaleEN, LocaleVI, LocaleZH} {
		got := T(locale, MsgOutputTruncated)
		if got == "" || got == MsgOutputTruncated {
			t.Errorf("locale %q: MsgOutputTruncated has no text (got %q)", locale, got)
		}
	}
}
