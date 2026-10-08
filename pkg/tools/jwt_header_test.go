package tools

import "testing"

func TestBearerTokenHeader(t *testing.T) {
	for _, tc := range []struct {
		header, want string
		ok           bool
	}{
		{"", "", false}, {"x", "", false}, {"bearer", "", false},
		{"Bearer ", "", false}, {"Basic abc", "", false},
		{"Bearer abc extra", "", false}, {"Bearerabc", "", false},
		{"bearer abc", "abc", true}, {"Bearer abc", "abc", true},
		{"BEARER\tabc", "abc", true},
	} {
		got, ok := BearerToken(tc.header)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%q: got %q/%v", tc.header, got, ok)
		}
	}
}

func TestUserHelpersRejectMalformedHeaders(t *testing.T) {
	for _, header := range []string{"x", "bearer", "Basic abc", "Bearer not-a-jwt", "Bearer a b"} {
		if GetUser(header) != "" || GetUserID(header) != "" {
			t.Errorf("invalid header accepted: %q", header)
		}
	}
}
