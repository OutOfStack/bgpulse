package main

import (
	"encoding/json"
	"testing"
)

func TestPathAndOrigin(t *testing.T) {
	cases := []struct {
		raw, path, origin string
	}{
		{`[64500, 64501, 64502]`, "64500 64501 64502", "AS64502"},
		{`[64500, 4200000000]`, "64500 4200000000", "AS4200000000"}, // 32-bit ASN
		{`[64500, [64510, 64511]]`, "64500 {64510,64511}", "?"},     // AS_SET origin is ambiguous
		{`[]`, "", "?"},
	}
	for _, c := range cases {
		var p []any
		if err := json.Unmarshal([]byte(c.raw), &p); err != nil {
			t.Fatal(err)
		}
		if got := pathString(p); got != c.path {
			t.Errorf("pathString(%s) = %q, want %q", c.raw, got, c.path)
		}
		if got := origin(p); got != c.origin {
			t.Errorf("origin(%s) = %q, want %q", c.raw, got, c.origin)
		}
	}
	if got := canonical("2a06:de02:664:0:0:0:0:0/48"); got != "2a06:de02:664::/48" {
		t.Errorf("canonical = %q", got)
	}
}
