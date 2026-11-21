package event

import (
	"encoding/json/jsontext"
	"testing"
)

func TestCaptureIdentityBeforeNormalization(t *testing.T) {
	raw := []byte(` {"type":"ris_message","data":{"id":"bad-route","timestamp":42.123456,"withdrawals":["bad-prefix"]}} `)
	var e Envelope
	e.Data(jsontext.Value(`"bad-route"`), jsontext.Value("42.123456"), raw)
	if e.MessageID != hashID("ris", "bad-route") || e.EventTimeUS == nil || *e.EventTimeUS != 42123456 {
		t.Fatalf("invalid child lost capture identity: %+v", e)
	}
	var invalid Envelope
	invalid.Data(nil, jsontext.Value(`"bad"`), raw)
	if invalid.MessageID == "" || invalid.MessageID == e.MessageID || invalid.EventTimeUS != nil || invalid.PayloadHash != e.PayloadHash {
		t.Fatalf("invalid metadata: %+v", invalid)
	}
}
