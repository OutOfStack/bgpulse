package persist

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/outofstack/bgpulse/internal/event"
)

func envelope(t *testing.T, edit func(*event.Envelope)) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/ris/rrc21-update.json")
	if err != nil {
		t.Fatal(err)
	}
	e := event.Envelope{Version: 1, Namespace: "live", Source: "ris", Collector: "rrc21", ReceivedAt: time.Unix(1, 0)}
	e.Data(jsontext.Value(`"37.49.236.177-01a1fda648f2000a"`), jsontext.Value("1795256895.730"), raw)
	edit(&e)
	b, err := json.Marshal(&e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecode(t *testing.T) {
	keep := func(*event.Envelope) {}
	lifecycle := func(l *event.Lifecycle) func(*event.Envelope) {
		return func(e *event.Envelope) { e.Kind, e.Payload, e.Lifecycle = event.SourceLifecycle, nil, l }
	}
	for _, tc := range []struct {
		name      string
		data      []byte
		category  string // quarantine category; "" for success
		stop      bool
		lifecycle bool // success without a message
	}{
		{"source", envelope(t, keep), "", false, false},
		{"lifecycle", envelope(t, lifecycle(&event.Lifecycle{Type: "gap_open", Gap: &event.Gap{ID: "g"}})), "", false, true},
		{"not JSON", []byte("{"), "invalid_envelope", false, false},
		{"wrong field type", []byte(`{"schema_version":1,"kind":7}`), "invalid_envelope", false, false},
		{"no schema version", []byte(`{}`), "invalid_envelope", false, false},
		{"zero schema version", []byte(`{"schema_version":0,"kind":7}`), "invalid_envelope", false, false},
		{"future schema", []byte(`{"schema_version":2,"kind":7}`), "", true, false},
		{"unknown kind", envelope(t, func(e *event.Envelope) { e.Kind = "rib_dump" }), "", true, false},
		{"other namespace", envelope(t, func(e *event.Envelope) { e.Namespace = "replay1" }), "invalid_envelope", false, false},
		{"no receipt time", envelope(t, func(e *event.Envelope) { e.ReceivedAt = time.Time{} }), "invalid_envelope", false, false},
		{"bad payload", envelope(t, func(e *event.Envelope) { e.Payload = []byte(`{"type":"ris_message"}`) }), "invalid_source", false, false},
		{"foreign ID", envelope(t, func(e *event.Envelope) { e.MessageID = "x" }), "invalid_source", false, false},
		{"no lifecycle", envelope(t, lifecycle(nil)), "invalid_lifecycle", false, false},
		{"gap without ID", envelope(t, lifecycle(&event.Lifecycle{Type: "gap_open", Gap: &event.Gap{}})), "invalid_lifecycle", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, m, err := decode(tc.data, "live")
			var bad *invalid
			switch {
			case tc.stop:
				if !errors.Is(err, errIncompatible) {
					t.Fatalf("err = %v, want errIncompatible", err)
				}
			case tc.category != "":
				if !errors.As(err, &bad) || bad.category != tc.category {
					t.Fatalf("err = %v, want category %s", err, tc.category)
				}
			case err != nil:
				t.Fatal(err)
			case tc.lifecycle != (m == nil), m != nil && len(m.Children) != 3:
				t.Fatalf("message %+v", m)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	for deliveries, want := range []time.Duration{time.Second, time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		if got := backoff(uint64(deliveries)); got != want {
			t.Errorf("backoff(%d) = %v, want %v", deliveries, got, want)
		}
	}
	if got := backoff(1 << 40); got != maxBackoff {
		t.Errorf("backoff(2^40) = %v", got)
	}
}
