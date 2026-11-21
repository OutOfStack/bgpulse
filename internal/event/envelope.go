package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strconv"
	"time"
)

// Raw envelope discriminators and transport size limit for v1.
const (
	SourceMessage   = "source_message"
	SourceLifecycle = "source_lifecycle"
	RawRecordLimit  = 2 << 20
)

// Envelope is one v1 JetStream record. JSON base64 preserves the exact RIS frame bytes.
type Envelope struct {
	Version     int        `json:"schema_version"`
	Kind        string     `json:"kind"`
	Namespace   string     `json:"namespace"`
	Source      string     `json:"source"`
	Collector   string     `json:"collector"`
	Epoch       string     `json:"connection_epoch"`
	ReceivedAt  time.Time  `json:"received_at"`
	MessageID   string     `json:"message_id"`
	SourceID    string     `json:"source_message_id,omitempty"`
	EventTimeUS *int64     `json:"event_time_us,omitzero"`
	PayloadHash string     `json:"payload_sha256,omitempty"`
	Payload     []byte     `json:"payload,omitempty"`
	Lifecycle   *Lifecycle `json:"lifecycle,omitzero"`
}

// Gap describes an interruption. Start is zero if no authoritative boundary survived retention.
type Gap struct {
	ID       string    `json:"id"`
	Start    time.Time `json:"start,omitzero"`
	End      time.Time `json:"end,omitzero"`
	Reason   string    `json:"reason"`
	Resolved bool      `json:"resolved"`
}

// Lifecycle carries a receiver-clock control event and its coverage evidence.
type Lifecycle struct {
	LastReceiveUS    int64  `json:"last_received_us,omitzero"`
	LastPubAckUS     int64  `json:"last_puback_us,omitzero"`
	LastSourceUS     int64  `json:"last_source_event_us,omitzero"`
	Type             string `json:"type"`
	Ordinal          uint64 `json:"ordinal"`
	SubscriptionHash string `json:"subscription_sha256"`
	Reason           string `json:"reason"`
	Gap              *Gap   `json:"gap,omitzero"`
}

// Data preserves malformed source metadata for downstream quarantine.
func (e *Envelope) Data(id, timestamp jsontext.Value, raw []byte) {
	e.Kind = SourceMessage
	sum := sha256.Sum256(raw)
	e.PayloadHash = hex.EncodeToString(sum[:])
	e.Payload = raw
	if err := json.Unmarshal(id, &e.SourceID); err == nil && e.SourceID != "" {
		e.MessageID = hashID(source, e.SourceID)
	} else {
		e.MessageID = hashID("invalid_ris_frame", e.PayloadHash)
	}
	if us, err := parseMicros(timestamp); err == nil {
		e.EventTimeUS = &us
	}
}

// Control identifies an ingester lifecycle record within its connection epoch.
func (e *Envelope) Control(control Lifecycle) {
	e.Kind = SourceLifecycle
	e.Lifecycle = &control
	e.MessageID = hashID(e.Namespace, e.Source, e.Collector, e.Epoch, strconv.FormatUint(control.Ordinal, 10))
}
