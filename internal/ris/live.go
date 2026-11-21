// Package ris reads the pilot's acknowledged all-types RIS Live subscription.
package ris

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/coder/websocket"
)

// The pilot captures all RIS message types from one collector.
const (
	Collector    = "rrc21"
	Subscription = `{"type":"ris_subscribe","data":{"host":"` + Collector + `","socketOptions":{"includeRaw":false,"acknowledge":true}}}`
	MaxFrame     = 1 << 20
)

// Frame contains the transport fields needed before durable capture.
type Frame struct {
	Type string `json:"type"`
	Data struct {
		Subscription  map[string]string `json:"subscription"`
		SocketOptions struct {
			IncludeRaw  *bool `json:"includeRaw"`
			Acknowledge bool  `json:"acknowledge"`
		} `json:"socketOptions"`
		Message   string         `json:"message"`
		Host      string         `json:"host"`
		ID        jsontext.Value `json:"id"`
		Timestamp jsontext.Value `json:"timestamp"`
	} `json:"data"`
}

// Decode checks the subscription, frame type and collector before capture.
func Decode(raw []byte, acknowledged bool) (Frame, error) {
	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return f, err
	}
	switch f.Type {
	case "ris_subscribe_ok":
		o := f.Data.SocketOptions
		if acknowledged || len(f.Data.Subscription) != 1 || f.Data.Subscription["host"] != Collector || o.IncludeRaw == nil || *o.IncludeRaw || !o.Acknowledge {
			return f, errors.New("unexpected RIS subscription acknowledgment")
		}
	case "ris_message":
		if !acknowledged {
			return f, errors.New("data before subscription acknowledgment")
		}
		if f.Data.Host != Collector && f.Data.Host != Collector+".ripe.net" {
			return f, fmt.Errorf("unexpected collector %q", f.Data.Host)
		}
	case "pong":
	case "ris_error":
		return f, fmt.Errorf("RIS error: %.256s", f.Data.Message)
	default:
		return f, fmt.Errorf("unexpected RIS frame type %.64q", f.Type)
	}
	return f, nil
}

// Read timestamps each bounded frame at receipt. A visit may block to backpressure the socket.
func Read(ctx context.Context, url string, visit func([]byte, time.Time) error) error {
	dial, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dial, url, nil)
	cancel()
	if err != nil {
		return err
	}
	defer func() { _ = conn.CloseNow() }()
	conn.SetReadLimit(MaxFrame)
	write, stop := context.WithTimeout(ctx, 10*time.Second)
	err = conn.Write(write, websocket.MessageText, []byte(Subscription))
	stop()
	if err != nil {
		return err
	}
	for {
		idle, end := context.WithTimeout(ctx, 30*time.Second)
		kind, raw, readErr := conn.Read(idle)
		now := time.Now().UTC()
		end()
		if readErr != nil {
			return readErr
		}
		if kind != websocket.MessageText {
			return errors.New("expected a RIS text frame")
		}
		if err = visit(raw, now); err != nil {
			return err
		}
	}
}
