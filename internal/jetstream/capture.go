package jetstream

import (
	"context"
	"errors"
	"fmt"

	natsjs "github.com/nats-io/nats.go/jetstream"
)

// Capture publishes and looks up raw records on a single provisioned collector subject.
type Capture struct {
	js            natsjs.JetStream
	stream        natsjs.Stream
	name, subject string
}

// OpenCapture verifies the raw stream without provisioning resources.
func OpenCapture(ctx context.Context, js natsjs.JetStream, ns, collector string) (*Capture, error) {
	stream, err := checkStream(ctx, js, RawStream(ns))
	if err != nil {
		return nil, err
	}
	return &Capture{js: js, stream: stream, name: stream.CachedInfo().Config.Name, subject: RawSubject(ns, collector)}, nil
}

// Last reads the authoritative tail for the collector, or nil if empty.
func (c *Capture) Last(ctx context.Context) ([]byte, error) {
	msg, err := c.stream.GetLastMsgForSubject(ctx, c.subject)
	if errors.Is(err, natsjs.ErrMsgNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

// Publish requires a server acknowledgment and reuses id for deduplication.
func (c *Capture) Publish(ctx context.Context, id string, data []byte) error {
	ack, err := c.js.Publish(ctx, c.subject, data, natsjs.WithMsgID(id), natsjs.WithExpectStream(c.name))
	if err != nil {
		return err
	}
	if ack.Stream != c.name || ack.Sequence == 0 {
		return fmt.Errorf("invalid PubAck for %s: %+v", c.subject, ack)
	}
	return nil
}
