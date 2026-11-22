package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"slices"
	"time"

	"github.com/coder/websocket"
	"github.com/outofstack/bgpulse/internal/event"
	"github.com/outofstack/bgpulse/internal/ris"
)

const sampleURL = "wss://ris-live.ripe.net/v1/ws/?client=bgpulse-sample"

// A frame is capped at 1 MiB, bounding the number of distinct sizes and child counts.
type histogram map[int]int64

func (h histogram) summary() map[string]int64 {
	var count, sum, seen int64
	for value, n := range h {
		count += n
		sum += int64(value) * n
	}
	result := map[string]int64{"count": count, "sum": sum}
	for _, v := range slices.Sorted(maps.Keys(h)) {
		seen += h[v]
		for _, p := range []int64{50, 95, 99} {
			if prev := seen - h[v]; prev*100 < p*count && seen*100 >= p*count {
				result[fmt.Sprintf("p%d", p)] = int64(v)
			}
		}
		result["max"] = int64(v)
	}
	return result
}

type sampleStats struct {
	sizes, fanout histogram
	types         map[string]int64
	typeOverflow  int64
	invalid       int64
}

type sampleWindow struct {
	Half         int       `json:"half"`
	Acknowledged time.Time `json:"acknowledged,omitzero"`
	First        time.Time `json:"first_data,omitzero"`
	Last         time.Time `json:"last_data,omitzero"`
	Messages     int64     `json:"messages"`
	End          string    `json:"end"`
}

func (s *sampleStats) observe(raw []byte, w *sampleWindow, now time.Time) error {
	f, err := ris.Decode(raw, !w.Acknowledged.IsZero())
	if err != nil {
		return err
	}
	switch f.Type {
	case "ris_subscribe_ok":
		w.Acknowledged = now
	case "ris_message":
		if w.Messages == 0 {
			w.First = now
		}
		w.Last = now
		w.Messages++
		s.sizes[len(raw)]++
		m, err := event.Normalize(raw)
		if err != nil {
			s.invalid++
			break
		}
		if _, tracked := s.types[m.Type]; tracked || (len(s.types) < 16 && len(m.Type) <= 64) {
			s.types[m.Type]++
		} else {
			s.typeOverflow++
		}
		s.fanout[len(m.Children)]++
	}
	return nil
}

func (s *sampleStats) readWindow(ctx context.Context, url string, w *sampleWindow) error {
	return ris.Read(ctx, url, func(raw []byte, now time.Time) error { return s.observe(raw, w, now) })
}

func retrySample(err error) bool {
	var networkErr net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &networkErr) ||
		slices.Contains([]websocket.StatusCode{websocket.StatusNormalClosure, websocket.StatusGoingAway,
			websocket.StatusServiceRestart, websocket.StatusInternalError, websocket.StatusTryAgainLater}, websocket.CloseStatus(err))
}

func (s *sampleStats) captureHalf(ctx context.Context, url string, half int) ([]sampleWindow, error) {
	var windows []sampleWindow
	var messages int64
	for ctx.Err() == nil {
		w := sampleWindow{Half: half}
		err := s.readWindow(ctx, url, &w)
		w.End = err.Error()
		expired := errors.Is(ctx.Err(), context.DeadlineExceeded) &&
			(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed))
		if expired {
			w.End = "deadline"
		}
		windows = append(windows, w)
		messages += w.Messages
		log.Printf("half %d session %d: %d messages, %s", half, len(windows), w.Messages, w.End)
		if expired {
			break
		}
		if !retrySample(err) || len(windows) >= 16 {
			return windows, err
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	if messages == 0 {
		return windows, fmt.Errorf("no source messages: %w", ctx.Err())
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return windows, ctx.Err()
	}
	return windows, nil
}

func sample(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sample", flag.ContinueOnError)
	duration := fs.Duration("duration", 30*time.Minute, "total wall time, reconnecting halfway through")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *duration <= 0 || fs.NArg() != 0 {
		return errors.New("sample requires a positive duration and no positional arguments")
	}
	return measureSample(ctx, sampleURL, *duration, os.Stdout)
}

func measureSample(ctx context.Context, url string, duration time.Duration, out io.Writer) error {
	start := time.Now()
	stats := sampleStats{sizes: histogram{}, fanout: histogram{}, types: map[string]int64{}}
	var windows []sampleWindow
	var captureErr error
	for i, deadline := range []time.Time{start.Add(duration / 2), start.Add(duration)} {
		halfCtx, cancel := context.WithDeadline(ctx, deadline)
		observed, err := stats.captureHalf(halfCtx, url, i+1)
		cancel()
		windows = append(windows, observed...)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			captureErr = fmt.Errorf("sample half %d: %w", i+1, err)
			break
		}
	}
	elapsed := time.Since(start).Seconds()
	sizes := stats.sizes.summary()
	fanout := stats.fanout.summary()
	var rawRate, prefixRate float64
	if elapsed > 0 {
		rawRate = float64(sizes["count"]) / elapsed
		prefixRate = float64(fanout["sum"]) / elapsed
	}
	writeErr := json.MarshalWrite(out, map[string]any{
		"complete":  captureErr == nil,
		"collector": ris.Collector, "started": start.UTC(), "elapsed_seconds": elapsed, "windows": windows,
		"raw_messages_per_second":  rawRate,
		"prefix_events_per_second": prefixRate,
		"payload_bytes":            sizes, "prefixes_per_valid_message": fanout,
		"valid_message_types": stats.types, "type_overflow_messages": stats.typeOverflow, "invalid_messages": stats.invalid,
	})
	return errors.Join(captureErr, writeErr)
}
