// Command bgpulse is the BGPulse binary. Its subcommands are:
//
//	watch  stream live BGP UPDATEs from one RIPE RIS collector and print them
//	setup  create or check the JetStream streams and Scylla schema
package main

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/outofstack/bgpulse/internal/event"
)

const streamURL = "https://ris-live.ripe.net/v1/stream/?format=json&client=bgpulse"

const usage = "usage: bgpulse <watch|setup> [flags]"

func main() {
	if len(os.Args) < 2 {
		log.Fatal(usage)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	var err error
	switch os.Args[1] {
	case "watch":
		err = watch(ctx, os.Args[2:])
	case "setup":
		err = setup(ctx, os.Args[2:])
	default:
		err = errors.New(usage)
	}
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Fatal(err)
	}
}

func watch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	host := fs.String("host", "rrc21", "RIS collector to follow")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// Ctrl-C is the normal way to stop watching, not an error.
	if err := run(ctx, *host); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func run(ctx context.Context, host string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-RIS-Subscribe", fmt.Sprintf(`{"host":%q,"type":"UPDATE"}`, host))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ris live: %s", resp.Status)
	}
	log.Printf("streaming UPDATEs from %s (Ctrl-C to stop)", host)

	var msgs, ann, wd, invalid, payloadBytes int
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		n := float64(max(msgs, 1))
		log.Printf("%d messages, %d announcements, %d withdrawals, %d invalid in %s",
			msgs, ann, wd, invalid, elapsed.Round(time.Second))
		log.Printf("%.1f msg/s, %.2f prefixes/msg, %.0f payload B/msg, %d payload bytes total",
			float64(msgs)/elapsed.Seconds(), float64(ann+wd)/n, float64(payloadBytes)/n, payloadBytes)
	}()

	dec := jsontext.NewDecoder(resp.Body)
	for {
		raw, err := dec.ReadValue()
		if err != nil {
			return err
		}
		m, err := event.Normalize(raw)
		if errors.Is(err, event.ErrNotData) {
			continue
		}
		if err != nil {
			invalid++
			log.Printf("invalid message: %v", err)
			continue
		}
		msgs++
		payloadBytes += len(raw)
		ts := time.UnixMicro(m.EventTimeUS).UTC().Format("15:04:05.000")
		peer := fmt.Sprintf("%s AS%d", m.PeerIP, m.PeerASN)
		for _, c := range m.Children {
			if c.Action == event.Withdraw {
				wd++
				fmt.Printf("%s %-14s W %-20s peer=%s\n", ts, m.Collector, c.Prefix, peer)
				continue
			}
			ann++
			origin := "?"
			if c.OriginResolution == event.OriginUnique {
				origin = fmt.Sprintf("AS%d", c.OriginASN)
			}
			fmt.Printf("%s %-14s A %-20s origin=%s path=%s peer=%s\n",
				ts, m.Collector, c.Prefix, origin, c.Path, peer)
		}
	}
}
