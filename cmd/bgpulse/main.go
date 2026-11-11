// Command bgpulse streams live BGP UPDATEs from one RIPE RIS collector and prints each announcement and withdrawal.
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

func main() {
	host := flag.String("host", "rrc21", "RIS collector to follow")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, *host)
	interrupted := ctx.Err() != nil
	stop()

	if err != nil && !interrupted {
		log.Fatal(err)
	}
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

	var msgs, ann, wd, invalid int
	start := time.Now()
	defer func() {
		log.Printf("%d messages, %d announcements, %d withdrawals, %d invalid in %s",
			msgs, ann, wd, invalid, time.Since(start).Round(time.Second))
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
