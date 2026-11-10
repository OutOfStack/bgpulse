// Command bgpulse streams live BGP UPDATEs from one RIPE RIS collector and prints each announcement and withdrawal.
package main

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

const streamURL = "https://ris-live.ripe.net/v1/stream/?format=json&client=bgpulse"

type risMessage struct {
	Type string `json:"type"`
	Data struct {
		Timestamp     float64 `json:"timestamp"`
		Peer          string  `json:"peer"`
		PeerASN       string  `json:"peer_asn"`
		Host          string  `json:"host"`
		Path          []any   `json:"path"` // ASNs; a trailing nested array is an AS_SET
		Announcements []struct {
			Prefixes []string `json:"prefixes"`
		} `json:"announcements"`
		Withdrawals []string `json:"withdrawals"`
	} `json:"data"`
}

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

	var msgs, ann, wd int
	start := time.Now()
	defer func() {
		log.Printf("%d messages, %d announcements, %d withdrawals in %s",
			msgs, ann, wd, time.Since(start).Round(time.Second))
	}()

	dec := jsontext.NewDecoder(resp.Body)
	for {
		var m risMessage
		if err := json.UnmarshalDecode(dec, &m); err != nil {
			return err
		}
		if m.Type != "ris_message" {
			continue
		}
		msgs++
		d := m.Data
		ts := time.UnixMilli(int64(d.Timestamp * 1000)).UTC().Format("15:04:05.000")
		peer := fmt.Sprintf("%s AS%s", d.Peer, d.PeerASN)
		for _, p := range d.Withdrawals {
			wd++
			fmt.Printf("%s %-14s W %-20s peer=%s\n", ts, d.Host, canonical(p), peer)
		}
		for _, a := range d.Announcements {
			for _, p := range a.Prefixes {
				ann++
				fmt.Printf("%s %-14s A %-20s origin=%s path=%s peer=%s\n",
					ts, d.Host, canonical(p), origin(d.Path), pathString(d.Path), peer)
			}
		}
	}
}

// canonical normalizes a CIDR (RIS may send uncompressed IPv6), leaving invalid input as-is.
func canonical(p string) string {
	if pfx, err := netip.ParsePrefix(p); err == nil {
		return pfx.Masked().String()
	}
	return p
}

// origin returns the last ASN of the path, or "?" when the path is empty or ends in an AS_SET.
func origin(path []any) string {
	if len(path) == 0 {
		return "?"
	}
	if asn, ok := path[len(path)-1].(float64); ok {
		return fmt.Sprintf("AS%d", int64(asn))
	}
	return "?"
}

func pathString(path []any) string {
	parts := make([]string, len(path))
	for i, hop := range path {
		switch v := hop.(type) {
		case float64:
			parts[i] = strconv.FormatInt(int64(v), 10)
		default: // AS_SET
			b, _ := json.Marshal(v)
			parts[i] = "{" + strings.Trim(string(b), "[]") + "}"
		}
	}
	return strings.Join(parts, " ")
}
