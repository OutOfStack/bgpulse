// Package event turns one RIS Live message into a normalized message with
// deterministic per-prefix children.
package event

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"
)

// ErrNotData marks transport/control frames (ris_subscribe_ok, pong, ris_error, ...).
var ErrNotData = errors.New("not a ris_message")

const source = "ris"

// Origin resolutions.
const (
	OriginUnique = "unique" // path ends in a single ASN
	OriginASSet  = "as_set" // path ends in an AS_SET: several candidates, none chosen
	OriginNone   = "none"   // empty path
)

// Actions.
const (
	Withdraw = "withdraw"
	Announce = "announce"
)

// Message is one normalized RIS source message.
type Message struct {
	Source          string
	Collector       string
	SourceMessageID string
	MessageID       string
	Type            string // data.type: UPDATE, RIS_PEER_STATE, OPEN, ...
	EventTimeUS     int64
	PeerIP          netip.Addr
	PeerASN         uint32
	ObserverID      string
	Children        []Child
}

// Child is one prefix withdrawal or announcement derived from a Message.
type Child struct {
	EventID          string
	Ordinal          int
	Action           string
	Prefix           netip.Prefix
	NextHops         []netip.Addr // announcements only
	Path             Path         // announcements only
	OriginASN        uint32       // set only when OriginResolution == OriginUnique
	OriginResolution string       // announcements only
}

// Segment is one AS_PATH element: a single ASN, or an AS_SET when Set is true.
type Segment struct {
	ASNs []uint32
	Set  bool
}

// Path is an AS_PATH in source order.
type Path []Segment

func (p Path) String() string {
	parts := make([]string, len(p))
	for i, s := range p {
		asns := make([]string, len(s.ASNs))
		for j, a := range s.ASNs {
			asns[j] = strconv.FormatUint(uint64(a), 10)
		}
		parts[i] = strings.Join(asns, ",")
		if s.Set {
			parts[i] = "{" + parts[i] + "}"
		}
	}
	return strings.Join(parts, " ")
}

type risMessage struct {
	Type string `json:"type"`
	Data struct {
		ID            string         `json:"id"`
		Type          string         `json:"type"`
		Timestamp     jsontext.Value `json:"timestamp"`
		Peer          string         `json:"peer"`
		PeerASN       string         `json:"peer_asn"`
		Host          string         `json:"host"`
		Path          []any          `json:"path"`
		Announcements []struct {
			NextHop  string   `json:"next_hop"`
			Prefixes []string `json:"prefixes"`
		} `json:"announcements"`
		Withdrawals []string `json:"withdrawals"`
	} `json:"data"`
}

// Normalize parses one RIS Live frame. Any invalid required field rejects the
// whole message so a malformed subset is never treated as complete.
func Normalize(raw []byte) (*Message, error) {
	var r risMessage
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	if r.Type != "ris_message" {
		return nil, fmt.Errorf("%w: %q", ErrNotData, r.Type)
	}
	d := r.Data
	if d.ID == "" || d.Host == "" || d.Type == "" {
		return nil, errors.New("missing id, host or type")
	}
	ts, err := parseMicros(d.Timestamp)
	if err != nil {
		return nil, err
	}
	peer, err := netip.ParseAddr(d.Peer)
	if err != nil {
		return nil, fmt.Errorf("peer: %w", err)
	}
	peer = peer.Unmap()
	peerASN, err := strconv.ParseUint(d.PeerASN, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("peer_asn: %w", err)
	}

	m := &Message{
		Source:          source,
		Collector:       strings.SplitN(d.Host, ".", 2)[0],
		SourceMessageID: d.ID,
		Type:            d.Type,
		EventTimeUS:     ts,
		PeerIP:          peer,
		PeerASN:         uint32(peerASN),
	}
	m.MessageID = hashID(source, d.ID)
	m.ObserverID = hashID(source, m.Collector, peer.String(), strconv.FormatUint(peerASN, 10))
	if d.Type != "UPDATE" {
		return m, nil
	}

	for i, p := range d.Withdrawals {
		pfx, perr := netip.ParsePrefix(p)
		if perr != nil {
			return nil, fmt.Errorf("withdrawal %d: %w", i, perr)
		}
		m.add(Child{Action: Withdraw, Prefix: pfx.Masked()}, 0, i)
	}
	path, err := parsePath(d.Path) // validated even when only withdrawals use the message
	if err != nil {
		return nil, err
	}
	origin, resolution := resolveOrigin(path)
	for g, a := range d.Announcements {
		if a.Prefixes == nil { // missing or null; an explicit [] is allowed
			return nil, fmt.Errorf("announcement %d: missing prefixes", g)
		}
		var hops []netip.Addr
		for h := range strings.SplitSeq(a.NextHop, ",") { // IPv6 may carry global,link-local
			addr, perr := netip.ParseAddr(strings.TrimSpace(h))
			if perr != nil {
				return nil, fmt.Errorf("announcement %d next_hop: %w", g, perr)
			}
			hops = append(hops, addr)
		}
		for i, p := range a.Prefixes {
			pfx, perr := netip.ParsePrefix(p)
			if perr != nil {
				return nil, fmt.Errorf("announcement %d prefix %d: %w", g, i, perr)
			}
			m.add(Child{
				Action: Announce, Prefix: pfx.Masked(), NextHops: hops,
				Path: path, OriginASN: origin, OriginResolution: resolution,
			}, g, i)
		}
	}
	return m, nil
}

func (m *Message) add(c Child, group, index int) {
	c.Ordinal = len(m.Children)
	c.EventID = hashID(m.MessageID, c.Action, strconv.Itoa(group), strconv.Itoa(index))
	m.Children = append(m.Children, c)
}

// hashID is SHA-256 over length-prefixed parts, so ("ab","c") != ("a","bc").
func hashID(parts ...string) string {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// parseMicros converts the JSON number text to microseconds without float rounding.
func parseMicros(v jsontext.Value) (int64, error) {
	s := string(v)
	whole, frac, _ := strings.Cut(s, ".")
	if len(frac) > 6 || strings.ContainsAny(s, "eE-") {
		return 0, fmt.Errorf("timestamp %q: unsupported format", s)
	}
	sec, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("timestamp %q: %w", s, err)
	}
	us := int64(0)
	if frac != "" {
		if us, err = strconv.ParseInt(frac+strings.Repeat("0", 6-len(frac)), 10, 64); err != nil {
			return 0, fmt.Errorf("timestamp %q: %w", s, err)
		}
	}
	if sec > math.MaxInt64/1_000_000-1 {
		return 0, fmt.Errorf("timestamp %q: out of range", s)
	}
	return sec*1_000_000 + us, nil
}

func parsePath(raw []any) (Path, error) {
	path := make(Path, len(raw))
	for i, hop := range raw {
		switch v := hop.(type) {
		case float64:
			a, err := asn(v)
			if err != nil {
				return nil, fmt.Errorf("path[%d]: %w", i, err)
			}
			path[i] = Segment{ASNs: []uint32{a}}
		case []any:
			if len(v) == 0 {
				return nil, fmt.Errorf("path[%d]: empty AS_SET", i)
			}
			seg := Segment{Set: true}
			for _, x := range v {
				f, ok := x.(float64)
				if !ok {
					return nil, fmt.Errorf("path[%d]: AS_SET member %v", i, x)
				}
				a, err := asn(f)
				if err != nil {
					return nil, fmt.Errorf("path[%d]: %w", i, err)
				}
				seg.ASNs = append(seg.ASNs, a)
			}
			path[i] = seg
		default:
			return nil, fmt.Errorf("path[%d]: unexpected %T", i, hop)
		}
	}
	return path, nil
}

func asn(f float64) (uint32, error) {
	if f < 0 || f > math.MaxUint32 || f != math.Trunc(f) {
		return 0, fmt.Errorf("invalid ASN %v", f)
	}
	return uint32(f), nil
}

func resolveOrigin(p Path) (uint32, string) {
	if len(p) == 0 {
		return 0, OriginNone
	}
	if last := p[len(p)-1]; !last.Set {
		return last.ASNs[0], OriginUnique
	}
	return 0, OriginASSet
}
