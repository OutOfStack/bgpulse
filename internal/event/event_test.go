package event

import (
	"errors"
	"reflect"
	"testing"
)

// update has two withdrawals, two announcement groups, a prefix repeated
// across withdraw/announce, uncompressed IPv6, host bits set, a 32-bit ASN
// and prepending.
const update = `{"type":"ris_message","data":{
 "timestamp":1568279796.63,"peer":"192.0.2.1","peer_asn":"4200000000",
 "id":"21-192-0-2-1-11188782","host":"rrc21.ripe.net","type":"UPDATE",
 "path":[64500,64500,4200000001],
 "announcements":[
  {"next_hop":"192.0.2.1","prefixes":["198.51.100.0/24","203.0.113.7/24"]},
  {"next_hop":"2001:db8::1,fe80::1","prefixes":["2a06:de02:664:0:0:0:0:0/48"]}],
 "withdrawals":["198.51.100.0/24","2001:db8:0:0:0:0:0:0/32"]}}`

func TestNormalizeUpdate(t *testing.T) {
	m, err := Normalize([]byte(update))
	if err != nil {
		t.Fatal(err)
	}
	if m.EventTimeUS != 1568279796630000 {
		t.Errorf("EventTimeUS = %d", m.EventTimeUS)
	}
	if m.Collector != "rrc21" || m.PeerASN != 4200000000 || m.PeerIP.String() != "192.0.2.1" {
		t.Errorf("message fields = %+v", m)
	}
	// Golden IDs: these are a storage contract; changing them is a schema change.
	if m.MessageID != goldenMessageID || m.ObserverID != goldenObserverID || m.Children[0].EventID != goldenEventID {
		t.Errorf("ids changed: message=%s observer=%s event0=%s", m.MessageID, m.ObserverID, m.Children[0].EventID)
	}

	want := []struct{ action, prefix string }{
		{Withdraw, "198.51.100.0/24"},
		{Withdraw, "2001:db8::/32"},
		{Announce, "198.51.100.0/24"},
		{Announce, "203.0.113.0/24"},
		{Announce, "2a06:de02:664::/48"},
	}
	if len(m.Children) != len(want) {
		t.Fatalf("%d children, want %d", len(m.Children), len(want))
	}
	seen := map[string]bool{}
	for i, c := range m.Children {
		if c.Ordinal != i || c.Action != want[i].action || c.Prefix.String() != want[i].prefix {
			t.Errorf("child %d = %d %s %s, want %s %s", i, c.Ordinal, c.Action, c.Prefix, want[i].action, want[i].prefix)
		}
		if seen[c.EventID] {
			t.Errorf("child %d: duplicate event id", i)
		}
		seen[c.EventID] = true
	}
	a := m.Children[4]
	if a.Path.String() != "64500 64500 4200000001" || a.OriginASN != 4200000001 || a.OriginResolution != OriginUnique {
		t.Errorf("announce path=%q origin=%d %s", a.Path, a.OriginASN, a.OriginResolution)
	}
	if len(a.NextHops) != 2 || a.NextHops[1].String() != "fe80::1" {
		t.Errorf("next hops = %v", a.NextHops)
	}

	again, _ := Normalize([]byte(update))
	if !reflect.DeepEqual(m, again) {
		t.Error("same input normalized differently")
	}
}

func TestNormalizeOrigin(t *testing.T) {
	cases := []struct{ path, str, resolution string }{
		{`[64500,[64510,64511]]`, "64500 {64510,64511}", OriginASSet},
		{`[]`, "", OriginNone},
	}
	for _, c := range cases {
		m, err := Normalize([]byte(`{"type":"ris_message","data":{"timestamp":1,"peer":"192.0.2.1","peer_asn":"64500",
			"id":"x","host":"rrc21","type":"UPDATE","path":` + c.path + `,
			"announcements":[{"next_hop":"192.0.2.1","prefixes":["192.0.2.0/24"]}]}}`))
		if err != nil {
			t.Fatal(c.path, err)
		}
		ch := m.Children[0]
		if ch.Path.String() != c.str || ch.OriginResolution != c.resolution || ch.OriginASN != 0 {
			t.Errorf("%s: path=%q resolution=%s origin=%d", c.path, ch.Path, ch.OriginResolution, ch.OriginASN)
		}
	}
}

func TestNormalizeOutcomes(t *testing.T) {
	const head = `{"type":"ris_message","data":{"timestamp":1568279796.63,"peer":"192.0.2.1","peer_asn":"64500","id":"x","host":"rrc21",`
	cases := []struct {
		name, raw    string
		children     int
		err, notData bool
	}{
		{"zero-prefix update", head + `"type":"UPDATE","path":[]}}`, 0, false, false},
		{"peer state", head + `"type":"RIS_PEER_STATE","state":"down"}}`, 0, false, false},
		{"control frame", `{"type":"ris_subscribe_ok","data":{}}`, 0, true, true},
		{"invalid prefix", head + `"type":"UPDATE","withdrawals":["192.0.2.0/24","nope"]}}`, 0, true, false},
		{"asn too large", head + `"type":"UPDATE","path":[4294967296],"announcements":[{"next_hop":"192.0.2.1","prefixes":["192.0.2.0/24"]}]}}`, 0, true, false},
		{"string in path", head + `"type":"UPDATE","path":["64500"],"announcements":[{"next_hop":"192.0.2.1","prefixes":["192.0.2.0/24"]}]}}`, 0, true, false},
		{"bad timestamp", `{"type":"ris_message","data":{"timestamp":1.5e9,"peer":"192.0.2.1","peer_asn":"1","id":"x","host":"rrc21","type":"UPDATE"}}`, 0, true, false},
		{"missing id", `{"type":"ris_message","data":{"timestamp":1,"peer":"192.0.2.1","peer_asn":"1","host":"rrc21","type":"UPDATE"}}`, 0, true, false},
		{"sub-microsecond timestamp", `{"type":"ris_message","data":{"timestamp":1.1234567,"peer":"192.0.2.1","peer_asn":"1","id":"x","host":"rrc21","type":"UPDATE"}}`, 0, true, false},
		{"fractional asn", head + `"type":"UPDATE","path":[64500.5]` + ann + `}}`, 0, true, false},
		{"negative asn", head + `"type":"UPDATE","path":[-1]` + ann + `}}`, 0, true, false},
		{"empty as_set", head + `"type":"UPDATE","path":[64500,[]]` + ann + `}}`, 0, true, false},
		{"bad path, withdrawal only", head + `"type":"UPDATE","path":["bad-asn"],"withdrawals":["192.0.2.0/24"]}}`, 0, true, false},
		{"missing prefixes", head + `"type":"UPDATE","path":[64500],"withdrawals":["192.0.2.0/24"],"announcements":[{"next_hop":"192.0.2.1"}]}}`, 0, true, false},
		{"null prefixes", head + `"type":"UPDATE","path":[64500],"withdrawals":["192.0.2.0/24"],"announcements":[{"next_hop":"192.0.2.1","prefixes":null}]}}`, 0, true, false},
		{"empty prefixes", head + `"type":"UPDATE","path":[64500],"withdrawals":["192.0.2.0/24"],"announcements":[{"next_hop":"192.0.2.1","prefixes":[]}]}}`, 1, false, false},
		{"bad next hop", head + `"type":"UPDATE","path":[64500],"announcements":[{"next_hop":"nope","prefixes":["192.0.2.0/24"]}]}}`, 0, true, false},
	}
	for _, c := range cases {
		m, err := Normalize([]byte(c.raw))
		if (err != nil) != c.err || errors.Is(err, ErrNotData) != c.notData {
			t.Errorf("%s: err = %v", c.name, err)
			continue
		}
		if err == nil && len(m.Children) != c.children {
			t.Errorf("%s: %d children", c.name, len(m.Children))
		}
	}
}

// Same observer as the update fixture, written non-canonically: mapped IPv4 peer, zero-padded ASN.
func TestObserverIDCanonical(t *testing.T) {
	m, err := Normalize([]byte(`{"type":"ris_message","data":{"timestamp":1,"peer":"::ffff:192.0.2.1",
		"peer_asn":"04200000000","id":"y","host":"rrc21","type":"UPDATE"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.ObserverID != goldenObserverID {
		t.Errorf("ObserverID = %s, want %s", m.ObserverID, goldenObserverID)
	}
}

const ann = `,"announcements":[{"next_hop":"192.0.2.1","prefixes":["192.0.2.0/24"]}]`

const (
	goldenMessageID  = "eb7b53daf4c03a5fbff4a50f5f3e22486206424a0f01449c2d760416e40d1abe"
	goldenObserverID = "2900f78ba093e4d9cd1160b7023fea3b26fedde1ed00d4b9d05a1899d0c87004"
	goldenEventID    = "4ea67de411a4fa19b2ba848e372afafa346bb5566ca7c3d1d2c5c34ea4149408"
)
