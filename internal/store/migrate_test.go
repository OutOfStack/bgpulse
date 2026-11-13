package store

import (
	"testing"
	"testing/fstest"

	"github.com/outofstack/bgpulse/migrations"
)

func TestLoad(t *testing.T) {
	migs, err := Load(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) == 0 || migs[0].Version != 1 || migs[0].Name != "raw_messages_v1" {
		t.Fatalf("embedded migrations: %+v", migs)
	}

	for name, fsys := range map[string]fstest.MapFS{
		"gap":       {"0001_a.cql": {}, "0003_c.cql": {}},
		"no name":   {"0001.cql": {}},
		"short num": {"1_a.cql": {}},
	} {
		if _, err := Load(fsys); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestPending(t *testing.T) {
	migs := []Migration{{Version: 1, Checksum: "a"}, {Version: 2, Checksum: "b"}, {Version: 3, Checksum: "c"}}
	tests := []struct {
		name    string
		applied map[int]string
		want    int // first pending version, 0 = none
		wantErr bool
	}{
		{"fresh", map[int]string{}, 1, false},
		{"partial", map[int]string{1: "a"}, 2, false},
		{"done", map[int]string{1: "a", 2: "b", 3: "c"}, 0, false},
		{"changed", map[int]string{1: "a", 2: "x"}, 0, true},
		{"gap", map[int]string{1: "a", 3: "c"}, 0, true},
		{"unknown", map[int]string{1: "a", 2: "b", 3: "c", 4: "d"}, 0, true},
	}
	for _, tt := range tests {
		got, err := pending(migs, tt.applied)
		if (err != nil) != tt.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		first := 0
		if len(got) > 0 {
			first = got[0].Version
		}
		if first != tt.want {
			t.Errorf("%s: first pending = %d, want %d", tt.name, first, tt.want)
		}
	}
}

func TestStatements(t *testing.T) {
	got := statements("CREATE TABLE {{keyspace}}.a (x int PRIMARY KEY);\n\nCREATE TABLE {{keyspace}}.b (y int PRIMARY KEY);\n", "ks")
	want := []string{"CREATE TABLE ks.a (x int PRIMARY KEY)", "CREATE TABLE ks.b (y int PRIMARY KEY)"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %q", got)
	}
}
