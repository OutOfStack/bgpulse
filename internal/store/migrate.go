// Package store owns the Scylla keyspace of one namespace and its migrations.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// Migration is one ordered CQL file.
type Migration struct {
	Version  int
	Name     string
	Checksum string
	CQL      string
}

// Keyspace is the keyspace for namespace ns. The caller validates ns.
func Keyspace(ns string) string { return "bgpulse_" + ns }

// Load reads NNNN_name.cql files from fsys and requires versions 1..n without gaps.
func Load(fsys fs.FS) ([]Migration, error) {
	files, err := fs.Glob(fsys, "*.cql")
	if err != nil {
		return nil, err
	}
	migs := make([]Migration, 0, len(files))
	for i, f := range files { // fs.Glob returns sorted names
		num, name, ok := strings.Cut(strings.TrimSuffix(f, path.Ext(f)), "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil || len(num) != 4 || v != i+1 {
			return nil, fmt.Errorf("migration %s: want %04d_<name>.cql", f, i+1)
		}
		b, err := fs.ReadFile(fsys, f)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		migs = append(migs, Migration{Version: v, Name: name, Checksum: hex.EncodeToString(sum[:]), CQL: string(b)})
	}
	return migs, nil
}

// pending returns the migrations not yet applied. Applied must be an unchanged
// prefix of migs: a changed checksum, a version this binary does not know, or a
// gap is drift and fails.
func pending(migs []Migration, applied map[int]string) ([]Migration, error) {
	if len(applied) > len(migs) {
		return nil, fmt.Errorf("schema has %d migrations, binary knows %d", len(applied), len(migs))
	}
	for i, m := range migs {
		sum, ok := applied[m.Version]
		if !ok {
			if i != len(applied) {
				return nil, fmt.Errorf("migration %d not applied but later ones are", m.Version)
			}
			return migs[i:], nil
		}
		if sum != m.Checksum {
			return nil, fmt.Errorf("migration %d (%s) changed after it was applied", m.Version, m.Name)
		}
	}
	return nil, nil
}

// statements splits a migration into single statements for the driver.
func statements(cql, keyspace string) []string {
	var out []string
	for s := range strings.SplitSeq(cql, ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, strings.ReplaceAll(s, "{{keyspace}}", keyspace))
		}
	}
	return out
}

// Migrate creates the namespace keyspace (RF 1 in the local datacenter) and
// applies pending migrations in order. It returns the migrations it applied.
func Migrate(ctx context.Context, s *gocql.Session, ns string, migs []Migration) ([]Migration, error) {
	ks := Keyspace(ns)
	var dc string
	if err := s.Query(`SELECT data_center FROM system.local`).WithContext(ctx).Scan(&dc); err != nil {
		return nil, fmt.Errorf("read datacenter: %w", err)
	}
	ddl := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': 1}`, ks, dc),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.schema_migrations (
			version int PRIMARY KEY, name text, checksum text, applied_at timestamp)`, ks),
	}
	for _, q := range ddl {
		if err := exec(ctx, s, q); err != nil {
			return nil, err
		}
	}
	if err := checkReplication(ctx, s, ks, dc); err != nil {
		return nil, err
	}
	todo, err := pendingIn(ctx, s, ks, migs)
	if err != nil {
		return nil, err
	}
	for _, m := range todo {
		for _, q := range statements(m.CQL, ks) {
			if err := exec(ctx, s, q); err != nil {
				return nil, fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
			}
		}
		insert := fmt.Sprintf(`INSERT INTO %s.schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`, ks)
		if err := s.Query(insert, m.Version, m.Name, m.Checksum, time.Now()).WithContext(ctx).Exec(); err != nil {
			return nil, fmt.Errorf("record migration %d: %w", m.Version, err)
		}
	}
	return todo, nil
}

// Check reports an error if the keyspace is missing, has other replication,
// or differs from migs. It changes nothing.
func Check(ctx context.Context, s *gocql.Session, ns string, migs []Migration) error {
	ks := Keyspace(ns)
	var dc string
	if err := s.Query(`SELECT data_center FROM system.local`).WithContext(ctx).Scan(&dc); err != nil {
		return fmt.Errorf("read datacenter: %w", err)
	}
	if err := checkReplication(ctx, s, ks, dc); err != nil {
		return err
	}
	todo, err := pendingIn(ctx, s, ks, migs)
	if err != nil {
		return err
	}
	if len(todo) > 0 {
		return fmt.Errorf("keyspace %s: %d migrations not applied, run setup", ks, len(todo))
	}
	return nil
}

func exec(ctx context.Context, s *gocql.Session, q string) error {
	if err := s.Query(q).WithContext(ctx).Exec(); err != nil {
		return err
	}
	return s.AwaitSchemaAgreement(ctx)
}

// checkReplication fails unless ks exists with exactly the declared replication,
// because CREATE KEYSPACE IF NOT EXISTS keeps whatever was there before.
func checkReplication(ctx context.Context, s *gocql.Session, ks, dc string) error {
	var repl map[string]string
	err := s.Query(`SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?`, ks).WithContext(ctx).Scan(&repl)
	if errors.Is(err, gocql.ErrNotFound) {
		return fmt.Errorf("keyspace %s does not exist, run setup", ks)
	}
	if err != nil {
		return fmt.Errorf("read keyspace %s: %w", ks, err)
	}
	if !strings.HasSuffix(repl["class"], "NetworkTopologyStrategy") || repl[dc] != "1" || len(repl) != 2 {
		return fmt.Errorf("keyspace %s has replication %v, want NetworkTopologyStrategy {%s: 1}", ks, repl, dc)
	}
	return nil
}

func pendingIn(ctx context.Context, s *gocql.Session, ks string, migs []Migration) ([]Migration, error) {
	applied := map[int]string{}
	it := s.Query(fmt.Sprintf(`SELECT version, checksum FROM %s.schema_migrations`, ks)).WithContext(ctx).Iter()
	var v int
	var sum string
	for it.Scan(&v, &sum) {
		applied[v] = sum
	}
	if err := it.Close(); err != nil {
		return nil, fmt.Errorf("read %s.schema_migrations: %w", ks, err)
	}
	return pending(migs, applied)
}
