// Package migrations embeds the ordered CQL migrations applied by bgpulse setup.
//
// Files are named NNNN_name.cql, numbered from 0001 without gaps. Statements
// end with ";" and write {{keyspace}} where the namespace keyspace goes. An
// applied file must never change: its checksum is recorded.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.cql
var FS embed.FS
