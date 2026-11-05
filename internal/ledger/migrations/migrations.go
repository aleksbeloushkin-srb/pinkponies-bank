// Package migrations embeds the ledger's SQL schema. The ledger binary
// applies it at start with goose, so a fresh database needs no manual step.
package migrations

import "embed"

// FS holds the migration files in goose format.
//
//go:embed *.sql
var FS embed.FS
