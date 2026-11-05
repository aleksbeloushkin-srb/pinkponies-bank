// Package migrations embeds the transfers service's SQL schema, applied at
// start with goose.
package migrations

import "embed"

// FS holds the migration files in goose format.
//
//go:embed *.sql
var FS embed.FS
