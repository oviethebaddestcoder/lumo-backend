// Package migrations embeds the messaging schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

const LockID int64 = 71_011
