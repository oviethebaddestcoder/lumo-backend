// Package migrations embeds the SQL schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

// LockID is this service's advisory lock key for migrations.
const LockID int64 = 71_001
