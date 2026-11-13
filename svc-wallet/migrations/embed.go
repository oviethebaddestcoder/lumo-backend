// Package migrations embeds the wallet schema (applied to every shard).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

const LockID int64 = 71_003
