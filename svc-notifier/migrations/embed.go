// Package migrations embeds the notifier schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

const LockID int64 = 71_007
