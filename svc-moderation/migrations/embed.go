// Package migrations embeds the moderation schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

const LockID int64 = 71_008
