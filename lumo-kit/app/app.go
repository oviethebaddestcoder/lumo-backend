// Package app is the common entrypoint for every Lumo service binary:
//
//	<svc> serve    HTTP API (scale on request load)
//	<svc> worker   Kafka consumers + outbox relay (scale on partition count / lag)
//	<svc> migrate  apply DB migrations (Kubernetes Job before rollout)
//
// Each runs until SIGINT/SIGTERM, then shuts down gracefully.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

// Commands maps a subcommand to its run function.
type Commands map[string]func(ctx context.Context) error

// Main parses os.Args, runs the command and exits non-zero on error.
func Main(name string, cmds Commands) {
	if len(os.Args) < 2 || cmds[os.Args[1]] == nil {
		fmt.Fprintf(os.Stderr, "usage: %s <command>\ncommands:", name)
		for k := range cmds {
			fmt.Fprintf(os.Stderr, " %s", k)
		}
		fmt.Fprintln(os.Stderr)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := cmds[os.Args[1]](ctx); err != nil {
		slog.Error("exit", "service", name, "command", os.Args[1], "err", err)
		os.Exit(1)
	}
	slog.Info("stopped", "service", name, "command", os.Args[1])
}

// Group runs functions concurrently; the first error cancels the rest.
func Group(ctx context.Context, fns ...func(ctx context.Context) error) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, fn := range fns {
		g.Go(func() error { return fn(gctx) })
	}
	return g.Wait()
}
