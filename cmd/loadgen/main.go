// Command loadgen drives the order service and prints a JSON report (SPEC 15).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"dualwrite-study/internal/config"
	"dualwrite-study/internal/load"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadLoadGen()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	report, err := load.Run(ctx, cfg)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}
