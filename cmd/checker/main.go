// Command checker compares one cycle's Postgres and Kafka state and prints the
// result as JSON (SPEC 11).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"dualwrite-study/internal/checker"
	"dualwrite-study/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "checker:", err)
		os.Exit(1)
	}
}

func run() error {
	cycle := flag.String("cycle", "", "cycle ID; the topic is orders.<cycle>")
	logs := flag.String("logs", "", "file holding the cycle's component logs")
	consumers := flag.String("consumers", "", "comma-separated consumer strategies to check")
	damagePath := flag.String("damage", "", "file to write the damaged pairs to")
	timeout := flag.Duration("timeout", 120*time.Second, "limit for the whole check")
	flag.Parse()
	if *cycle == "" {
		return errors.New("-cycle is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var logReader io.Reader
	if *logs != "" {
		file, err := os.Open(*logs)
		if err != nil {
			return fmt.Errorf("open logs: %w", err)
		}
		defer func() { _ = file.Close() }()
		logReader = file
	}
	var strategies []string
	if *consumers != "" {
		strategies = strings.Split(*consumers, ",")
	}
	measurement, err := config.LoadMeasurement()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	in, err := checker.Collect(ctx, measurement.PostgresDSN, measurement.KafkaBrokers, "orders."+*cycle, logReader, strategies)
	if err != nil {
		return err
	}

	result, damage := checker.Check(in)
	if *damagePath != "" {
		raw, err := json.MarshalIndent(damage, "", "  ")
		if err != nil {
			return fmt.Errorf("encode damage: %w", err)
		}
		if err := os.WriteFile(*damagePath, raw, 0o644); err != nil {
			return fmt.Errorf("write damage: %w", err)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
