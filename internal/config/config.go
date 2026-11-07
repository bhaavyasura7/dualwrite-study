// Package config loads every tunable from the environment. Defaults match the
// `defaults.env` block of experiments/vectors.yaml.
package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config holds the settings shared by the Go components.
type Config struct {
	CycleID string // CYCLE_ID, required; names the topic orders.<cycle_id>
	Pattern string // PATTERN, required; P1..P5

	HTTPAddr        string        // HTTP_ADDR, default :8080
	HandlerTimeout  time.Duration // HANDLER_TIMEOUT_MS, default 5000
	ShutdownTimeout time.Duration // SHUTDOWN_TIMEOUT_MS, default 8000; below docker stop's 10 s grace

	PostgresDSN string // POSTGRES_DSN, default through the pg-app proxy
	PGMaxConns  int32  // PG_MAX_CONNS, default 20

	KafkaBrokers    []string      // KAFKA_BROKERS, comma separated, default through the kafka-app proxy
	KafkaAcks       string        // KAFKA_ACKS, "all" or "1", default all
	KafkaIdempotent bool          // KAFKA_IDEMPOTENT, default true
	KafkaLinger     time.Duration // KAFKA_LINGER_MS, default 5
	KafkaSyncSend   bool          // KAFKA_SYNC_SEND, default false
	// KafkaRequestTimeout is the produce timeout sent to the broker.
	KafkaRequestTimeout time.Duration // KAFKA_REQUEST_TIMEOUT_MS, default 3000
	// KafkaRequestOverhead is added to KafkaRequestTimeout for the client-side
	// connection deadline. See docs/SPEC_NOTES.md.
	KafkaRequestOverhead time.Duration // KAFKA_REQUEST_OVERHEAD_MS, default 1000
	KafkaDeliveryTimeout time.Duration // KAFKA_DELIVERY_TIMEOUT_MS, default 5000
}

// Topic returns the cycle's Kafka topic.
func (c Config) Topic() string { return "orders." + c.CycleID }

// Load reads Config from the environment and reports every invalid value at once.
func Load() (Config, error) {
	var errs []error
	cfg := Config{
		CycleID:              os.Getenv("CYCLE_ID"),
		Pattern:              os.Getenv("PATTERN"),
		HTTPAddr:             str("HTTP_ADDR", ":8080"),
		HandlerTimeout:       millis("HANDLER_TIMEOUT_MS", 5000, &errs),
		ShutdownTimeout:      millis("SHUTDOWN_TIMEOUT_MS", 8000, &errs),
		PostgresDSN:          str("POSTGRES_DSN", "postgres://app:app@toxiproxy:15432/shop"),
		PGMaxConns:           int32(integer("PG_MAX_CONNS", 20, &errs)),
		KafkaBrokers:         strings.Split(str("KAFKA_BROKERS", "toxiproxy:19091"), ","),
		KafkaAcks:            str("KAFKA_ACKS", "all"),
		KafkaIdempotent:      boolean("KAFKA_IDEMPOTENT", true, &errs),
		KafkaLinger:          millis("KAFKA_LINGER_MS", 5, &errs),
		KafkaSyncSend:        boolean("KAFKA_SYNC_SEND", false, &errs),
		KafkaRequestTimeout:  millis("KAFKA_REQUEST_TIMEOUT_MS", 3000, &errs),
		KafkaRequestOverhead: millis("KAFKA_REQUEST_OVERHEAD_MS", 1000, &errs),
		KafkaDeliveryTimeout: millis("KAFKA_DELIVERY_TIMEOUT_MS", 5000, &errs),
	}
	if cfg.CycleID == "" {
		errs = append(errs, errors.New("CYCLE_ID is required"))
	}
	if !slices.Contains([]string{"P1", "P2", "P3", "P4", "P5"}, cfg.Pattern) {
		errs = append(errs, fmt.Errorf("PATTERN must be P1..P5, got %q", cfg.Pattern))
	}
	if cfg.KafkaAcks != "all" && cfg.KafkaAcks != "1" {
		errs = append(errs, fmt.Errorf("KAFKA_ACKS must be all or 1, got %q", cfg.KafkaAcks))
	}
	return cfg, errors.Join(errs...)
}

func str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func integer(key string, def int, errs *[]error) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
	}
	return n
}

func millis(key string, def int, errs *[]error) time.Duration {
	return time.Duration(integer(key, def, errs)) * time.Millisecond
}

func boolean(key string, def bool, errs *[]error) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
	}
	return b
}
