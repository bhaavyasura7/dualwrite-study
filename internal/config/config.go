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
	KafkaTraceAttempts   bool          // KAFKA_TRACE_ATTEMPTS, default false; logs every produce request
	// KafkaMaxBuffered is the client's record buffer. Produce blocks when it is full.
	KafkaMaxBuffered   int // KAFKA_MAX_BUFFERED_RECORDS, default 50000
	KafkaRecordRetries int // KAFKA_RECORD_RETRIES, default 0, meaning unlimited

	RetryMax  int           // RETRY_MAX, default 5
	RetryBase time.Duration // RETRY_BASE_MS, default 200
	RetryCap  time.Duration // RETRY_CAP_MS, default 5000

	RelayBatchSize    int           // RELAY_BATCH_SIZE, default 100
	RelayPollInterval time.Duration // RELAY_POLL_INTERVAL_MS, default 500
	// RelayDBTimeout bounds each relay database call. It matches HandlerTimeout, so
	// the relay and the order service react alike to a dead database.
	RelayDBTimeout time.Duration // RELAY_DB_TIMEOUT_MS, default 5000
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
		PostgresDSN:          str("POSTGRES_DSN", "postgres://app:app@toxiproxy-pg:15432/shop"),
		PGMaxConns:           int32(integer("PG_MAX_CONNS", 20, &errs)),
		KafkaBrokers:         strings.Split(str("KAFKA_BROKERS", "toxiproxy:19091"), ","),
		KafkaAcks:            str("KAFKA_ACKS", "all"),
		KafkaIdempotent:      boolean("KAFKA_IDEMPOTENT", true, &errs),
		KafkaLinger:          millis("KAFKA_LINGER_MS", 5, &errs),
		KafkaSyncSend:        boolean("KAFKA_SYNC_SEND", false, &errs),
		KafkaRequestTimeout:  millis("KAFKA_REQUEST_TIMEOUT_MS", 3000, &errs),
		KafkaRequestOverhead: millis("KAFKA_REQUEST_OVERHEAD_MS", 1000, &errs),
		KafkaDeliveryTimeout: millis("KAFKA_DELIVERY_TIMEOUT_MS", 5000, &errs),
		KafkaTraceAttempts:   boolean("KAFKA_TRACE_ATTEMPTS", false, &errs),
		KafkaMaxBuffered:     integer("KAFKA_MAX_BUFFERED_RECORDS", 50000, &errs),
		KafkaRecordRetries:   integer("KAFKA_RECORD_RETRIES", 0, &errs),
		RetryMax:             integer("RETRY_MAX", 5, &errs),
		RetryBase:            millis("RETRY_BASE_MS", 200, &errs),
		RetryCap:             millis("RETRY_CAP_MS", 5000, &errs),
		RelayBatchSize:       integer("RELAY_BATCH_SIZE", 100, &errs),
		RelayPollInterval:    millis("RELAY_POLL_INTERVAL_MS", 500, &errs),
		RelayDBTimeout:       millis("RELAY_DB_TIMEOUT_MS", 5000, &errs),
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

// Measurement holds the direct connections used by the runner and checker.
// They bypass Toxiproxy so that faults never affect measurement (SPEC 16).
type Measurement struct {
	PostgresDSN  string   // POSTGRES_DIRECT_DSN
	KafkaBrokers []string // KAFKA_DIRECT_BROKERS, comma separated
	ConnectURL   string   // CONNECT_URL
	// A cycle starting above either threshold is flagged, not stopped.
	PrecheckMaxP99MS    float64 // PRECHECK_MAX_P99_MS, default 30
	PrecheckMaxHostLoad float64 // PRECHECK_MAX_HOST_LOAD, default 8
}

// LoadMeasurement reads Measurement from the environment.
func LoadMeasurement() (Measurement, error) {
	var errs []error
	measurement := Measurement{
		PostgresDSN:         str("POSTGRES_DIRECT_DSN", "postgres://app:app@localhost:55432/shop"),
		KafkaBrokers:        strings.Split(str("KAFKA_DIRECT_BROKERS", "localhost:29091"), ","),
		ConnectURL:          str("CONNECT_URL", "http://localhost:8083"),
		PrecheckMaxP99MS:    float64(integer("PRECHECK_MAX_P99_MS", 30, &errs)),
		PrecheckMaxHostLoad: float64(integer("PRECHECK_MAX_HOST_LOAD", 8, &errs)),
	}
	return measurement, errors.Join(errs...)
}

// LoadGen holds the load generator settings (SPEC 15).
type LoadGen struct {
	Target         string        // LOAD_TARGET, default http://localhost:8080
	Mode           string        // LOAD_MODE, constant or burst, default constant
	Rate           int           // LOAD_RATE_PER_S, default 100
	Duration       time.Duration // LOAD_DURATION_S, default 40; 0 runs until cancelled
	Warmup         time.Duration // LOAD_WARMUP_S, default 0; excluded from latency and throughput
	BurstRate      int           // LOAD_BURST_RATE_PER_S, default 10x the rate
	BurstAt        time.Duration // LOAD_BURST_AT_S, default 10
	BurstLength    time.Duration // LOAD_BURST_S, default 10
	RequestTimeout time.Duration // LOAD_REQUEST_TIMEOUT_MS, default 10000
	LatencyFile    string        // LOAD_LATENCY_FILE, optional CSV of start_ms,latency_ms per request
	Seed           uint64        // LOAD_SEED, default 1
}

// LoadLoadGen reads LoadGen from the environment.
func LoadLoadGen() (LoadGen, error) {
	var errs []error
	rate := integer("LOAD_RATE_PER_S", 100, &errs)
	cfg := LoadGen{
		Target:         str("LOAD_TARGET", "http://localhost:8080"),
		Mode:           str("LOAD_MODE", "constant"),
		Rate:           rate,
		Duration:       seconds("LOAD_DURATION_S", 40, &errs),
		Warmup:         seconds("LOAD_WARMUP_S", 0, &errs),
		BurstRate:      integer("LOAD_BURST_RATE_PER_S", 10*rate, &errs),
		BurstAt:        seconds("LOAD_BURST_AT_S", 10, &errs),
		BurstLength:    seconds("LOAD_BURST_S", 10, &errs),
		RequestTimeout: millis("LOAD_REQUEST_TIMEOUT_MS", 10000, &errs),
		LatencyFile:    os.Getenv("LOAD_LATENCY_FILE"),
		Seed:           uint64(integer("LOAD_SEED", 1, &errs)),
	}
	if cfg.Mode != "constant" && cfg.Mode != "burst" {
		errs = append(errs, fmt.Errorf("LOAD_MODE must be constant or burst, got %q", cfg.Mode))
	}
	if cfg.Rate < 1 || cfg.BurstRate < 1 {
		errs = append(errs, errors.New("load rates must be positive"))
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

func seconds(key string, def int, errs *[]error) time.Duration {
	return time.Duration(integer(key, def, errs)) * time.Second
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
