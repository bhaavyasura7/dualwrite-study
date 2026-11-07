package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsMatchCatalogue(t *testing.T) {
	t.Setenv("CYCLE_ID", "A-1_P1_r01")
	t.Setenv("PATTERN", "P1")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Topic() != "orders.A-1_P1_r01" {
		t.Errorf("topic = %s", cfg.Topic())
	}
	if cfg.KafkaAcks != "all" || !cfg.KafkaIdempotent || cfg.KafkaSyncSend {
		t.Errorf("producer defaults = %+v", cfg)
	}
	if cfg.KafkaLinger != 5*time.Millisecond || cfg.KafkaRequestTimeout != 3*time.Second || cfg.KafkaDeliveryTimeout != 5*time.Second {
		t.Errorf("timing defaults = %+v", cfg)
	}
}

func TestLoadReportsEveryInvalidValue(t *testing.T) {
	t.Setenv("CYCLE_ID", "")
	t.Setenv("PATTERN", "P9")
	t.Setenv("KAFKA_ACKS", "0")
	t.Setenv("KAFKA_LINGER_MS", "soon")

	_, err := Load()
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{"CYCLE_ID", "PATTERN", "KAFKA_ACKS", "KAFKA_LINGER_MS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
