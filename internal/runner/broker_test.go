package runner

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReloadLine(t *testing.T) {
	for _, test := range []struct {
		line, logs, ms string
		unclean        bool
	}{
		{"INFO Loaded 3054 logs in 31352ms (unclean log dirs = ArrayBuffer(/var/lib/kafka/data)) (kafka.log.LogManager)", "3054", "31352", true},
		{"INFO Loaded 90 logs in 412ms (kafka.log.LogManager)", "90", "412", false},
	} {
		m := reloadLine.FindStringSubmatch(test.line)
		if m == nil || m[1] != test.logs || m[2] != test.ms || (m[3] != "") != test.unclean {
			t.Errorf("%q: got %q", test.line, m)
		}
	}
}

func TestCycleTopics(t *testing.T) {
	orders, heartbeat := cycleTopics(Cycle{ID: "C-1_P5_c2_r02", Pattern: "P5"})
	if orders != "orders.C-1_P5_c2_r02" || heartbeat != "__debezium-heartbeat.cdc"+cycleSlug("C-1_P5_c2_r02") {
		t.Errorf("P5: %s, %s", orders, heartbeat)
	}
	if _, heartbeat := cycleTopics(Cycle{ID: "C-1_P1_c1_r01", Pattern: "P1"}); heartbeat != "" {
		t.Errorf("P1 has no heartbeat topic, got %s", heartbeat)
	}
}

func TestParseDeleteDirs(t *testing.T) {
	n, kb, err := parseDeleteDirs("12\t/var/lib/kafka/data/orders.a-0.abc-delete\n8\t/var/lib/kafka/data/orders.a-1.abc-delete\n")
	if err != nil || n != 2 || kb != 20 {
		t.Fatalf("got %d, %d, %v", n, kb, err)
	}
	// A directory removed between the listing and du prints nothing: empty output is zero, not an error.
	if n, kb, err := parseDeleteDirs(""); err != nil || n != 0 || kb != 0 {
		t.Fatalf("empty: %d, %d, %v", n, kb, err)
	}
	if _, _, err := parseDeleteDirs("x\t/y"); err == nil {
		t.Fatal("garbage accepted")
	}
}

// TestDeleteDirsScriptSurvivesVanishingDirs runs the script against a
// directory that disappears, as Kafka's deletion does (review finding 1).
func TestDeleteDirsScriptSurvivesVanishingDirs(t *testing.T) {
	dir := t.TempDir()
	script := strings.ReplaceAll(deleteDirsScript, "/var/lib/kafka/data", dir)
	// A glob that matches nothing, and du on a missing path, must both exit 0.
	out, err := exec.Command("sh", "-c", script).Output()
	if err != nil || strings.TrimSpace(string(out)) != "" {
		t.Fatalf("no dirs: %q, %v", out, err)
	}
	vanishing := strings.Replace(script, `[ -d "$d" ] && du`, `[ -d "$d" ] && rm -r "$d" && du`, 1)
	if err := os.Mkdir(filepath.Join(dir, "t-0.x-delete"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sh", "-c", vanishing).Output(); err != nil {
		t.Fatalf("vanishing dir: %q, %v", out, err)
	}
}
