package runner

import (
	"os"
	"path/filepath"
	"testing"
)

func supply(t *testing.T, root, name, kind, online string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "type"), []byte(kind+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if online != "" {
		if err := os.WriteFile(filepath.Join(dir, "online"), []byte(online+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinuxMainsPower(t *testing.T) {
	if err := linuxMainsPower(t.TempDir()); err != nil {
		t.Errorf("no supplies (desktop, WSL2) must count as mains: %v", err)
	}
	laptop := t.TempDir()
	supply(t, laptop, "BAT0", "Battery", "")
	supply(t, laptop, "AC", "Mains", "0")
	if err := linuxMainsPower(laptop); err == nil {
		t.Error("battery with mains offline accepted")
	}
	supply(t, laptop, "AC", "Mains", "1")
	if err := linuxMainsPower(laptop); err != nil {
		t.Errorf("battery with mains online refused: %v", err)
	}
}

func TestParseLoad(t *testing.T) {
	for in, want := range map[string]float64{"0.52 0.58 0.59 1/321 12345": 0.52, "3.10 2.00 1.00": 3.1, "": 0, "x": 0} {
		if got := parseLoad(in); got != want {
			t.Errorf("%q: %v, want %v", in, got, want)
		}
	}
}

func TestLinuxThermal(t *testing.T) {
	if got := linuxThermal(t.TempDir()); got != "unavailable" {
		t.Errorf("no zones: %q", got)
	}
	root := t.TempDir()
	for zone, temp := range map[string]string{"thermal_zone0": "41000", "thermal_zone1": "58500"} {
		if err := os.MkdirAll(filepath.Join(root, zone), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, zone, "temp"), []byte(temp), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := linuxThermal(root); got != "58.5C" {
		t.Errorf("got %q", got)
	}
}

func TestMacWarnings(t *testing.T) {
	quiet := "Note: No thermal warning level has been recorded\nNote: No performance warning level has been recorded\n"
	if got := macWarnings(quiet); got != "" {
		t.Errorf("quiet: %q", got)
	}
	if got := macWarnings(quiet + "CPU_Speed_Limit = 80\n"); got != "CPU_Speed_Limit = 80" {
		t.Errorf("warning: %q", got)
	}
}
