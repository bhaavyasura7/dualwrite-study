package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// Machine checks for the two host kinds of SPEC 2.1: development Macs and the
// Linux (WSL2) results machine.

// onMainsPower refuses a machine running on battery (SPEC 14.5). On Linux a
// machine with no battery (a desktop, or WSL2, which exposes none) is on mains.
func onMainsPower(ctx context.Context) error {
	if runtime.GOOS == "darwin" {
		power, err := exec.CommandContext(ctx, "pmset", "-g", "batt").Output()
		if err != nil {
			return fmt.Errorf("read power source: %w", err)
		}
		if !strings.Contains(string(power), "AC Power") {
			return errors.New("the Mac is on battery; plug it in")
		}
		return nil
	}
	return linuxMainsPower("/sys/class/power_supply")
}

func linuxMainsPower(root string) error {
	supplies, err := filepath.Glob(filepath.Join(root, "*"))
	if err != nil {
		return err
	}
	battery, mains := false, false
	for _, dir := range supplies {
		kind := readTrimmed(filepath.Join(dir, "type"))
		switch kind {
		case "Battery":
			battery = true
		case "Mains":
			if readTrimmed(filepath.Join(dir, "online")) == "1" {
				mains = true
			}
		}
	}
	if battery && !mains {
		return errors.New("the machine is on battery; plug it in")
	}
	return nil
}

func readTrimmed(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// hostLoad is the one-minute load average, or 0 if unavailable.
func hostLoad() float64 {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
		if err != nil {
			return 0
		}
		return parseLoad(strings.Trim(strings.TrimSpace(string(out)), "{} "))
	}
	return parseLoad(readTrimmed("/proc/loadavg"))
}

// parseLoad reads the first number of "0.52 0.58 0.59 ..." (Linux) or
// "0.52 0.58 0.59" (macOS, without braces).
func parseLoad(s string) float64 {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0
	}
	load, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0
	}
	return load
}

// thermalState is what the thermal column records, and whether it is a warning
// that flags the cycle (SPEC 14.5). macOS reports thermal and performance
// warnings; Linux reports the hottest thermal zone in °C, which never flags;
// WSL2 exposes no thermal zones, so it reports "unavailable".
func thermalState() (string, bool) {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("pmset", "-g", "therm").Output()
		if err != nil {
			return "unknown", false
		}
		state := macWarnings(string(out))
		return state, state != ""
	}
	return linuxThermal("/sys/class/thermal"), false
}

func macWarnings(out string) string {
	var states []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "No ") && strings.Contains(line, "has been recorded") {
			continue
		}
		states = append(states, line)
	}
	return strings.Join(states, "; ")
}

func linuxThermal(root string) string {
	zones, _ := filepath.Glob(filepath.Join(root, "thermal_zone*", "temp"))
	hottest, found := 0, false
	for _, zone := range zones {
		milli, err := strconv.Atoi(readTrimmed(zone))
		if err != nil {
			continue
		}
		if !found || milli > hottest {
			hottest, found = milli, true
		}
	}
	if !found {
		return "unavailable"
	}
	return strconv.FormatFloat(float64(hottest)/1000, 'f', 1, 64) + "C"
}
