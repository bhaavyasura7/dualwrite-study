// Command recompute rewrites a session's metric columns from its raw files alone
// (SPEC 11.5) into results.recomputed.csv. With -check it fails if any value
// differs from results.csv: at the run commit that proves the raw data is
// complete; after a checker fix it lists every value the fix changed.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"dualwrite-study/internal/runner"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "recompute:", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "", "session or trial directory, for example results/runs/<commit>/must")
	check := flag.Bool("check", false, "fail if any recomputed value differs from results.csv")
	flag.Parse()
	if *dir == "" {
		return errors.New("-dir is required")
	}
	head, err := exec.Command("git", "rev-parse", "--short=12", "HEAD").Output()
	if err != nil {
		return fmt.Errorf("read git commit: %w", err)
	}
	commit := strings.TrimSpace(string(head))
	if status, _ := exec.Command("git", "status", "--porcelain", "--", ".", ":!results").Output(); len(status) > 0 {
		commit += "-dirty"
	}
	report, err := runner.Recompute(*dir, commit)
	if err != nil {
		return err
	}
	fmt.Printf("rows: %d, recomputed: %d, kept as recorded (check did not run): %d, differences: %d\nanalysis commit: %s\n",
		report.Rows, report.Recomputed, report.Skipped, len(report.Differences), commit)
	for _, d := range report.Differences {
		fmt.Println("DIFF:", d)
	}
	if *check && len(report.Differences) > 0 {
		return fmt.Errorf("%d values differ from results.csv", len(report.Differences))
	}
	return nil
}
