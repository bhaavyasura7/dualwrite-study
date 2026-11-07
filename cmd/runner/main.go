// Command runner executes the test catalogue (SPEC 14).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"dualwrite-study/internal/runner"
)

const resultsDir = "results"

func main() {
	if err := run(); err != nil {
		slog.Error("runner failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	catalogue := flag.String("catalogue", "experiments/vectors.yaml", "test catalogue")
	vectors := flag.String("vectors", "", "comma-separated vector IDs; empty runs all")
	patterns := flag.String("patterns", "", "comma-separated patterns; empty runs all")
	repeats := flag.Int("repeats", 0, "cap repeats per cell; 0 keeps the catalogue's value")
	shardFlag := flag.String("shard", "", "this machine's share of the vectors, as i/n")
	perf := flag.Bool("perf", false, "with -shard, also run the performance vectors on this machine")
	seed := flag.Uint64("seed", 1, "base seed; every cycle's seed derives from it")
	results := flag.String("results", "", "results file; default depends on -shard")
	dryRun := flag.Bool("dry-run", false, "print the plan and exit")
	overhead := flag.Float64("overhead", 75, "with -dry-run, fixed seconds per cycle")
	allowDirty := flag.Bool("allow-dirty", false, "run on a dirty tree, marking the commit -dirty")
	duration := flag.Int("duration", 0, "trial only: override load duration_s; recorded in env_json")
	noConsumers := flag.Bool("no-consumers", false, "trial only: drop consumers; recorded in env_json")
	only := flag.String("cycles", "", "comma-separated cycle IDs; empty runs all selected")
	overrides := flag.String("env", "", "trial only: comma-separated KEY=VALUE overrides; recorded in env_json")
	merge := flag.Bool("merge", false, "merge results/results.*.csv into results/results.csv")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if *merge {
		sources, err := filepath.Glob(filepath.Join(resultsDir, "results.*.csv"))
		if err != nil {
			return err
		}
		n, err := runner.Merge(sources, filepath.Join(resultsDir, "results.csv"))
		if err != nil {
			return err
		}
		fmt.Printf("merged %d rows from %d files\n", n, len(sources))
		return nil
	}

	var shard runner.Shard
	if *shardFlag != "" {
		var err error
		if shard, err = runner.ParseShard(*shardFlag); err != nil {
			return err
		}
	}
	loaded, err := runner.LoadCatalogue(*catalogue)
	if err != nil {
		return err
	}
	all, err := loaded.Expand()
	if err != nil {
		return err
	}
	cycles := selectCycles(all, split(*vectors), split(*patterns), *repeats, shard, *perf)
	if ids := split(*only); len(ids) > 0 {
		cycles = slices.DeleteFunc(cycles, func(c runner.Cycle) bool { return !slices.Contains(ids, c.ID) })
	}
	for i := range cycles {
		if *duration > 0 {
			cycles[i].Load.DurationS = *duration
			cycles[i].Env["TRIAL_DURATION_S"] = fmt.Sprint(*duration)
		}
		for _, pair := range split(*overrides) {
			key, value, _ := strings.Cut(pair, "=")
			cycles[i].Env[key] = value
			cycles[i].Env["TRIAL_OVERRIDE"] = *overrides
		}
		if *noConsumers {
			cycles[i].Topology.Consumers = nil
			cycles[i].Env["TRIAL_NO_CONSUMERS"] = "true"
		}
	}
	if *dryRun {
		fmt.Print(runner.FormatEstimates(runner.EstimateRun(cycles), *overhead))
		return nil
	}
	if len(cycles) == 0 {
		return errors.New("no cycles selected")
	}
	for _, cycle := range cycles {
		if err := runner.Supported(cycle); err != nil {
			return fmt.Errorf("%s: %w", cycle.ID, err)
		}
	}

	commit, err := gitCommit(*allowDirty)
	if err != nil {
		return err
	}
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("hostname: %w", err)
	}
	host = strings.Split(host, ".")[0]
	path := *results
	if path == "" {
		path = filepath.Join(resultsDir, "results.csv")
		if shard.Count > 0 {
			path = filepath.Join(resultsDir, "results."+host+".csv")
		}
	}
	store, err := runner.OpenResults(path, commit)
	if err != nil {
		return err
	}

	// The service image must match the commit recorded in every row.
	build := exec.Command("docker", "compose", "--profile", "app", "build", "orderservice", "relay")
	if output, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("build image: %w: %s", err, output)
	}
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		return fmt.Errorf("create results directory: %w", err)
	}
	dockerHost, err := dockerEndpoint()
	if err != nil {
		return err
	}
	docker, err := runner.NewDocker(dockerHost, "dualwrite_default")
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	versions, err := docker.Versions(ctx, runner.BaseContainers, []string{"dualwrite/orderservice:dev", "dualwrite/relay:dev"})
	if err != nil {
		return err
	}
	versions["go"] = runtime.Version()
	versionsJSON, err := json.Marshal(versions)
	if err != nil {
		return err
	}

	r, err := runner.New(runner.Options{
		RunID:      fmt.Sprintf("%s-%s", commit, time.Now().UTC().Format("20060102T150405")),
		GitCommit:  commit,
		Host:       host,
		Versions:   string(versionsJSON),
		Shard:      shard,
		Seed:       *seed,
		ResultsDir: resultsDir,
	}, docker, store)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := r.Preflight(ctx); err != nil {
		return err
	}
	slog.Info("run started", "cycles", len(cycles), "commit", commit, "results", path)
	return r.Run(ctx, cycles)
}

func split(list string) []string {
	if list == "" {
		return nil
	}
	return strings.Split(list, ",")
}

func selectCycles(all []runner.Cycle, vectors, patterns []string, repeats int, shard runner.Shard, perf bool) []runner.Cycle {
	var selected []runner.Cycle
	for _, c := range all {
		if len(vectors) > 0 && !slices.Contains(vectors, c.VectorID) {
			continue
		}
		if len(patterns) > 0 && !slices.Contains(patterns, c.Pattern) {
			continue
		}
		if repeats > 0 && c.Repeat > repeats {
			continue
		}
		runsHere := shard.Includes(c) || (perf && c.Group == "perf")
		if !runsHere {
			continue
		}
		selected = append(selected, c)
	}
	// Failover cycles rebuild the database, so they run last (SPEC 12). Cycles are
	// grouped by broker count and Connect worker configuration, which need a
	// recreate to change.
	rank := func(c runner.Cycle) string {
		last := "0"
		if c.RequiresRebuild {
			last = "1"
		}
		return fmt.Sprint(last, c.Topology.Brokers, c.Env["CONNECT_EOS"], c.Env["OFFSET_FLUSH_MS"])
	}
	slices.SortStableFunc(selected, func(a, b runner.Cycle) int { return strings.Compare(rank(a), rank(b)) })
	return selected
}

// gitCommit returns HEAD, refusing a dirty tree unless told otherwise. Results
// are excluded, because the run itself writes them.
func gitCommit(allowDirty bool) (string, error) {
	head, err := exec.Command("git", "rev-parse", "--short=12", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("read git commit: %w", err)
	}
	commit := strings.TrimSpace(string(head))
	status, err := exec.Command("git", "status", "--porcelain", "--", ".", ":!results").Output()
	if err != nil {
		return "", fmt.Errorf("read git status: %w", err)
	}
	if len(status) == 0 {
		return commit, nil
	}
	if !allowDirty {
		return "", fmt.Errorf("working tree is dirty; commit first or pass -allow-dirty:\n%s", status)
	}
	return commit + "-dirty", nil
}

func dockerEndpoint() (string, error) {
	if os.Getenv("DOCKER_HOST") != "" {
		return "", nil
	}
	out, err := exec.Command("docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
	if err != nil {
		return "", fmt.Errorf("find docker endpoint: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
