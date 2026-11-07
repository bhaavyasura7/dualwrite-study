// Command runner executes the test catalogue (SPEC 14).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
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
	priorities := flag.String("priority", "", "comma-separated priorities (must, should, optional); empty runs all")
	exclude := flag.String("exclude-groups", "", "comma-separated groups to leave out, for example perf")
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
	verify := flag.String("verify", "", "check a results file against the selected cycles and exit")
	fixedSeed := flag.Bool("fixed-seed", false, "trial only: use -seed unchanged in every cycle")
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
	if wanted := split(*priorities); len(wanted) > 0 {
		cycles = slices.DeleteFunc(cycles, func(c runner.Cycle) bool { return !slices.Contains(wanted, c.Priority) })
	}
	if skipped := split(*exclude); len(skipped) > 0 {
		cycles = slices.DeleteFunc(cycles, func(c runner.Cycle) bool { return slices.Contains(skipped, c.Group) })
	}
	cycles = shuffleWithinGroups(cycles, *seed)
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
	if *verify != "" {
		return verifyResults(*verify, cycles)
	}
	if *dryRun {
		fmt.Print(runner.FormatEstimates(runner.EstimateRun(cycles), *overhead))
		refused := make(map[string]int)
		for _, cycle := range cycles {
			if err := runner.Supported(cycle); err != nil {
				refused[cycle.VectorID+": "+err.Error()]++
			}
		}
		fmt.Printf("refused: %d kinds\n", len(refused))
		for _, reason := range slices.Sorted(maps.Keys(refused)) {
			fmt.Printf("  %4d  %s\n", refused[reason], reason)
		}
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
	// Each run lives in its own directory, named by commit, with a manifest.
	runDir := filepath.Join(resultsDir, "runs", commit)
	if *allowDirty {
		runDir = filepath.Join(resultsDir, "trials", commit)
	}
	path := *results
	if path == "" {
		path = filepath.Join(runDir, "results.csv")
		if shard.Count > 0 {
			path = filepath.Join(runDir, "results."+host+".csv")
		}
	}
	if err := writeManifest(filepath.Dir(path), commit, *seed, *catalogue, os.Args[1:], host); err != nil {
		return err
	}
	store, err := runner.OpenResults(path, commit)
	if err != nil {
		return err
	}

	// The service image must match the commit recorded in every row.
	build := exec.Command("docker", "compose", "--profile", "app", "build", "orderservice", "relay", "consumer")
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

	// First Ctrl-C: finish the current cycle, then stop. Second: stop now; the
	// interrupted cycle is discarded and reruns on resume.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stopping atomic.Bool
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		stopping.Store(true)
		slog.Warn("stopping after the current cycle; press Ctrl-C again to stop now")
		<-signals
		cancel()
	}()
	_ = os.Remove(filepath.Join(resultsDir, "STOP"))
	versions, err := docker.Versions(ctx, runner.BaseContainers, []string{"dualwrite/orderservice:dev", "dualwrite/relay:dev", "dualwrite/consumer:dev"})
	if err != nil {
		return err
	}
	versions["go"] = runtime.Version()
	versionsJSON, err := json.Marshal(versions)
	if err != nil {
		return err
	}

	r, err := runner.New(runner.Options{
		RunID:         fmt.Sprintf("%s-%s", commit, time.Now().UTC().Format("20060102T150405")),
		GitCommit:     commit,
		Host:          host,
		Versions:      string(versionsJSON),
		Shard:         shard,
		Strict:        !*allowDirty,
		FixedSeed:     *fixedSeed,
		StopRequested: stopping.Load,
		Seed:          *seed,
		ResultsDir:    filepath.Dir(path),
		StopFile:      filepath.Join(resultsDir, "STOP"),
	}, docker, store)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := r.Preflight(ctx); err != nil {
		return err
	}
	if err := writeOrder(path, cycles); err != nil {
		return err
	}
	slog.Info("run started", "cycles", len(cycles), "commit", commit, "results", path)
	return r.Run(ctx, cycles)
}

// rank orders groups of cycles. Controls run first: a failure there means the
// setup is wrong. Failover cycles rebuild the database, so they run last (SPEC 12).
// In between, cycles are grouped by broker count and Connect worker
// configuration, which need a recreate to change.
func rank(c runner.Cycle) string {
	order := "2"
	switch {
	case c.VectorID == "C0-1":
		order = "0" // the no-fault control proves the setup before anything else
	case c.Group == "control":
		order = "1"
	case c.RequiresRebuild:
		order = "3"
	}
	return fmt.Sprint(order, c.Topology.Brokers, c.Env["CONNECT_EOS"], c.Env["OFFSET_FLUSH_MS"])
}

// shuffleWithinGroups randomises the order of cycles that share a rank, using the
// run seed, so slow drift such as heat cannot favour one pattern. Groups keep
// their place, and the same seed gives the same order, so resume works.
func shuffleWithinGroups(cycles []runner.Cycle, seed uint64) []runner.Cycle {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	shuffled := slices.Clone(cycles)
	for start := 0; start < len(shuffled); {
		end := start + 1
		for end < len(shuffled) && rank(shuffled[end]) == rank(shuffled[start]) {
			end++
		}
		group := shuffled[start:end]
		rng.Shuffle(len(group), func(i, j int) { group[i], group[j] = group[j], group[i] })
		start = end
	}
	return shuffled
}

// writeOrder records the run order next to the results file.
func writeOrder(results string, cycles []runner.Cycle) error {
	var out strings.Builder
	for i, c := range cycles {
		fmt.Fprintf(&out, "%d %s\n", i+1, c.ID)
	}
	path := strings.TrimSuffix(results, ".csv") + ".order.txt"
	if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
		return fmt.Errorf("write run order: %w", err)
	}
	return nil
}

// manifest records what produced a run directory.
type manifest struct {
	Commit          string   `json:"commit"`
	Tag             string   `json:"tag"`
	Seed            uint64   `json:"seed"`
	CatalogueSHA256 string   `json:"catalogue_sha256"`
	Spec            string   `json:"spec"`
	Host            string   `json:"host"`
	Arguments       []string `json:"arguments"`
	CreatedAt       string   `json:"created_at"`
}

// writeManifest creates the run directory's manifest, or checks that an existing
// one belongs to the same commit and seed, so a resume can never mix builds.
func writeManifest(dir, commit string, seed uint64, catalogue string, args []string, host string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	path := filepath.Join(dir, "manifest.json")
	if raw, err := os.ReadFile(path); err == nil {
		var existing manifest
		if err := json.Unmarshal(raw, &existing); err != nil {
			return fmt.Errorf("read manifest: %w", err)
		}
		if existing.Commit != commit || existing.Seed != seed {
			return fmt.Errorf("%s belongs to commit %s seed %d, not %s seed %d", path, existing.Commit, existing.Seed, commit, seed)
		}
		return nil
	}
	content, err := os.ReadFile(catalogue)
	if err != nil {
		return fmt.Errorf("read catalogue: %w", err)
	}
	tag, _ := exec.Command("git", "describe", "--tags", "--exact-match").Output()
	spec, _ := exec.Command("sed", "-n", "3p", "docs/SPEC.md").Output()
	m := manifest{
		Commit:          commit,
		Tag:             strings.TrimSpace(string(tag)),
		Seed:            seed,
		CatalogueSHA256: fmt.Sprintf("%x", sha256.Sum256(content)),
		Spec:            strings.TrimSpace(string(spec)),
		Host:            host,
		Arguments:       args,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func verifyResults(path string, cycles []runner.Cycle) error {
	summary, err := runner.Verify(path, cycles)
	if err != nil {
		return err
	}
	fmt.Printf("rows: %d\ncommits: %v\nimage version sets: %d\nstatuses: %v\nmissing cycles: %d of %d\n",
		summary.Rows, summary.Commits, summary.Versions, summary.Statuses, len(summary.Missing), len(cycles))
	for _, problem := range summary.Problems {
		fmt.Println("PROBLEM:", problem)
	}
	if len(summary.Problems) > 0 {
		return errors.New("results file is not usable for the paper")
	}
	return nil
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
