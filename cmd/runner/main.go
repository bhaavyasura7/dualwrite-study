// Command runner executes the test catalogue (SPEC 14).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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

// Sessions of the paper run (SPEC 14.7). Each selects its cycles itself, so a
// session's directory always holds one well-defined set.
var sessions = map[string]func(c runner.Cycle) bool{
	"must":   func(c runner.Cycle) bool { return c.Priority == "must" && c.Group != "perf" },
	"should": func(c runner.Cycle) bool { return c.Priority == "should" && c.Group != "perf" },
	"perf":   func(c runner.Cycle) bool { return c.Group == "perf" },
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("runner failed", "error", err)
		os.Exit(1)
	}
}

// options are the parsed command line.
type options struct {
	catalogue, vectors, patterns, priorities, exclude string
	repeats                                           int
	shard                                             string
	perf                                              bool
	seed                                              uint64
	results                                           string
	dryRun                                            bool
	overhead                                          float64
	allowDirty                                        bool
	duration                                          int
	noConsumers                                       bool
	only                                              string
	overrides                                         string
	verify                                            string
	fixedSeed                                         bool
	merge                                             bool
	session                                           string
	trial                                             string
	checkRun                                          bool
	backupEvery                                       time.Duration
}

func parseOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("runner", flag.ContinueOnError)
	fs.StringVar(&o.catalogue, "catalogue", "experiments/vectors.yaml", "test catalogue")
	fs.StringVar(&o.vectors, "vectors", "", "comma-separated vector IDs; empty runs all")
	fs.StringVar(&o.patterns, "patterns", "", "comma-separated patterns; empty runs all")
	fs.StringVar(&o.priorities, "priority", "", "comma-separated priorities (must, should, optional); empty runs all")
	fs.StringVar(&o.exclude, "exclude-groups", "", "comma-separated groups to leave out, for example perf")
	fs.IntVar(&o.repeats, "repeats", 0, "cap repeats per cell; 0 keeps the catalogue's value")
	fs.StringVar(&o.shard, "shard", "", "this machine's share of the vectors, as i/n")
	fs.BoolVar(&o.perf, "perf", false, "with -shard, also run the performance vectors on this machine")
	fs.Uint64Var(&o.seed, "seed", 1, "base seed; every cycle's seed derives from it")
	fs.StringVar(&o.results, "results", "", "results file; default depends on the session or trial")
	fs.BoolVar(&o.dryRun, "dry-run", false, "print the plan and exit")
	fs.Float64Var(&o.overhead, "overhead", 15, "with -dry-run, fixed seconds per cycle (study-v3 measured about 15)")
	fs.BoolVar(&o.allowDirty, "allow-dirty", false, "run on a dirty tree, marking the commit -dirty")
	fs.IntVar(&o.duration, "duration", 0, "trial only: override load duration_s; recorded in env_json")
	fs.BoolVar(&o.noConsumers, "no-consumers", false, "trial only: drop consumers; recorded in env_json")
	fs.StringVar(&o.only, "cycles", "", "comma-separated cycle IDs; empty runs all selected")
	fs.StringVar(&o.overrides, "env", "", "trial only: comma-separated KEY=VALUE overrides; recorded in env_json")
	fs.StringVar(&o.verify, "verify", "", "check a session or trial directory against its manifest and exit")
	fs.BoolVar(&o.fixedSeed, "fixed-seed", false, "trial only: use -seed unchanged in every cycle")
	fs.BoolVar(&o.merge, "merge", false, "merge results/results.*.csv into results/results.csv")
	fs.StringVar(&o.session, "session", "", "paper-run session: must, should or perf (SPEC 14.7)")
	fs.StringVar(&o.trial, "trial", "", "name of a trial directory, results/trials/<commit>-<name>")
	fs.BoolVar(&o.checkRun, "check-run", false, "run the check-run cycles of SPEC 14.7 (implies -trial check)")
	fs.DurationVar(&o.backupEvery, "backup-every", time.Hour, "run make backup between cycles this often; 0 disables")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if o.checkRun && o.trial == "" {
		o.trial = "check"
	}
	if o.session != "" {
		if _, ok := sessions[o.session]; !ok {
			return options{}, fmt.Errorf("unknown session %q; use must, should or perf", o.session)
		}
		if o.vectors+o.patterns+o.priorities+o.exclude+o.only+o.shard+o.trial+o.overrides != "" ||
			o.repeats != 0 || o.duration != 0 || o.noConsumers || o.allowDirty || o.fixedSeed || o.checkRun || o.results != "" {
			return options{}, errors.New("-session selects its own cycles and runs on the frozen tree; it takes no selection, trial or results flags")
		}
	}
	return o, nil
}

// selection applies the options to the catalogue, in run order.
func selection(o options) ([]runner.Cycle, error) {
	var shard runner.Shard
	if o.shard != "" {
		var err error
		if shard, err = runner.ParseShard(o.shard); err != nil {
			return nil, err
		}
	}
	loaded, err := runner.LoadCatalogue(o.catalogue)
	if err != nil {
		return nil, err
	}
	all, err := loaded.Expand()
	if err != nil {
		return nil, err
	}
	var cycles []runner.Cycle
	switch {
	case o.session != "":
		in := sessions[o.session]
		cycles = selectCycles(slices.DeleteFunc(slices.Clone(all), func(c runner.Cycle) bool { return !in(c) }), nil, nil, 0, runner.Shard{}, false)
	case o.checkRun:
		cycles = selectCycles(checkRunCycles(all), nil, nil, 0, runner.Shard{}, false)
	default:
		cycles = selectCycles(all, split(o.vectors), split(o.patterns), o.repeats, shard, o.perf)
		if wanted := split(o.priorities); len(wanted) > 0 {
			cycles = slices.DeleteFunc(cycles, func(c runner.Cycle) bool { return !slices.Contains(wanted, c.Priority) })
		}
		if skipped := split(o.exclude); len(skipped) > 0 {
			cycles = slices.DeleteFunc(cycles, func(c runner.Cycle) bool { return slices.Contains(skipped, c.Group) })
		}
	}
	cycles = shuffleWithinGroups(cycles, o.seed)
	if ids := split(o.only); len(ids) > 0 {
		cycles = slices.DeleteFunc(cycles, func(c runner.Cycle) bool { return !slices.Contains(ids, c.ID) })
	}
	for i := range cycles {
		if o.duration > 0 {
			cycles[i].Load.DurationS = o.duration
			cycles[i].Env["TRIAL_DURATION_S"] = fmt.Sprint(o.duration)
		}
		for _, pair := range split(o.overrides) {
			key, value, _ := strings.Cut(pair, "=")
			cycles[i].Env[key] = value
			cycles[i].Env["TRIAL_OVERRIDE"] = o.overrides
		}
		if o.noConsumers {
			cycles[i].Topology.Consumers = nil
			cycles[i].Env["TRIAL_NO_CONSUMERS"] = "true"
		}
	}
	return cycles, nil
}

// checkRunCycles is the check run of SPEC 14.7, taken from the catalogue in
// file order: the first cycles of each listed vector, pattern and cell.
func checkRunCycles(all []runner.Cycle) []runner.Cycle {
	type rule struct {
		vector   string
		patterns []string
		perCell  bool // the first cycle of every sweep cell, instead of the first n
		n        int
		rate     int // only cycles at this load rate
	}
	rules := []rule{
		{vector: "C0-1", patterns: []string{"P4"}, n: 1},
		{vector: "C0-3", patterns: []string{"P1", "P2", "P3", "P4", "P5"}, n: 1},
		{vector: "C-1", patterns: []string{"P1", "P4"}, perCell: true},
		{vector: "A-1", patterns: []string{"P1"}, n: 3},
		{vector: "A-3", patterns: []string{"P2"}, n: 1},
		{vector: "E-2", patterns: []string{"P4"}, perCell: true},
		{vector: "D-1", patterns: []string{"P1", "P4"}, n: 2},
		{vector: "D-2", patterns: []string{"P4"}, n: 1},
		{vector: "B-1", patterns: []string{"P3"}, n: 1},
		{vector: "G-1", patterns: []string{"P3"}, n: 1},
		{vector: "G-2", patterns: []string{"P3"}, n: 1},
		{vector: "P-1", patterns: []string{"P3", "P4"}, n: 1, rate: 1000},
	}
	var out []runner.Cycle
	for _, r := range rules {
		for _, pattern := range r.patterns {
			taken := 0
			cells := map[string]bool{}
			for _, c := range all {
				if c.VectorID != r.vector || c.Pattern != pattern || (r.rate != 0 && c.Load.RatePerS != r.rate) {
					continue
				}
				if r.perCell {
					cell, _ := json.Marshal(c.Sweep)
					if cells[string(cell)] {
						continue
					}
					cells[string(cell)] = true
				} else if taken >= r.n {
					continue
				}
				taken++
				out = append(out, c)
			}
		}
	}
	return out
}

func run(args []string) error {
	o, err := parseOptions(args)
	if err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if o.merge {
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
	if o.verify != "" {
		return verifyDir(o.verify)
	}
	cycles, err := selection(o)
	if err != nil {
		return err
	}
	if o.dryRun {
		fmt.Print(runner.FormatEstimates(runner.EstimateRun(cycles), o.overhead))
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
		if o.checkRun {
			for _, c := range cycles {
				fmt.Println(c.ID)
			}
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

	commit, err := gitCommit(o.allowDirty)
	if err != nil {
		return err
	}
	if o.session != "" {
		// A paper-run session runs only on the frozen tag (SPEC 14.5, 14.7).
		if _, err := studyTag(); err != nil {
			return err
		}
	}
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("hostname: %w", err)
	}
	host = strings.Split(host, ".")[0]
	path := o.results
	if path == "" {
		path = filepath.Join(runDir(o, commit), "results.csv")
		if o.shard != "" {
			path = filepath.Join(runDir(o, commit), "results."+host+".csv") // for make merge
		}
	}
	dir := filepath.Dir(path)
	if err := writeManifest(dir, commit, o.seed, o.catalogue, args, host, o.session); err != nil {
		return err
	}
	if err := hostInfo(dir); err != nil {
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
		Strict:        !o.allowDirty,
		FixedSeed:     o.fixedSeed,
		StopRequested: stopping.Load,
		Seed:          o.seed,
		ResultsDir:    dir,
		StopFile:      filepath.Join(resultsDir, "STOP"),
		BackupEvery:   o.backupEvery,
		Backup:        backup,
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

// runDir is where a run's files go (SPEC 14.6): a session of the paper run, or
// a named trial. Anything else is an ad hoc trial.
func runDir(o options, commit string) string {
	switch {
	case o.session != "":
		return filepath.Join(resultsDir, "runs", commit, o.session)
	case o.trial != "":
		return filepath.Join(resultsDir, "trials", commit+"-"+o.trial)
	}
	return filepath.Join(resultsDir, "trials", commit)
}

// studyTag returns the study-* tag HEAD is at, or an error.
func studyTag() (string, error) {
	out, err := exec.Command("git", "describe", "--tags", "--exact-match", "HEAD").Output()
	tag := strings.TrimSpace(string(out))
	if err != nil || !strings.HasPrefix(tag, "study-") {
		return "", errors.New("HEAD is not at a study-* tag; check out the frozen tag (SPEC 14.7, step 6)")
	}
	return tag, nil
}

// backup runs make backup between cycles (SPEC 14.5).
func backup(ctx context.Context) error {
	command := exec.CommandContext(ctx, "./deploy/backup.sh")
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("backup: %w: %s", err, output)
	}
	return nil
}

// hostInfo writes host-info.txt into the run directory once (SPEC 2.1).
func hostInfo(dir string) error {
	path := filepath.Join(dir, "host-info.txt")
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	out, err := exec.Command("./deploy/host-info.sh").CombinedOutput()
	if err != nil {
		return fmt.Errorf("host-info.sh: %w: %s", err, out)
	}
	return os.WriteFile(path, out, 0o644)
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
	path := strings.TrimSuffix(results, ".csv") + ".order.txt"
	return os.WriteFile(path, []byte(orderText(cycles)), 0o644)
}

func orderText(cycles []runner.Cycle) string {
	var out strings.Builder
	for i, c := range cycles {
		fmt.Fprintf(&out, "%d %s\n", i+1, c.ID)
	}
	return out.String()
}

// manifest records what produced a run directory.
type manifest struct {
	Commit          string   `json:"commit"`
	Tag             string   `json:"tag"`
	Seed            uint64   `json:"seed"`
	CatalogueSHA256 string   `json:"catalogue_sha256"`
	Spec            string   `json:"spec"`
	Host            string   `json:"host"`
	Session         string   `json:"session,omitempty"`
	Arguments       []string `json:"arguments"`
	CreatedAt       string   `json:"created_at"`
}

// writeManifest creates the run directory's manifest, or checks that an existing
// one has the same commit, seed, session and arguments, so a resume can never
// mix builds or selections.
func writeManifest(dir, commit string, seed uint64, catalogue string, args []string, host, session string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	path := filepath.Join(dir, "manifest.json")
	if raw, err := os.ReadFile(path); err == nil {
		var existing manifest
		if err := json.Unmarshal(raw, &existing); err != nil {
			return fmt.Errorf("read manifest: %w", err)
		}
		switch {
		case existing.Commit != commit || existing.Seed != seed:
			return fmt.Errorf("%s belongs to commit %s seed %d, not %s seed %d", path, existing.Commit, existing.Seed, commit, seed)
		case existing.Session != session:
			return fmt.Errorf("%s belongs to session %q, not %q", path, existing.Session, session)
		case !slices.Equal(existing.Arguments, args):
			return fmt.Errorf("%s was started with %q; resume with exactly the same arguments, not %q", path, existing.Arguments, args)
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
		Session:         session,
		Arguments:       args,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

// verifyDir checks a session or trial directory against the selection its own
// manifest describes (SPEC 14.8).
func verifyDir(dir string) error {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	o, err := parseOptions(m.Arguments)
	if err != nil {
		return fmt.Errorf("manifest arguments: %w", err)
	}
	cycles, err := selection(o)
	if err != nil {
		return err
	}
	order, err := os.ReadFile(filepath.Join(dir, "results.order.txt"))
	if err != nil {
		return fmt.Errorf("read run order: %w", err)
	}
	summary, err := runner.Verify(filepath.Join(dir, "results.csv"), cycles)
	if err != nil {
		return err
	}
	if string(order) != orderText(cycles) {
		summary.Problems = append(summary.Problems, "results.order.txt differs from the order the seed and catalogue give")
	}
	fmt.Printf("rows: %d\ncommits: %v\nimage version sets: %d\nstatuses: %v\nmissing cycles: %d of %d\n",
		summary.Rows, summary.Commits, summary.Versions, summary.Statuses, len(summary.Missing), len(cycles))
	for _, cell := range summary.Incomplete {
		fmt.Println("INCOMPLETE:", cell)
	}
	for _, problem := range summary.Problems {
		fmt.Println("PROBLEM:", problem)
	}
	if len(summary.Problems) > 0 {
		return errors.New("results are not usable for the paper")
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
