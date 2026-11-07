// Package runner executes the test catalogue (SPEC 14).
package runner

import (
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Catalogue is experiments/vectors.yaml.
type Catalogue struct {
	Defaults Defaults `yaml:"defaults"`
	Vectors  []Vector `yaml:"vectors"`
}

// Defaults apply to every vector.
type Defaults struct {
	Repeats  int            `yaml:"repeats"`
	Load     LoadOverride   `yaml:"load"`
	Drain    Drain          `yaml:"drain"`
	Topology TopoOverride   `yaml:"topology"`
	Env      map[string]any `yaml:"env"`
}

// Drain controls the DRAIN state.
type Drain struct {
	QuietS   int `yaml:"quiet_s"`
	TimeoutS int `yaml:"timeout_s"`
}

// LoadOverride holds load fields, of which a vector sets only some.
type LoadOverride struct {
	Mode          *string `yaml:"mode"`
	RatePerS      *int    `yaml:"rate_per_s"`
	WarmupS       *int    `yaml:"warmup_s"`
	PostFaultS    *int    `yaml:"post_fault_s"`
	DurationS     *int    `yaml:"duration_s"`
	BurstRatePerS *int    `yaml:"burst_rate_per_s"`
	BurstAtS      *int    `yaml:"burst_at_s"`
	BurstS        *int    `yaml:"burst_s"`
}

// TopoOverride holds topology fields, of which a vector sets only some.
type TopoOverride struct {
	Relays            *int      `yaml:"relays"`
	Brokers           *int      `yaml:"brokers"`
	Replication       *string   `yaml:"replication"`
	Consumers         *[]string `yaml:"consumers"`
	ConsumersPerGroup *int      `yaml:"consumers_per_group"`
}

// Point is a crash or stall point.
type Point struct {
	Component string `yaml:"component"`
	Point     string `yaml:"point"`
	AfterN    int    `yaml:"after_n"`
	Instance  string `yaml:"instance"`
	MS        int    `yaml:"ms"`
}

// Range is a number or a [lo, hi] pair.
type Range struct {
	Lo, Hi float64
}

// UnmarshalYAML accepts both forms.
func (r *Range) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		if err := node.Decode(&r.Lo); err != nil {
			return err
		}
		r.Hi = r.Lo
		return nil
	}
	var pair []float64
	if err := node.Decode(&pair); err != nil {
		return err
	}
	if len(pair) != 2 {
		return fmt.Errorf("line %d: want [lo, hi]", node.Line)
	}
	r.Lo, r.Hi = pair[0], pair[1]
	return nil
}

// Mean is the expected value of a uniform draw.
func (r Range) Mean() float64 { return (r.Lo + r.Hi) / 2 }

// Fault is one entry of a vector's fault list.
type Fault struct {
	Type      string         `yaml:"type"`
	Target    string         `yaml:"target"`
	AtS       *Range         `yaml:"at_s"`
	OnLog     string         `yaml:"on_log"`
	DurationS *Range         `yaml:"duration_s"`
	Params    map[string]any `yaml:"params"`
}

// PatternOverride replaces fields for one pattern.
type PatternOverride struct {
	Crash  *Point         `yaml:"crash"`
	Stall  *Point         `yaml:"stall"`
	Faults []Fault        `yaml:"faults"`
	Env    map[string]any `yaml:"env"`
}

// Vector is one catalogue entry.
type Vector struct {
	ID              string                     `yaml:"id"`
	Group           string                     `yaml:"group"`
	Priority        string                     `yaml:"priority"`
	Mode            string                     `yaml:"mode"`
	Patterns        []string                   `yaml:"patterns"`
	Repeats         int                        `yaml:"repeats"`
	Load            LoadOverride               `yaml:"load"`
	Topology        TopoOverride               `yaml:"topology"`
	Env             map[string]any             `yaml:"env"`
	Sweep           map[string][]any           `yaml:"sweep"`
	Cells           []map[string]any           `yaml:"cells"`
	Crash           *Point                     `yaml:"crash"`
	Stall           *Point                     `yaml:"stall"`
	Faults          []Fault                    `yaml:"faults"`
	ByPattern       map[string]PatternOverride `yaml:"by_pattern"`
	Assert          map[string]int             `yaml:"assert"`
	AssertScope     string                     `yaml:"assert_scope"`
	RequiresRebuild bool                       `yaml:"requires_rebuild"`
	Expect          string                     `yaml:"expect"`
	Command         string                     `yaml:"command"`
	CoveredBy       []string                   `yaml:"covered_by"`
}

// LoadCatalogue parses the file and rejects unknown keys, so a typo can never
// silently change an experiment.
func LoadCatalogue(path string) (Catalogue, error) {
	file, err := os.Open(path)
	if err != nil {
		return Catalogue{}, fmt.Errorf("open catalogue: %w", err)
	}
	defer func() { _ = file.Close() }()

	var catalogue Catalogue
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&catalogue); err != nil {
		return Catalogue{}, fmt.Errorf("parse catalogue: %w", err)
	}
	return catalogue, nil
}

// Load is the resolved load for a cycle.
type Load struct {
	Mode                                     string
	RatePerS, WarmupS, PostFaultS, DurationS int
	BurstRatePerS, BurstAtS, BurstS          int
}

// Topology is the resolved topology for a cycle.
type Topology struct {
	Relays, Brokers, ConsumersPerGroup int
	Replication                        string
	Consumers                          []string
}

// Cycle is one (vector, pattern, cell, repeat).
type Cycle struct {
	ID              string
	VectorID        string
	Group           string
	Priority        string
	Mode            string
	Pattern         string
	Repeat          int
	Env             map[string]string
	Sweep           map[string]any
	Load            Load
	Drain           Drain
	Topology        Topology
	Crash           *Point
	Stall           *Point
	Faults          []Fault
	Assert          map[string]int
	AssertScope     string
	RequiresRebuild bool
}

// Expand produces every cycle of the catalogue, in file order.
func (c Catalogue) Expand() ([]Cycle, error) {
	var cycles []Cycle
	seen := make(map[string]struct{})
	for _, v := range c.Vectors {
		if v.Mode == "unit" || v.Mode == "alias" {
			continue
		}
		expanded, err := c.expandVector(v)
		if err != nil {
			return nil, fmt.Errorf("vector %s: %w", v.ID, err)
		}
		for _, cycle := range expanded {
			if _, dup := seen[cycle.ID]; dup {
				return nil, fmt.Errorf("duplicate cycle id %s", cycle.ID)
			}
			seen[cycle.ID] = struct{}{}
		}
		cycles = append(cycles, expanded...)
	}
	return cycles, nil
}

func (c Catalogue) expandVector(v Vector) ([]Cycle, error) {
	cells := v.Cells
	if len(cells) == 0 {
		cells = cartesian(v.Sweep)
	}
	repeats := v.Repeats
	if repeats == 0 {
		repeats = c.Defaults.Repeats
	}
	var cycles []Cycle
	for _, pattern := range v.Patterns {
		for index, cell := range cells {
			for repeat := 1; repeat <= repeats; repeat++ {
				cycle, err := c.resolve(v, pattern, cell)
				if err != nil {
					return nil, err
				}
				cycle.Repeat = repeat
				cycle.ID = fmt.Sprintf("%s_%s_r%02d", v.ID, pattern, repeat)
				if len(cells) > 1 {
					cycle.ID = fmt.Sprintf("%s_%s_c%d_r%02d", v.ID, pattern, index+1, repeat)
				}
				cycles = append(cycles, cycle)
			}
		}
	}
	return cycles, nil
}

func (c Catalogue) resolve(v Vector, pattern string, cell map[string]any) (Cycle, error) {
	cycle := Cycle{
		VectorID:        v.ID,
		Group:           v.Group,
		Priority:        v.Priority,
		Mode:            v.Mode,
		Pattern:         pattern,
		Sweep:           cell,
		Drain:           c.Defaults.Drain,
		Crash:           v.Crash,
		Stall:           v.Stall,
		Faults:          cloneFaults(v.Faults),
		Assert:          v.Assert,
		AssertScope:     v.AssertScope,
		RequiresRebuild: v.RequiresRebuild,
		Env:             make(map[string]string),
	}
	load := mergeLoad(c.Defaults.Load, v.Load)
	topology := mergeTopology(c.Defaults.Topology, v.Topology)
	setEnv(cycle.Env, c.Defaults.Env)
	setEnv(cycle.Env, v.Env)
	if override, ok := v.ByPattern[pattern]; ok {
		if override.Crash != nil {
			cycle.Crash = override.Crash
		}
		if override.Stall != nil {
			cycle.Stall = override.Stall
		}
		if override.Faults != nil {
			cycle.Faults = cloneFaults(override.Faults)
		}
		setEnv(cycle.Env, override.Env)
	}
	for _, key := range slices.Sorted(maps.Keys(cell)) {
		if err := applySweep(&cycle, &load, &topology, key, cell[key]); err != nil {
			return Cycle{}, err
		}
	}
	var err error
	if cycle.Load, err = resolveLoad(load); err != nil {
		return Cycle{}, err
	}
	if cycle.Topology, err = resolveTopology(topology); err != nil {
		return Cycle{}, err
	}
	return cycle, nil
}

func setEnv(env map[string]string, values map[string]any) {
	for key, value := range values {
		env[key] = fmt.Sprint(value)
	}
}

func cloneFaults(faults []Fault) []Fault {
	cloned := slices.Clone(faults)
	for i := range cloned {
		cloned[i].Params = maps.Clone(cloned[i].Params)
	}
	return cloned
}

func mergeLoad(base, over LoadOverride) LoadOverride {
	pick := func(a, b *int) *int {
		if b != nil {
			return b
		}
		return a
	}
	merged := LoadOverride{
		Mode:          base.Mode,
		RatePerS:      pick(base.RatePerS, over.RatePerS),
		WarmupS:       pick(base.WarmupS, over.WarmupS),
		PostFaultS:    pick(base.PostFaultS, over.PostFaultS),
		DurationS:     pick(base.DurationS, over.DurationS),
		BurstRatePerS: pick(base.BurstRatePerS, over.BurstRatePerS),
		BurstAtS:      pick(base.BurstAtS, over.BurstAtS),
		BurstS:        pick(base.BurstS, over.BurstS),
	}
	if over.Mode != nil {
		merged.Mode = over.Mode
	}
	return merged
}

func mergeTopology(base, over TopoOverride) TopoOverride {
	merged := base
	if over.Relays != nil {
		merged.Relays = over.Relays
	}
	if over.Brokers != nil {
		merged.Brokers = over.Brokers
	}
	if over.Replication != nil {
		merged.Replication = over.Replication
	}
	if over.Consumers != nil {
		merged.Consumers = over.Consumers
	}
	if over.ConsumersPerGroup != nil {
		merged.ConsumersPerGroup = over.ConsumersPerGroup
	}
	return merged
}

func resolveLoad(l LoadOverride) (Load, error) {
	if l.Mode == nil || l.RatePerS == nil || l.WarmupS == nil || l.PostFaultS == nil {
		return Load{}, errors.New("load needs mode, rate_per_s, warmup_s and post_fault_s")
	}
	value := func(p *int) int {
		if p == nil {
			return 0
		}
		return *p
	}
	return Load{
		Mode:          *l.Mode,
		RatePerS:      *l.RatePerS,
		WarmupS:       *l.WarmupS,
		PostFaultS:    *l.PostFaultS,
		DurationS:     value(l.DurationS),
		BurstRatePerS: value(l.BurstRatePerS),
		BurstAtS:      value(l.BurstAtS),
		BurstS:        value(l.BurstS),
	}, nil
}

func resolveTopology(t TopoOverride) (Topology, error) {
	if t.Relays == nil || t.Brokers == nil || t.Replication == nil || t.Consumers == nil || t.ConsumersPerGroup == nil {
		return Topology{}, errors.New("topology defaults are incomplete")
	}
	return Topology{
		Relays:            *t.Relays,
		Brokers:           *t.Brokers,
		Replication:       *t.Replication,
		Consumers:         *t.Consumers,
		ConsumersPerGroup: *t.ConsumersPerGroup,
	}, nil
}

var faultKey = regexp.MustCompile(`^faults\[(\d+)\]\.(.+)$`)

func applySweep(cycle *Cycle, load *LoadOverride, topology *TopoOverride, key string, value any) error {
	number := func() (*int, error) {
		n, ok := value.(int)
		if !ok {
			return nil, fmt.Errorf("sweep %s: want an integer, got %v", key, value)
		}
		return &n, nil
	}
	var err error
	switch {
	case key == "load.rate_per_s":
		load.RatePerS, err = number()
	case key == "topology.relays":
		topology.Relays, err = number()
	case strings.HasPrefix(key, "load."), strings.HasPrefix(key, "topology."):
		err = fmt.Errorf("unsupported sweep key %s", key)
	case strings.HasPrefix(key, "faults["):
		err = applyFaultSweep(cycle, key, value)
	default:
		cycle.Env[key] = fmt.Sprint(value)
	}
	return err
}

func applyFaultSweep(cycle *Cycle, key string, value any) error {
	match := faultKey.FindStringSubmatch(key)
	if match == nil {
		return fmt.Errorf("bad sweep key %s", key)
	}
	index, err := strconv.Atoi(match[1])
	if err != nil || index >= len(cycle.Faults) {
		return fmt.Errorf("sweep key %s: no such fault", key)
	}
	fault := &cycle.Faults[index]
	switch field := match[2]; {
	case field == "duration_s":
		seconds, ok := toFloat(value)
		if !ok {
			return fmt.Errorf("sweep %s: want a number, got %v", key, value)
		}
		fault.DurationS = &Range{Lo: seconds, Hi: seconds}
	case strings.HasPrefix(field, "params."):
		if fault.Params == nil {
			fault.Params = make(map[string]any)
		}
		fault.Params[strings.TrimPrefix(field, "params.")] = value
	default:
		return fmt.Errorf("unsupported sweep key %s", key)
	}
	return nil
}

func toFloat(value any) (float64, bool) {
	switch n := value.(type) {
	case int:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// cartesian returns every combination of the sweep, or one empty cell.
func cartesian(sweep map[string][]any) []map[string]any {
	cells := []map[string]any{{}}
	for _, key := range slices.Sorted(maps.Keys(sweep)) {
		var next []map[string]any
		for _, cell := range cells {
			for _, value := range sweep[key] {
				combined := maps.Clone(cell)
				combined[key] = value
				next = append(next, combined)
			}
		}
		cells = next
	}
	return cells
}

// Shard selects a share of the catalogue by vector.
type Shard struct {
	Index, Count int
}

// ParseShard reads "i/n".
func ParseShard(s string) (Shard, error) {
	var shard Shard
	if _, err := fmt.Sscanf(s, "%d/%d", &shard.Index, &shard.Count); err != nil {
		return Shard{}, fmt.Errorf("shard %q: want i/n", s)
	}
	if shard.Count < 1 || shard.Index < 1 || shard.Index > shard.Count {
		return Shard{}, fmt.Errorf("shard %q: want 1 <= i <= n", s)
	}
	return shard, nil
}

// Includes reports whether this shard runs the cycle. Every cycle of a vector
// lands on one machine, and performance vectors are never sharded.
func (s Shard) Includes(c Cycle) bool {
	if s.Count == 0 {
		return true
	}
	if c.Group == "perf" {
		return false
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(c.VectorID))
	return int(h.Sum32()%uint32(s.Count)) == s.Index-1
}

func (s Shard) String() string {
	if s.Count == 0 {
		return ""
	}
	return fmt.Sprintf("%d/%d", s.Index, s.Count)
}
