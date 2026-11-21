// Package dump writes and reads a cycle topic's raw log (SPEC 14.2, DUMP) and
// derives from it the committed view a read_committed consumer would see, so
// every Kafka-side metric can be recomputed from the file alone (SPEC 11.5).
package dump

import (
	"bufio"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Timeout bounds reading one cycle's topic.
const Timeout = 120 * time.Second

// Header is one record header, kept in order and as raw bytes, so repeated
// keys and binary values survive.
type Header struct {
	Key   string `json:"k"`
	Value []byte `json:"v"`
}

// Record is one offset of the log. Key and Value are raw bytes (base64 in JSON).
type Record struct {
	Partition     int32    `json:"p"`
	Offset        int64    `json:"o"`
	TimestampMS   int64    `json:"ts_ms"`
	TimestampType int8     `json:"ts_type"`
	Key           []byte   `json:"k,omitempty"`
	Value         []byte   `json:"v,omitempty"`
	Headers       []Header `json:"h,omitempty"`
	Control       bool     `json:"ctl,omitempty"`
	Transactional bool     `json:"txn,omitempty"`
	ProducerID    int64    `json:"pid"`
	ProducerEpoch int16    `json:"pe"`
}

// Timestamp is the record's timestamp.
func (r Record) Timestamp() time.Time { return time.UnixMilli(r.TimestampMS) }

// Stats is what a dump reports for the results row.
type Stats struct {
	Records, Control int64
	Bytes            int64
	Seconds          float64
}

// Topic writes every offset of topic, control records and aborted records
// included, to path as gzipped JSON lines, in partition and offset order. It
// verifies that each partition's offsets run from its log start to its end
// with no gap and no repeat, and leaves no file if they do not.
func Topic(ctx context.Context, seeds []string, topic, path string) (Stats, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		kgo.KeepControlRecords(),
	)
	if err != nil {
		return Stats{}, fmt.Errorf("dump client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return Stats{}, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return Stats{}, fmt.Errorf("list end offsets: %w", err)
	}
	next := make(map[int32]int64) // the offset each partition must deliver next
	end := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) {
		s, _ := starts.Lookup(topic, o.Partition)
		next[o.Partition], end[o.Partition] = s.Offset, o.Offset
	})

	byPartition := make(map[int32][]Record)
	for remaining := unfinished(next, end); remaining > 0; remaining = unfinished(next, end) {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			return Stats{}, fmt.Errorf("fetch %s (%d partitions unfinished): %w", topic, remaining, err)
		}
		var ferr error
		fetches.EachError(func(_ string, p int32, err error) { ferr = errors.Join(ferr, fmt.Errorf("partition %d: %w", p, err)) })
		if ferr != nil {
			return Stats{}, fmt.Errorf("fetch %s: %w", topic, ferr)
		}
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			if rec.Offset >= end[rec.Partition] {
				continue
			}
			if rec.Offset != next[rec.Partition] {
				return Stats{}, fmt.Errorf("partition %d: got offset %d, want %d (gap or repeat)", rec.Partition, rec.Offset, next[rec.Partition])
			}
			next[rec.Partition]++
			byPartition[rec.Partition] = append(byPartition[rec.Partition], fromKgo(rec))
		}
	}

	stats := Stats{}
	var all []Record
	for _, p := range slices.Sorted(maps.Keys(byPartition)) {
		all = append(all, byPartition[p]...)
	}
	for _, r := range all {
		stats.Records++
		if r.Control {
			stats.Control++
		}
	}
	if err := write(path, all); err != nil {
		return Stats{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Stats{}, err
	}
	stats.Bytes, stats.Seconds = info.Size(), time.Since(start).Seconds()
	return stats, nil
}

func unfinished(next, end map[int32]int64) int {
	n := 0
	for p, e := range end {
		if next[p] < e {
			n++
		}
	}
	return n
}

func fromKgo(rec *kgo.Record) Record {
	r := Record{
		Partition: rec.Partition, Offset: rec.Offset, TimestampMS: rec.Timestamp.UnixMilli(),
		TimestampType: rec.Attrs.TimestampType(), Key: rec.Key, Value: rec.Value,
		Control: rec.Attrs.IsControl(), Transactional: rec.Attrs.IsTransactional(),
		ProducerID: rec.ProducerID, ProducerEpoch: rec.ProducerEpoch,
	}
	for _, h := range rec.Headers {
		r.Headers = append(r.Headers, Header{Key: h.Key, Value: h.Value})
	}
	return r
}

// write stores records atomically: a temporary file, fsynced, then renamed.
func write(path string, records []Record) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(tmp) }()
	zw := gzip.NewWriter(file)
	buf := bufio.NewWriter(zw)
	encoder := json.NewEncoder(buf)
	for _, r := range records {
		if err := encoder.Encode(r); err != nil {
			return err
		}
	}
	if err := errors.Join(buf.Flush(), zw.Close(), file.Sync(), file.Close()); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Read loads a dump file.
func Read(path string) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	zr, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	decoder := json.NewDecoder(zr)
	var records []Record
	for {
		var r Record
		if err := decoder.Decode(&r); errors.Is(err, io.EOF) {
			return records, nil
		} else if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		records = append(records, r)
	}
}

// Control record types, from the first four bytes of a control record's key:
// int16 version, int16 type.
const (
	controlAbort  = 0
	controlCommit = 1
)

func controlType(key []byte) (int16, error) {
	if len(key) < 4 {
		return 0, fmt.Errorf("control record key of %d bytes", len(key))
	}
	return int16(binary.BigEndian.Uint16(key[2:4])), nil
}

// Committed returns the data records a read_committed consumer reading to the
// last stable offset would receive, in partition and offset order: records of
// committed transactions and non-transactional records, without control
// records, without aborted records, and stopping each partition at its first
// still-open transaction, as the broker's last stable offset does.
func Committed(records []Record) ([]Record, error) {
	sorted := slices.Clone(records)
	slices.SortFunc(sorted, func(a, b Record) int {
		return cmp.Or(cmp.Compare(a.Partition, b.Partition), cmp.Compare(a.Offset, b.Offset))
	})
	var out []Record
	for start := 0; start < len(sorted); {
		stop := start
		for stop < len(sorted) && sorted[stop].Partition == sorted[start].Partition {
			stop++
		}
		part, err := committedPartition(sorted[start:stop])
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
		start = stop
	}
	return out, nil
}

func committedPartition(records []Record) ([]Record, error) {
	type txn struct {
		first   int64
		records []Record
	}
	open := make(map[int64]*txn)        // by producer ID
	decided := make(map[int64][]Record) // offset of the decision -> records released there
	var plain []Record
	for _, r := range records {
		switch {
		case r.Control:
			kind, err := controlType(r.Key)
			if err != nil {
				return nil, fmt.Errorf("partition %d offset %d: %w", r.Partition, r.Offset, err)
			}
			t := open[r.ProducerID]
			delete(open, r.ProducerID)
			if t != nil && kind == controlCommit {
				decided[r.Offset] = t.records
			}
			if kind != controlCommit && kind != controlAbort {
				return nil, fmt.Errorf("partition %d offset %d: control type %d", r.Partition, r.Offset, kind)
			}
		case r.Transactional:
			t := open[r.ProducerID]
			if t == nil {
				t = &txn{first: r.Offset}
				open[r.ProducerID] = t
			}
			t.records = append(t.records, r)
		default:
			plain = append(plain, r)
		}
	}
	// The last stable offset: the first offset of the earliest transaction
	// still open at the end of the log.
	lso := int64(-1)
	for _, t := range open {
		if lso < 0 || t.first < lso {
			lso = t.first
		}
	}
	out := slices.Clone(plain)
	for _, released := range decided {
		out = append(out, released...)
	}
	if lso >= 0 {
		out = slices.DeleteFunc(out, func(r Record) bool { return r.Offset >= lso })
	}
	slices.SortFunc(out, func(a, b Record) int { return cmp.Compare(a.Offset, b.Offset) })
	return out, nil
}

// WriteForTest stores records as Topic would, for tests in other packages.
func WriteForTest(path string, records []Record) error { return write(path, records) }
