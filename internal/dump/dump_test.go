package dump

import (
	"path/filepath"
	"reflect"
	"testing"
)

// marker builds a control record's key: int16 version 0, int16 type.
func marker(kind int16) []byte { return []byte{0, 0, 0, byte(kind)} }

func offsets(records []Record) []int64 {
	var out []int64
	for _, r := range records {
		out = append(out, r.Offset)
	}
	return out
}

func TestCommitted(t *testing.T) {
	for _, test := range []struct {
		name string
		log  []Record
		want []int64
	}{
		{"plain records pass", []Record{{Offset: 0}, {Offset: 1}, {Offset: 2}}, []int64{0, 1, 2}},
		{
			"committed transaction kept, marker dropped",
			[]Record{{Offset: 0, Transactional: true, ProducerID: 7}, {Offset: 1, Transactional: true, ProducerID: 7},
				{Offset: 2, Control: true, Transactional: true, ProducerID: 7, Key: marker(controlCommit)}},
			[]int64{0, 1},
		},
		{
			"aborted transaction dropped",
			[]Record{{Offset: 0, Transactional: true, ProducerID: 7},
				{Offset: 1, Control: true, Transactional: true, ProducerID: 7, Key: marker(controlAbort)},
				{Offset: 2, Transactional: true, ProducerID: 7},
				{Offset: 3, Control: true, Transactional: true, ProducerID: 7, Key: marker(controlCommit)}},
			[]int64{2},
		},
		{
			"interleaved producers decided separately",
			[]Record{{Offset: 0, Transactional: true, ProducerID: 1}, {Offset: 1, Transactional: true, ProducerID: 2},
				{Offset: 2, Control: true, Transactional: true, ProducerID: 2, Key: marker(controlAbort)},
				{Offset: 3, Control: true, Transactional: true, ProducerID: 1, Key: marker(controlCommit)}},
			[]int64{0},
		},
		{
			"open transaction stops the partition at the last stable offset",
			[]Record{{Offset: 0}, {Offset: 1, Transactional: true, ProducerID: 9}, {Offset: 2},
				{Offset: 3, Transactional: true, ProducerID: 4},
				{Offset: 4, Control: true, Transactional: true, ProducerID: 4, Key: marker(controlCommit)}},
			[]int64{0},
		},
		{
			"partitions independent",
			[]Record{{Partition: 1, Offset: 0}, {Partition: 0, Offset: 0, Transactional: true, ProducerID: 3},
				{Partition: 0, Offset: 1, Control: true, Transactional: true, ProducerID: 3, Key: marker(controlCommit)}},
			[]int64{0, 0},
		},
	} {
		got, err := Committed(test.log)
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		if !reflect.DeepEqual(offsets(got), test.want) {
			t.Errorf("%s: got offsets %v, want %v", test.name, offsets(got), test.want)
		}
	}
}

func TestCommittedRejectsBadMarker(t *testing.T) {
	if _, err := Committed([]Record{{Control: true, Key: []byte{0}}}); err == nil {
		t.Error("a control record with a short key must be an error")
	}
	if _, err := Committed([]Record{{Control: true, Key: marker(5)}}); err == nil {
		t.Error("an unknown control type must be an error")
	}
}

func TestWriteRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.jsonl.gz")
	in := []Record{
		{Partition: 0, Offset: 0, TimestampMS: 1700000000123, TimestampType: 1, Key: []byte{0xff, 0x00},
			Value: []byte(`{"a":1}`), Headers: []Header{{"h", []byte("1")}, {"h", []byte{0xfe}}}, ProducerID: 5, ProducerEpoch: 2},
		{Partition: 2, Offset: 9, Control: true, Transactional: true, Key: marker(controlCommit)},
	}
	if err := write(path, in); err != nil {
		t.Fatal(err)
	}
	out, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip changed records:\n in  %+v\n out %+v", in, out)
	}
	if got := out[0].Timestamp().UnixMilli(); got != 1700000000123 {
		t.Errorf("timestamp %d", got)
	}
}
