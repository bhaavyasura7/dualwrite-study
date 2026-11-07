package faults

import (
	"context"
	"reflect"
	"testing"
)

type recorder struct{ calls []string }

func (r *recorder) note(op, name string) error {
	r.calls = append(r.calls, op+" "+name)
	return nil
}
func (r *recorder) Kill(_ context.Context, n string) error    { return r.note("kill", n) }
func (r *recorder) Start(_ context.Context, n string) error   { return r.note("start", n) }
func (r *recorder) Stop(_ context.Context, n string) error    { return r.note("stop", n) }
func (r *recorder) Restart(_ context.Context, n string) error { return r.note("restart", n) }
func (r *recorder) Pause(_ context.Context, n string) error   { return r.note("pause", n) }
func (r *recorder) Unpause(_ context.Context, n string) error { return r.note("unpause", n) }

func TestContainerFaults(t *testing.T) {
	names := []string{"a", "b"}
	for _, test := range []struct {
		build func(Containers) Fault
		want  []string
	}{
		{func(c Containers) Fault { return Pause(c, names) }, []string{"pause a", "pause b", "unpause a", "unpause b"}},
		{func(c Containers) Fault { return GracefulStop(c, names) }, []string{"stop a", "stop b", "start a", "start b"}},
		{func(c Containers) Fault { return BrokerStop(c, names) }, []string{"stop a", "stop b", "start a", "start b"}},
		{func(c Containers) Fault { return Restart(c, names) }, []string{"restart a", "restart b"}},
		{func(c Containers) Fault { return Crash(c, names, none) }, []string{"kill a", "kill b"}},
	} {
		r := &recorder{}
		f := test.build(r)
		if err := f.Inject(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := f.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(r.calls, test.want) {
			t.Errorf("%s: %v, want %v", f.Name(), r.calls, test.want)
		}
	}
}
