// Package faults injects and recovers the study's failures (SPEC 12).
package faults

import (
	"context"
	"errors"
	"fmt"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
)

// Fault is one injected failure.
type Fault interface {
	Inject(ctx context.Context) error  // start the fault
	Recover(ctx context.Context) error // end it and restore service
	Name() string
}

// Containers is the Docker control a fault needs.
type Containers interface {
	Kill(ctx context.Context, name string) error
	Start(ctx context.Context, name string) error
	Stop(ctx context.Context, name string) error
	Restart(ctx context.Context, name string) error
	Pause(ctx context.Context, name string) error
	Unpause(ctx context.Context, name string) error
}

type step func(context.Context) error

type fault struct {
	name             string
	inject, recovery step
}

func (f fault) Inject(ctx context.Context) error  { return f.inject(ctx) }
func (f fault) Recover(ctx context.Context) error { return f.recovery(ctx) }
func (f fault) Name() string                      { return f.name }

// New builds a fault from two steps, for failures that need the runner's state.
func New(name string, inject, recovery func(context.Context) error) Fault {
	return fault{name, inject, recovery}
}

func none(context.Context) error { return nil }

func each(names []string, action func(context.Context, string) error) step {
	return func(ctx context.Context) error {
		var errs []error
		for _, name := range names {
			errs = append(errs, action(ctx, name))
		}
		return errors.Join(errs...)
	}
}

// Crash SIGKILLs the containers. restore brings them back: a Go component is
// recreated by the runner, and others are simply started.
func Crash(c Containers, names []string, restore func(context.Context) error) Fault {
	return fault{"crash", each(names, c.Kill), restore}
}

// GracefulStop stops the containers and starts them on recovery.
func GracefulStop(c Containers, names []string) Fault {
	return fault{"graceful_stop", each(names, c.Stop), each(names, c.Start)}
}

// Restart restarts the containers gracefully.
func Restart(c Containers, names []string) Fault {
	return fault{"restart", each(names, c.Restart), none}
}

// Pause freezes the containers.
func Pause(c Containers, names []string) Fault {
	return fault{"pause", each(names, c.Pause), each(names, c.Unpause)}
}

// BrokerStop stops broker containers and starts them on recovery.
func BrokerStop(c Containers, names []string) Fault {
	return fault{"broker_stop", each(names, c.Stop), each(names, c.Start)}
}

func proxies(client *toxiproxy.Client, names []string, action func(*toxiproxy.Proxy) error) step {
	return func(context.Context) error {
		var errs []error
		for _, name := range names {
			proxy, err := client.Proxy(name)
			if err != nil {
				errs = append(errs, fmt.Errorf("proxy %s: %w", name, err))
				continue
			}
			errs = append(errs, action(proxy))
		}
		return errors.Join(errs...)
	}
}

// ProxyDisable cuts the proxy group.
func ProxyDisable(client *toxiproxy.Client, names []string) Fault {
	return fault{
		"proxy_disable",
		proxies(client, names, (*toxiproxy.Proxy).Disable),
		proxies(client, names, (*toxiproxy.Proxy).Enable),
	}
}

type toxic struct {
	name, kind, stream string
	toxicity           float32
	attributes         toxiproxy.Attributes
}

func withToxics(label string, client *toxiproxy.Client, names []string, toxics []toxic) Fault {
	add := proxies(client, names, func(p *toxiproxy.Proxy) error {
		for _, t := range toxics {
			if _, err := p.AddToxic(t.name, t.kind, t.stream, t.toxicity, t.attributes); err != nil {
				return fmt.Errorf("add %s to %s: %w", t.name, p.Name, err)
			}
		}
		return nil
	})
	remove := proxies(client, names, func(p *toxiproxy.Proxy) error {
		for _, t := range toxics {
			if err := p.RemoveToxic(t.name); err != nil {
				return fmt.Errorf("remove %s from %s: %w", t.name, p.Name, err)
			}
		}
		return nil
	})
	return fault{label, add, remove}
}

// Latency delays both streams of the proxy group.
func Latency(client *toxiproxy.Client, names []string, latencyMS, jitterMS int) Fault {
	attributes := toxiproxy.Attributes{"latency": latencyMS, "jitter": jitterMS}
	return withToxics("latency", client, names, []toxic{
		{"latency_up", "latency", "upstream", 1, attributes},
		{"latency_down", "latency", "downstream", 1, attributes},
	})
}

// WithholdDownstream lets requests reach the server but drops every response
// until removed: lost acknowledgements.
func WithholdDownstream(client *toxiproxy.Client, names []string) Fault {
	return withToxics("withhold_downstream", client, names, []toxic{
		{"withhold", "timeout", "downstream", 1, toxiproxy.Attributes{"timeout": 0}},
	})
}

// ResetPeer resets the given share of connections.
func ResetPeer(client *toxiproxy.Client, names []string, toxicity float32) Fault {
	return withToxics("reset_peer", client, names, []toxic{
		{"reset", "reset_peer", "upstream", toxicity, toxiproxy.Attributes{"timeout": 0}},
	})
}
