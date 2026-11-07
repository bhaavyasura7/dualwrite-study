// Package crash implements targeted crash points (SPEC 7).
package crash

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"

	"dualwrite-study/internal/obs"
)

const defaultAfterN = 50

var (
	hits atomic.Int64
	kill = func() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }
)

// Point kills the process immediately if CRASH_AT equals name, on the
// CRASH_AFTER_N-th time the point is reached. attrs identify the event(s) being
// processed at this line, so the checker can separate hooked from collateral damage.
func Point(name string, attrs ...slog.Attr) {
	if name == "" || name != os.Getenv("CRASH_AT") {
		return
	}
	if hits.Add(1) != afterN() {
		return
	}
	// Stdout is unbuffered, so the marker is out once this call returns.
	slog.LogAttrs(context.Background(), slog.LevelInfo, obs.CrashPointHit, append(attrs, slog.String("point", name))...)
	kill()
}

func afterN() int64 {
	v := os.Getenv("CRASH_AFTER_N")
	if v == "" {
		return defaultAfterN
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		// A bad value must not turn into a cycle that silently never crashes.
		panic(fmt.Sprintf("invalid CRASH_AFTER_N %q", v))
	}
	return n
}
