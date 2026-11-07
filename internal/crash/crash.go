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
	"time"

	"dualwrite-study/internal/obs"
)

const defaultAfterN = 50

var (
	hits      atomic.Int64
	stallHits atomic.Int64
	kill      = func() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }
	sleep     = time.Sleep
)

// Point kills the process immediately if CRASH_AT equals name, on the
// CRASH_AFTER_N-th time the point is reached. attrs identify the event(s) being
// processed at this line, so the checker can separate hooked from collateral damage.
func Point(name string, attrs ...slog.Attr) {
	if name == "" || name != os.Getenv("CRASH_AT") {
		return
	}
	if hits.Add(1) != afterN("CRASH_AFTER_N") {
		return
	}
	// Stdout is unbuffered, so the marker is out once this call returns.
	slog.LogAttrs(context.Background(), slog.LevelInfo, obs.CrashPointHit, append(attrs, slog.String("point", name))...)
	kill()
}

// Stall blocks the calling goroutine for STALL_MS if STALL_AT equals name, on the
// STALL_AFTER_N-th time the point is reached (SPEC 7.2). The client libraries'
// background goroutines keep running, unlike under docker pause.
func Stall(name string, attrs ...slog.Attr) {
	if name == "" || name != os.Getenv("STALL_AT") {
		return
	}
	if stallHits.Add(1) != afterN("STALL_AFTER_N") {
		return
	}
	ms, err := strconv.Atoi(os.Getenv("STALL_MS"))
	if err != nil || ms < 1 {
		panic(fmt.Sprintf("invalid STALL_MS %q", os.Getenv("STALL_MS")))
	}
	attrs = append(attrs, slog.String("point", name))
	slog.LogAttrs(context.Background(), slog.LevelInfo, obs.StallPointHit, attrs...)
	sleep(time.Duration(ms) * time.Millisecond)
	slog.LogAttrs(context.Background(), slog.LevelInfo, obs.StallPointEnd, attrs...)
}

func afterN(key string) int64 {
	v := os.Getenv(key)
	if v == "" {
		return defaultAfterN
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		// A bad value must not turn into a cycle that silently never fires.
		panic(fmt.Sprintf("invalid %s %q", key, v))
	}
	return n
}
