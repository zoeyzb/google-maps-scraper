package webrunner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gosom/scrapemate"
)

type stuckMateRunner struct {
	closeCalls atomic.Int32
	release    chan struct{}
}

func (f *stuckMateRunner) Start(context.Context, ...scrapemate.IJob) error {
	<-f.release
	return nil
}

func (f *stuckMateRunner) Close() error {
	f.closeCalls.Add(1)
	return nil
}

func TestWaitForMateStopReturnsSentinelWhenStartStaysBlockedAfterClose(t *testing.T) {
	mate := &stuckMateRunner{release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- mate.Start(context.Background()) }()

	err := waitForMateStop(done, mate, 5*time.Millisecond, 5*time.Millisecond)
	if !errors.Is(err, errMateDidNotStop) {
		t.Fatalf("expected errMateDidNotStop, got %v", err)
	}
	if got := mate.closeCalls.Load(); got != 1 {
		t.Fatalf("expected one forced Close call, got %d", got)
	}
}

func TestPoisonedMateLifecycleClosesOnlyOnce(t *testing.T) {
	mate := &stuckMateRunner{release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- mate.Start(context.Background()) }()

	_ = waitForMateStop(done, mate, 5*time.Millisecond, 5*time.Millisecond)
	// scrapeJob currently also has `defer mate.Close()`. This explicit second
	// close models that deferred cleanup and should remain idempotent at the
	// lifecycle level after the fix.
	_ = mate.Close()

	if got := mate.closeCalls.Load(); got != 1 {
		t.Fatalf("expected exactly one browser Close for a poisoned lifecycle, got %d", got)
	}
}
