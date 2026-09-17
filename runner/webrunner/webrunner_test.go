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

type hangingCloseMateRunner struct {
	closeCalls atomic.Int32
	closeBlock chan struct{}
}

func (f *hangingCloseMateRunner) Close() error {
	f.closeCalls.Add(1)
	<-f.closeBlock
	return nil
}

func TestWaitForMateStopReturnsSentinelWhenStartStaysBlockedAfterClose(t *testing.T) {
	mate := &stuckMateRunner{release: make(chan struct{})}
	closer := newOnceMateCloser(mate)
	done := make(chan error, 1)
	go func() { done <- mate.Start(context.Background()) }()

	err := waitForMateStop(done, closer, 5*time.Millisecond, 5*time.Millisecond)
	if !errors.Is(err, errMateDidNotStop) {
		t.Fatalf("expected errMateDidNotStop, got %v", err)
	}
	if got := mate.closeCalls.Load(); got != 1 {
		t.Fatalf("expected one forced Close call, got %d", got)
	}
}

func TestWaitForMateStopBoundsAHangingClose(t *testing.T) {
	mate := &hangingCloseMateRunner{closeBlock: make(chan struct{})}
	closer := newOnceMateCloser(mate)
	done := make(chan error, 1)

	started := time.Now()
	err := waitForMateStop(done, closer, 0, 10*time.Millisecond)
	elapsed := time.Since(started)
	close(mate.closeBlock)

	if !errors.Is(err, errMateDidNotStop) {
		t.Fatalf("expected errMateDidNotStop, got %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("hanging Close was not bounded; elapsed %s", elapsed)
	}
	if got := mate.closeCalls.Load(); got != 1 {
		t.Fatalf("expected one Close call, got %d", got)
	}
}

func TestRecoverMateStopHardKillsDescendantsAndKeepsLaneRecoverable(t *testing.T) {
	mate := &stuckMateRunner{release: make(chan struct{})}
	closer := newOnceMateCloser(mate)
	done := make(chan error, 1)
	go func() { done <- mate.Start(context.Background()) }()

	originalCleanup := forceBrowserCleanup
	defer func() { forceBrowserCleanup = originalCleanup }()
	cleanupCalls := atomic.Int32{}
	forceBrowserCleanup = func() (int, error) {
		cleanupCalls.Add(1)
		close(mate.release)
		return 2, nil
	}

	err := recoverMateStop(done, closer, 5*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond)
	if !errors.Is(err, errMateForceKilled) {
		t.Fatalf("expected errMateForceKilled, got %v", err)
	}
	if got := cleanupCalls.Load(); got != 1 {
		t.Fatalf("expected one hard cleanup call, got %d", got)
	}
	if got := mate.closeCalls.Load(); got != 1 {
		t.Fatalf("expected one normal Close before hard cleanup, got %d", got)
	}
}

func TestPoisonedMateLifecycleClosesOnlyOnce(t *testing.T) {
	mate := &stuckMateRunner{release: make(chan struct{})}
	closer := newOnceMateCloser(mate)
	done := make(chan error, 1)
	go func() { done <- mate.Start(context.Background()) }()

	_ = waitForMateStop(done, closer, 5*time.Millisecond, 5*time.Millisecond)
	_ = closer.Close()

	if got := mate.closeCalls.Load(); got != 1 {
		t.Fatalf("expected exactly one browser Close for a poisoned lifecycle, got %d", got)
	}
}
