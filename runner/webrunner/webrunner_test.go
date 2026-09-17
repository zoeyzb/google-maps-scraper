package webrunner

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gosom/google-maps-scraper/web"
	"github.com/gosom/scrapemate"
)

type fakeMateRunner struct {
	closeCalls atomic.Int32
	startDone  chan struct{}
}

func (f *fakeMateRunner) Start(context.Context, ...scrapemate.IJob) error {
	<-f.startDone
	return nil
}

func (f *fakeMateRunner) Close() error {
	f.closeCalls.Add(1)
	return nil
}

func TestWaitForMateStopReturnsSentinelWhenStartStaysBlockedAfterClose(t *testing.T) {
	mate := &fakeMateRunner{startDone: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- mate.Start(context.Background()) }()

	err := waitForMateStop(done, mate, 5*time.Millisecond, 5*time.Millisecond)
	if !errors.Is(err, errMateDidNotStop) {
		t.Fatalf("expected errMateDidNotStop, got %v", err)
	}
	if got := mate.closeCalls.Load(); got != 1 {
		t.Fatalf("expected exactly one close call, got %d", got)
	}
}

func TestScrapeJobDoesNotDoubleCloseMateWhenCleanupEscalates(t *testing.T) {
	// Regression contract: cleanup escalation owns the Close call. scrapeJob must
	// not also defer a second Close on the same poisoned browser lifecycle.
	mate := &fakeMateRunner{startDone: make(chan struct{})}
	w := &webrunner{
		cfg: &runner.Config{DataFolder: t.TempDir()},
		setupMate: func(context.Context, io.Writer, *web.Job) (mateRunner, error) {
			return mate, nil
		},
	}
	_ = w
	_ = mate
	t.Fatal("cleanup ownership is not yet injectable/testable; implement single-owner browser cleanup")
}
