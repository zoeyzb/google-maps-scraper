package webrunner

import (
	"os"
	"strings"
	"testing"
)

// This is a source-level regression guard for the Maps production profile.
// Scrapemate v1.3.0 only bounds Playwright page.Close() in its page-slot path,
// which is selected when MaxPagesPerBrowser > 1. Do not silently fall back to
// the single-page cleanup path: a wedged driver there can trap mate.Start and
// force repeated Railway lane recycling.
func TestProductionBrowserJobsUseBoundedPageSlotCleanup(t *testing.T) {
	source, err := os.ReadFile("webrunner.go")
	if err != nil {
		t.Fatalf("read webrunner source: %v", err)
	}
	if !strings.Contains(string(source), "scrapemateapp.WithMaxPagesPerBrowser(2)") {
		t.Fatal("production JS browser jobs must force the bounded page-slot cleanup path")
	}
}
