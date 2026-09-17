//go:build linux

package webrunner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// killDescendantProcesses force-kills every process descended from rootPID,
// deepest first. The Maps web runner processes scrape jobs serially, so any
// Playwright/browser descendants at this point belong to the poisoned job.
// The HTTP server process itself is never killed.
func killDescendantProcesses(rootPID int) (int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("read /proc: %w", err)
	}

	children := make(map[int][]int)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || pid == rootPID {
			continue
		}
		ppid, err := procParentPID(pid)
		if err != nil || ppid <= 0 {
			continue
		}
		children[ppid] = append(children[ppid], pid)
	}

	var descendants []int
	var walk func(int)
	walk = func(pid int) {
		for _, child := range children[pid] {
			walk(child)
			descendants = append(descendants, child)
		}
	}
	walk(rootPID)

	// walk already emits deepest-first, but keep ordering deterministic for
	// sibling processes so logs/tests are stable.
	if len(descendants) > 1 {
		// Preserve depth grouping by only sorting equal-parent siblings is more
		// complexity than it is worth; SIGKILL is idempotent for our purpose.
		// Reverse numeric order simply makes repeated runs deterministic enough.
		sort.SliceStable(descendants, func(i, j int) bool { return descendants[i] > descendants[j] })
	}

	killed := 0
	var errs []error
	for _, pid := range descendants {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				continue
			}
			errs = append(errs, fmt.Errorf("kill pid %d: %w", pid, err))
			continue
		}
		killed++
	}
	return killed, errors.Join(errs...)
}

func procParentPID(pid int) (int, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	// comm is wrapped in parentheses and can contain spaces. Everything after
	// the final ')' starts with state then ppid.
	text := string(data)
	closeIdx := strings.LastIndex(text, ")")
	if closeIdx < 0 || closeIdx+1 >= len(text) {
		return 0, fmt.Errorf("malformed stat for pid %d", pid)
	}
	fields := strings.Fields(text[closeIdx+1:])
	if len(fields) < 2 {
		return 0, fmt.Errorf("missing ppid for pid %d", pid)
	}
	return strconv.Atoi(fields[1])
}
