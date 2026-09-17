package webrunner

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gosom/google-maps-scraper/deduper"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/google-maps-scraper/runner"
	"github.com/gosom/google-maps-scraper/tlmt"
	"github.com/gosom/google-maps-scraper/web"
	"github.com/gosom/google-maps-scraper/web/sqlite"
	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/writers/csvwriter"
	"github.com/gosom/scrapemate/scrapemateapp"
	"golang.org/x/sync/errgroup"
)

type webrunner struct {
	srv       *web.Server
	svc       *web.Service
	cfg       *runner.Config
	setupMate func(context.Context, io.Writer, *web.Job) (mateRunner, error)
}

type mateRunner interface {
	Start(context.Context, ...scrapemate.IJob) error
	Close() error
}

type mateCloser interface {
	Close() error
}

type onceMateCloser struct {
	mate mateCloser
	once sync.Once
	err  error
}

func newOnceMateCloser(mate mateCloser) *onceMateCloser {
	return &onceMateCloser{mate: mate}
}

func (c *onceMateCloser) Close() error {
	c.once.Do(func() { c.err = c.mate.Close() })
	return c.err
}

var (
	errMateDidNotStop = errors.New("scraper did not stop after close")
	errMateForceKilled = errors.New("scraper browser descendants force-killed after stuck close")
	forceBrowserCleanup = func() (int, error) {
		return killDescendantProcesses(os.Getpid())
	}
)

// waitForMateStop gives a scraper a bounded grace period to finish after its
// context is cancelled. Close itself is also bounded: Playwright can wedge
// inside Close(), so it must never be allowed to pin the Maps lane forever.
func waitForMateStop(done <-chan error, closer mateCloser, cancelGrace, closeGrace time.Duration) error {
	if cancelGrace > 0 {
		timer := time.NewTimer(cancelGrace)
		select {
		case err := <-done:
			timer.Stop()
			return err
		case <-timer.C:
		}
	} else {
		select {
		case err := <-done:
			return err
		default:
		}
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- closer.Close() }()

	if closeGrace <= 0 {
		select {
		case err := <-done:
			return err
		case <-closeDone:
			select {
			case err := <-done:
				return err
			default:
				return errMateDidNotStop
			}
		default:
			return errMateDidNotStop
		}
	}

	timer := time.NewTimer(closeGrace)
	defer timer.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-closeDone:
			// Close returning does not guarantee Start returned. Keep waiting
			// inside the same bounded grace period for the Start goroutine.
			closeDone = nil
		case <-timer.C:
			return errMateDidNotStop
		}
	}
}

// recoverMateStop is the second-stage recovery path for a poisoned Playwright
// lifecycle. If normal cancellation + Close cannot stop mate.Start, kill only
// descendant browser/driver processes and give the goroutine a short chance to
// unwind. A recovered poisoned job fails, but the HTTP Maps lane stays alive.
func recoverMateStop(done <-chan error, closer mateCloser, cancelGrace, closeGrace, hardGrace time.Duration) error {
	err := waitForMateStop(done, closer, cancelGrace, closeGrace)
	if !errors.Is(err, errMateDidNotStop) {
		return err
	}

	killed, killErr := forceBrowserCleanup()
	if killErr != nil {
		log.Printf("hard browser cleanup killed %d descendants with errors: %v", killed, killErr)
	} else {
		log.Printf("hard browser cleanup killed %d descendants", killed)
	}

	if hardGrace <= 0 {
		select {
		case <-done:
			return fmt.Errorf("%w: killed %d descendants", errMateForceKilled, killed)
		default:
			return errMateDidNotStop
		}
	}

	timer := time.NewTimer(hardGrace)
	defer timer.Stop()
	select {
	case startErr := <-done:
		if startErr != nil && !errors.Is(startErr, context.Canceled) && !errors.Is(startErr, context.DeadlineExceeded) {
			return fmt.Errorf("%w: killed %d descendants; scraper exit: %v", errMateForceKilled, killed, startErr)
		}
		return fmt.Errorf("%w: killed %d descendants", errMateForceKilled, killed)
	case <-timer.C:
		return errMateDidNotStop
	}
}

func New(cfg *runner.Config) (runner.Runner, error) {
	if cfg.DataFolder == "" {
		return nil, fmt.Errorf("data folder is required")
	}

	if err := os.MkdirAll(cfg.DataFolder, os.ModePerm); err != nil {
		return nil, err
	}

	const dbfname = "jobs.db"
	dbpath := filepath.Join(cfg.DataFolder, dbfname)

	repo, err := sqlite.New(dbpath)
	if err != nil {
		return nil, err
	}

	svc := web.NewService(repo, cfg.DataFolder)
	srv, err := web.New(svc, cfg.Addr)
	if err != nil {
		return nil, err
	}

	ans := webrunner{
		srv:       srv,
		svc:       svc,
		cfg:       cfg,
		setupMate: defaultSetupMate(cfg),
	}
	return &ans, nil
}

func (w *webrunner) Run(ctx context.Context) error {
	egroup, ctx := errgroup.WithContext(ctx)
	egroup.Go(func() error { return w.work(ctx) })
	egroup.Go(func() error { return w.srv.Start(ctx) })
	return egroup.Wait()
}

func (w *webrunner) Close(context.Context) error { return nil }

func (w *webrunner) work(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			jobs, err := w.svc.SelectPending(ctx)
			if err != nil {
				return err
			}

			for i := range jobs {
				select {
				case <-ctx.Done():
					return nil
				default:
					t0 := time.Now().UTC()
					if err := w.scrapeJob(ctx, &jobs[i]); err != nil {
						params := map[string]any{
							"job_count": len(jobs[i].Data.Keywords),
							"duration":  time.Since(t0).String(),
							"error":     err.Error(),
						}
						_ = runner.Telemetry().Send(ctx, tlmt.NewEvent("web_runner", params))
						log.Printf("error scraping job %s: %v", jobs[i].ID, err)

						if errors.Is(err, errMateDidNotStop) {
							log.Printf("stuck browser cleanup failed for job %s after hard descendant kill; recycling Maps lane", jobs[i].ID)
							return err
						}
						if errors.Is(err, errMateForceKilled) {
							log.Printf("poisoned browser recovered for job %s; keeping Maps lane online", jobs[i].ID)
						}
					} else {
						params := map[string]any{
							"job_count": len(jobs[i].Data.Keywords),
							"duration":  time.Since(t0).String(),
						}
						_ = runner.Telemetry().Send(ctx, tlmt.NewEvent("web_runner", params))
						log.Printf("job %s scraped successfully", jobs[i].ID)
					}
				}
			}
		}
	}
}

func (w *webrunner) scrapeJob(ctx context.Context, job *web.Job) error {
	job.Status = web.StatusWorking
	if err := w.svc.Update(ctx, job); err != nil {
		return err
	}

	if len(job.Data.Keywords) == 0 {
		job.Status = web.StatusFailed
		return w.svc.Update(ctx, job)
	}

	outpath := filepath.Join(w.cfg.DataFolder, job.ID+".csv")
	outfile, err := os.Create(outpath)
	if err != nil {
		return err
	}
	defer func() { _ = outfile.Close() }()

	setupMate := w.setupMate
	if setupMate == nil {
		setupMate = defaultSetupMate(w.cfg)
	}
	mate, err := setupMate(ctx, outfile, job)
	if err != nil {
		job.Status = web.StatusFailed
		if err2 := w.svc.Update(ctx, job); err2 != nil {
			log.Printf("failed to update job status: %v", err2)
		}
		return err
	}

	mateCloser := newOnceMateCloser(mate)
	defer mateCloser.Close()

	var coords string
	if job.Data.Lat != "" && job.Data.Lon != "" {
		coords = job.Data.Lat + "," + job.Data.Lon
	}

	dedup := deduper.New()
	exitMonitor := exiter.New()
	seedJobs, err := runner.CreateSeedJobs(
		job.Data.FastMode,
		job.Data.Lang,
		strings.NewReader(strings.Join(job.Data.Keywords, "\n")),
		job.Data.Depth,
		job.Data.Email,
		coords,
		job.Data.Zoom,
		func() float64 {
			if job.Data.Radius <= 0 {
				return 10000
			}
			return float64(job.Data.Radius)
		}(),
		dedup,
		exitMonitor,
		w.cfg.ExtraReviews || job.Data.ExtraReviews,
	)
	if err != nil {
		if err2 := w.svc.Update(ctx, job); err2 != nil {
			log.Printf("failed to update job status: %v", err2)
		}
		return err
	}

	if len(seedJobs) > 0 {
		exitMonitor.SetSeedCount(len(seedJobs))
		allowedSeconds := max(60, len(seedJobs)*10*job.Data.Depth/50+120)
		if job.Data.MaxTime > 0 {
			if job.Data.MaxTime.Seconds() < 180 {
				allowedSeconds = 180
			} else {
				allowedSeconds = int(job.Data.MaxTime.Seconds())
			}
		}

		log.Printf("running job %s with %d seed jobs and %d allowed seconds", job.ID, len(seedJobs), allowedSeconds)
		mateCtx, cancel := context.WithTimeout(ctx, time.Duration(allowedSeconds)*time.Second)
		defer cancel()
		exitMonitor.SetCancelFunc(cancel)
		go exitMonitor.Run(mateCtx)

		done := make(chan error, 1)
		go func() { done <- mate.Start(mateCtx, seedJobs...) }()

		select {
		case err = <-done:
		case <-mateCtx.Done():
			ctxErr := mateCtx.Err()
			if errors.Is(ctxErr, context.Canceled) {
				log.Printf("job %s cancelled; waiting for scraper shutdown", job.ID)
				err = recoverMateStop(done, mateCloser, 3*time.Second, 4*time.Second, 3*time.Second)
			} else {
				log.Printf("job %s hit hard timeout after %d seconds; closing scraper", job.ID, allowedSeconds)
				err = recoverMateStop(done, mateCloser, 0, 4*time.Second, 3*time.Second)
			}

			if errors.Is(err, errMateDidNotStop) {
				job.Status = web.StatusFailed
				if updateErr := w.svc.Update(context.WithoutCancel(ctx), job); updateErr != nil {
					log.Printf("failed to update stuck job status: %v", updateErr)
				}
				log.Printf("job %s failed to stop even after hard browser cleanup; marking job failed before lane recycle", job.ID)
				return fmt.Errorf("job %s: %w", job.ID, errMateDidNotStop)
			}

			if errors.Is(err, errMateForceKilled) {
				job.Status = web.StatusFailed
				if updateErr := w.svc.Update(context.WithoutCancel(ctx), job); updateErr != nil {
					log.Printf("failed to update force-killed job status: %v", updateErr)
				}
				return fmt.Errorf("job %s: %w", job.ID, err)
			}
		}

		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			cancel()
			if err2 := w.svc.Update(ctx, job); err2 != nil {
				log.Printf("failed to update job status: %v", err2)
			}
			return err
		}
		cancel()
	}

	job.Status = web.StatusOK
	return w.svc.Update(ctx, job)
}

func defaultSetupMate(cfg *runner.Config) func(context.Context, io.Writer, *web.Job) (mateRunner, error) {
	return func(_ context.Context, writer io.Writer, job *web.Job) (mateRunner, error) {
		opts := []func(*scrapemateapp.Config) error{
			scrapemateapp.WithConcurrency(cfg.Concurrency),
			scrapemateapp.WithExitOnInactivity(time.Minute * 3),
		}

		if !job.Data.FastMode {
			opts = append(opts, scrapemateapp.WithJS(scrapemateapp.DisableImages()))
		} else {
			opts = append(opts, scrapemateapp.WithStealth("firefox"))
		}

		opts = runner.AppendBrowserCapacityOptions(opts, cfg)

		// Scrapemate's default single-page JS path performs several Playwright
		// cleanup calls synchronously. Force the page-slot path, where page.Close
		// is bounded, so a wedged page is less likely to poison the whole mate.
		if !job.Data.FastMode {
			opts = append(opts, scrapemateapp.WithMaxPagesPerBrowser(2))
		}

		hasProxy := false
		if len(cfg.Proxies) > 0 {
			opts = append(opts, scrapemateapp.WithProxies(cfg.Proxies))
			hasProxy = true
		} else if len(job.Data.Proxies) > 0 {
			opts = append(opts, scrapemateapp.WithProxies(job.Data.Proxies))
			hasProxy = true
		}

		if !cfg.DisablePageReuse {
			opts = append(opts,
				scrapemateapp.WithPageReuseLimit(2),
				scrapemateapp.WithBrowserReuseLimit(200),
			)
		}

		log.Printf("job %s has proxy: %v", job.ID, hasProxy)
		csvWriter := csvwriter.NewCsvWriter(csv.NewWriter(writer))
		writers := []scrapemate.ResultWriter{csvWriter}
		matecfg, err := scrapemateapp.NewConfig(writers, opts...)
		if err != nil {
			return nil, err
		}
		return scrapemateapp.NewScrapeMateApp(matecfg)
	}
}
