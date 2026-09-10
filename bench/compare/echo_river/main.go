// River (Go + PostgreSQL) head-to-head benchmark against ChopFlow.
//
// River is a durable, transactional job queue for Postgres. It fills the gap
// between Celery (fire-and-forget, Redis) and Temporal (full durable workflow
// engine): it persists per-task state to Postgres but carries no workflow/replay
// machinery. So its numbers here are the "cost of lightweight durability" —
// the middle datapoint the comparison was missing.
//
// Same workload (echo / resize), same sweep, one worker with 4 job slots
// (matches ChopFlow cpu:4, Celery --concurrency=4, apalis ConcurrencyLimit(4)).
// The worker runs in-process (same shape as echo_temporal.py / echo_apalis);
// completion is tracked via an in-process atomic the handler bumps, mirroring
// each system's lightest completion signal.
//
// Prerequisites (River needs PostgreSQL — none of the other systems do):
//   - a Postgres instance, e.g. brew install postgresql@16 && pg_ctl start
//   - DATABASE_URL set, e.g. postgres://localhost:5432/riverbench?sslmode=disable
//   - the database must exist (createdb riverbench); River creates its own schema
//
// Usage:
//   DATABASE_URL=postgres://localhost:5432/riverbench?sslmode=disable \
//     go run . --tasks 10000 --concurrency 32 --sample-size 500
//
// Emits the shared machine-readable line:
//   RESULT system=river tasks=N conc=C workload=echo|resize throughput=T submit_s=.. drain_s=.. p50_ms=.. p95_ms=.. p99_ms=.. failures=..
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"image"
	"image/color"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// ---- job args + worker ---------------------------------------------------

type Args struct {
	I    int    `json:"i"`
	Work string `json:"work"`
	W    int    `json:"w"`
	H    int    `json:"h"`
}

func (Args) Kind() string { return "echo" }

type State struct {
	completed atomic.Int64
	failures  atomic.Int64

	mu           sync.RWMutex
	sample       map[int]bool
	submittedAt  map[int]time.Time
	completedAt  map[int]time.Time
}

func (s *State) markSubmitted(i int) {
	s.mu.RLock()
	if s.sample[i] {
		s.mu.RUnlock()
		s.mu.Lock()
		s.submittedAt[i] = time.Now()
		s.mu.Unlock()
		return
	}
	s.mu.RUnlock()
}

func (s *State) markCompleted(i int) {
	s.mu.RLock()
	if s.sample[i] {
		s.mu.RUnlock()
		s.mu.Lock()
		s.completedAt[i] = time.Now()
		s.mu.Unlock()
		return
	}
	s.mu.RUnlock()
}

type Worker struct {
	river.WorkerDefaults[Args]
	state *State
}

func (w *Worker) Work(ctx context.Context, job *river.Job[Args]) error {
	if err := doWork(job.Args); err != nil {
		w.state.failures.Add(1)
		w.state.completed.Add(1)
		w.state.markCompleted(job.Args.I)
		return err
	}
	w.state.completed.Add(1)
	w.state.markCompleted(job.Args.I)
	return nil
}

func doWork(a Args) error {
	if a.Work != "resize" {
		return nil
	}
	w := a.W
	if w < 1 {
		w = 1
	}
	h := a.H
	if h < 1 {
		h = 1
	}
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for x := 0; x < w; x++ {
		for y := 0; y < h; y++ {
			r := uint8(float64(x) / float64(w) * 255)
			g := uint8(float64(y) / float64(h) * 255)
			img.Set(x, y, color.RGBA{R: r, G: g, B: 128, A: 255})
		}
	}
	// nearest-neighbor half-size resize via the standard library
	bounds := img.Bounds()
	ow := w / 2
	if ow < 1 {
		ow = 1
	}
	oh := h / 2
	if oh < 1 {
		oh = 1
	}
	_ = bounds
	// SubSample (Nearest) is not in stdlib; emulate by drawing scaled pixels.
	out := image.NewRGBA(image.Rect(0, 0, ow, oh))
	for x := 0; x < ow; x++ {
		for y := 0; y < oh; y++ {
			out.Set(x, y, img.At(x*2, y*2))
		}
	}
	_ = out
	return nil
}

// ---- helpers -------------------------------------------------------------

func pickSample(n, sampleSize int) map[int]bool {
	m := map[int]bool{}
	if sampleSize <= 0 || sampleSize >= n {
		for i := 0; i < n; i++ {
			m[i] = true
		}
		return m
	}
	stride := float64(n) / float64(sampleSize)
	for i := 0; i < sampleSize; i++ {
		m[int(math.Round(float64(i)*stride))] = true
	}
	return m
}

func pct(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	s := append([]float64(nil), values...)
	sort.Float64s(s)
	k := int(math.Round(q * float64(len(s)-1)))
	if k < 0 {
		k = 0
	}
	if k > len(s)-1 {
		k = len(s) - 1
	}
	return s[k]
}

func main() {
	tasks := flag.Int("tasks", 1000, "")
	conc := flag.Int("concurrency", 16, "")
	sampleSize := flag.Int("sample-size", 500, "")
	workload := flag.String("workload", "echo", "")
	w := flag.Int("width", 256, "")
	h := flag.Int("height", 256, "")
	pollInterval := flag.Float64("poll-interval", 0.001, "")
	timeBudget := flag.Float64("time-budget", 0, "")
	flag.Parse()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://localhost:5432/riverbench?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("cannot connect to Postgres (%s): %v\n(start postgres, create the db, set DATABASE_URL)", dbURL, err)
	}
	defer pool.Close()

	state := &State{
		sample:      pickSample(*tasks, *sampleSize),
		submittedAt: map[int]time.Time{},
		completedAt: map[int]time.Time{},
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &Worker{state: state})
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{"echo": {MaxWorkers: 4}},
		Workers: workers,
	})
	if err != nil {
		log.Fatalf("river client: %v", err)
	}
	// Create/upgrade River's schema. In v0.47 this is a separate migrator,
	// not a method on the client.
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		log.Fatalf("river migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		log.Fatalf("river schema migrate: %v", err)
	}
	if err := client.Start(ctx); err != nil {
		log.Fatalf("river start: %v", err)
	}

	fmt.Fprintf(os.Stderr, "river driver: %d tasks (submit conc %d, workload %s, latency sample %d)\n",
		*tasks, *conc, *workload, len(state.sample))

	// Submit all jobs, bounded by a semaphore.
	fmt.Fprintf(os.Stderr, "submitting %d tasks…\n", *tasks)
	wallStart := time.Now()
	submitStart := time.Now()
	sem := make(chan struct{}, *conc)
	var wg sync.WaitGroup
	for i := 0; i < *tasks; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			state.markSubmitted(i)
			_, _ = client.Insert(ctx, Args{I: i, Work: *workload, W: *w, H: *h}, &river.InsertOpts{Queue: "echo"})
		}(i)
	}
	wg.Wait()
	submitElapsed := time.Since(submitStart)
	fmt.Fprintf(os.Stderr, "  submitted %d tasks in %.2fs (%.0f submit/s)\n",
		*tasks, submitElapsed.Seconds(), float64(*tasks)/submitElapsed.Seconds())

	// Drain: poll the in-process completion counter.
	drainStart := time.Now()
	for {
		done := state.completed.Load()
		elapsed := time.Since(drainStart).Seconds()
		fmt.Fprintf(os.Stderr, "\r  completed=%7d/%d  elapsed=%6.1fs", done, *tasks, elapsed)
		if done >= int64(*tasks) {
			break
		}
		if *timeBudget > 0 && time.Since(wallStart).Seconds() >= *timeBudget {
			fmt.Fprintf(os.Stderr, "\n  ⚠ time budget (%.1fs) exceeded with %d pending\n", *timeBudget, int64(*tasks)-done)
			break
		}
		time.Sleep(time.Duration(*pollInterval * float64(time.Second)))
	}
	drainElapsed := time.Since(drainStart)
	fmt.Fprintln(os.Stderr)

	// Stop the worker cleanly (drains in-flight).
	stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_ = client.Stop(stopCtx)

	completed := state.completed.Load()
	failures := state.failures.Load()
	drained := completed

	state.mu.RLock()
	latenciesMs := []float64{}
	for i, t0 := range state.submittedAt {
		if t1, ok := state.completedAt[i]; ok {
			latenciesMs = append(latenciesMs, float64(t1.Sub(t0).Microseconds())/1000.0)
		}
	}
	state.mu.RUnlock()

	wallElapsed := submitElapsed + drainElapsed
	e2e := float64(drained) / wallElapsed.Seconds()

	fmt.Println("─" + repeat("─", 51))
	fmt.Printf("system:             river\n")
	fmt.Printf("tasks:              %d\n", *tasks)
	fmt.Printf("concurrency:        %d\n", *conc)
	fmt.Printf("submit time:        %.2fs\n", submitElapsed.Seconds())
	fmt.Printf("drain time:         %.2fs\n", drainElapsed.Seconds())
	fmt.Printf("e2e throughput:     %.0f tasks/s\n", e2e)
	if len(latenciesMs) > 0 {
		fmt.Printf("latency p50:        %.1f ms\n", pct(latenciesMs, 0.50))
		fmt.Printf("latency p95:        %.1f ms\n", pct(latenciesMs, 0.95))
		fmt.Printf("latency p99:        %.1f ms\n", pct(latenciesMs, 0.99))
		fmt.Printf("(end-to-end; from %d sampled tasks)\n", len(latenciesMs))
	} else {
		fmt.Println("latency p50/p95/p99: (no timing samples captured)")
	}
	fmt.Printf("failures:           %d\n", failures)
	fmt.Println("─" + repeat("─", 51))
	fmt.Printf("RESULT system=river tasks=%d conc=%d workload=%s throughput=%.0f submit_s=%.2f drain_s=%.2f p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f failures=%d\n",
		*tasks, *conc, *workload, e2e, submitElapsed.Seconds(), drainElapsed.Seconds(),
		pct(latenciesMs, 0.50), pct(latenciesMs, 0.95), pct(latenciesMs, 0.99), failures)
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

// guard against unused import during partial edits
var _ = errors.New
