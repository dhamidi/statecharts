package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: counters serve|writer|reader")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(ctx, os.Args[2:])
	case "writer":
		err = runWriter(ctx, os.Args[2:])
	case "reader":
		err = runReader(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func runServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "listen address")
	dbPath := fs.String("db", "data/counters.db", "SQLite path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := openLog(*dbPath)
	if err != nil {
		return err
	}
	runtime, err := setupCounters(ctx, store)
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.stop(stopCtx)
	}()
	server := &http.Server{
		Addr:        *addr,
		Handler:     counterHandler(runtime),
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	log.Printf("serving counters on %s", ln.Addr())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func runWriter(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("writer", flag.ContinueOnError)
	base := fs.String("server", "http://127.0.0.1:8080", "server URL")
	rate := fs.Float64("rate", 25, "mean writes per second")
	maxInFlight := fs.Int("max-in-flight", 128, "maximum writes awaiting acknowledgement")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rate <= 0 {
		return fmt.Errorf("rate must be positive")
	}
	if *maxInFlight < 1 {
		return fmt.Errorf("max-in-flight must be positive")
	}
	selected, err := selectColors(fs.Args(), len(colors))
	if err != nil {
		return err
	}
	serverURL, err := normalizeServerURL(*base)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	connection, err := newConnectionActor(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = connection.stop(stopCtx)
	}()
	stats := newWriterStats(selected)
	terminalDone := make(chan struct{})
	go func() {
		runWriterTerminal(ctx, connection, selected, *rate, stats)
		close(terminalDone)
	}()
	defer func() { <-terminalDone }()
	var nextColor func() string
	if len(selected) == 1 {
		nextColor = func() string { return selected[0] }
	} else {
		zipf := rand.NewZipf(rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())), 1.3, 1, uint64(len(selected)-1))
		nextColor = func() string { return selected[zipf.Uint64()] }
	}
	slots := make(chan struct{}, *maxInFlight)
	var writes sync.WaitGroup
	defer writes.Wait()
	seq := uint64(0)
	for {
		delay := time.Duration(rand.ExpFloat64() / (*rate) * float64(time.Second))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		select {
		case <-ctx.Done():
			return nil
		case slots <- struct{}{}:
		}
		seq++
		id := strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(seq, 36)
		color := nextColor()
		stats.generated(color)
		writes.Add(1)
		go func() {
			defer writes.Done()
			defer func() { <-slots }()
			defer stats.completed(color)
			retryWrite(ctx, client, serverURL, color, id, connection, stats)
		}()
	}
}

func retryWrite(ctx context.Context, client *http.Client, base, color, id string, connection *connectionActor, stats *writerStats) {
	backoff := 100 * time.Millisecond
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/counters/"+url.PathEscape(color)+"/writes/"+url.PathEscape(id), nil)
		if err != nil {
			return
		}
		stats.attempted(color)
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				stats.succeeded(color)
				connection.outcome(ctx, true)
				return
			}
		}
		stats.retried(color)
		connection.outcome(ctx, false)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func normalizeServerURL(raw string) (string, error) {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid server URL %q", raw)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("server URL must use http or https")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("server URL must not contain a query or fragment")
	}
	return strings.TrimRight(raw, "/"), nil
}
