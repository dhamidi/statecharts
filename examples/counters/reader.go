package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type readerState struct {
	mu       sync.RWMutex
	counters []projection
}

func newReaderState() *readerState { return &readerState{} }

func (s *readerState) set(p []projection) {
	s.mu.Lock()
	s.counters = p
	s.mu.Unlock()
}

func (s *readerState) snapshot() []projection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]projection(nil), s.counters...)
}

func runReader(ctx context.Context, args []string) error {
	fs := flagSet("reader")
	base := fs.String("server", "http://127.0.0.1:8080", "server URL")
	n := fs.Int("n", 7, "number of counters when no colors are named (1..7)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	selected, err := selectColors(fs.Args(), *n)
	if err != nil {
		return err
	}
	serverURL, err := normalizeServerURL(*base)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	state := newReaderState()
	connection, err := newConnectionActor(ctx, nil)
	if err != nil {
		cancel()
		return err
	}
	defer func() {
		cancel()
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = connection.stop(stopCtx)
	}()
	go followSnapshots(ctx, serverURL, selected, state, connection)
	runReaderTerminal(ctx, connection, selected, state)
	return nil
}

// flagSet is isolated to keep reader tests from touching command-line globals.
func flagSet(name string) *flag.FlagSet { return flag.NewFlagSet(name, flag.ContinueOnError) }

func followSnapshots(ctx context.Context, base string, selected []string, state *readerState, connection *connectionActor) {
	client := &http.Client{Timeout: 0}
	followSnapshotsWithClient(ctx, client, base, selected, state, connection)
}

func followSnapshotsWithClient(ctx context.Context, client *http.Client, base string, selected []string, state *readerState, connection *connectionActor) {
	const initialBackoff = 200 * time.Millisecond
	backoff := initialBackoff
	endpoint := base + "/events?colors=" + url.QueryEscape(strings.Join(selected, ","))
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		resp, err := client.Do(req)
		gotSnapshot := false
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = consumeSSE(resp.Body, func(p []projection) {
				gotSnapshot = true
				connection.outcome(ctx, true)
				state.set(p)
			})
			resp.Body.Close()
		} else if resp != nil {
			resp.Body.Close()
		}
		if ctx.Err() != nil {
			return
		}
		connection.outcome(ctx, false)
		if gotSnapshot {
			backoff = initialBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func consumeSSE(r io.Reader, onSnapshot func([]projection)) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(line, "data: ") {
			var p []projection
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &p); err == nil {
				onSnapshot(p)
			}
		}
	}
	return s.Err()
}
