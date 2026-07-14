package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/sqllog"
	_ "modernc.org/sqlite"
)

func openLog(path string) (*sqllog.Log, *sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	l, err := sqllog.New(db, sqllog.SQLite)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return l, db, nil
}

func setupCounters(ctx context.Context, store *sqllog.Log, hub *projectionHub) (*actors.System, error) {
	registerCounterDataTypes()
	chart, err := buildCounterChart()
	if err != nil {
		return nil, err
	}
	// Snapshot currently excludes an application's Go datamodel. Retain the
	// configuration checkpoint but replay the complete short counter log so
	// Value and Processed are always reconstructed as well.
	snapshots := fullReplaySnapshots{SnapshotStore: store}
	sys := actors.NewSystem(actors.WithLog(store), actors.WithSnapshotStore(snapshots), actors.WithMaxResident(3), actors.WithIdleTimeout(time.Minute), actors.WithFallback(hub))
	ready := false
	defer func() {
		if !ready {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = sys.Stop(stopCtx)
		}
	}()
	if err := sys.Register(chart); err != nil {
		return nil, err
	}
	for _, name := range colors {
		if err := sys.Spawn(ctx, statecharts.Identifier(name), counterKind, actors.Durable()); err != nil {
			return nil, err
		}
	}
	for _, name := range colors {
		if err := sys.Tell(ctx, statecharts.Identifier(name), statecharts.Event{Name: "publish", Type: statecharts.EventExternal}); err != nil {
			return nil, err
		}
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for len(hub.snapshot(7)) < 7 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("timed out rebuilding projection")
		case <-time.After(time.Millisecond):
		}
	}
	ready = true
	return sys, nil
}

type fullReplaySnapshots struct{ statecharts.SnapshotStore }

func (s fullReplaySnapshots) Save(ctx context.Context, id statecharts.SessionID, cp statecharts.Checkpoint) error {
	cp.Seq = 0
	return s.SnapshotStore.Save(ctx, id, cp)
}

func counterHandler(sys *actors.System, hub *projectionHub) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", pageHandler)
	mux.HandleFunc("GET /datastar.js", datastarHandler)
	mux.HandleFunc("GET /ui/events", serverBrowserEvents(sys, hub))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /counters/{color}/writes/{writeID}", func(w http.ResponseWriter, r *http.Request) {
		color, writeID := r.PathValue("color"), r.PathValue("writeID")
		if _, ok := colorValues[color]; !ok {
			http.Error(w, "unknown color", 404)
			return
		}
		if _, err := statecharts.NewIdentifier(writeID); err != nil || strings.Contains(writeID, ".") {
			http.Error(w, "invalid write ID", 400)
			return
		}
		if err := tellIncrement(r.Context(), sys, hub, color, statecharts.Identifier(writeID)); err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /counters/{color}/increment", func(w http.ResponseWriter, r *http.Request) {
		color := r.PathValue("color")
		if _, ok := colorValues[color]; !ok {
			http.Error(w, "unknown color", http.StatusNotFound)
			return
		}
		writeID, err := randomWriteID("ui")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := tellIncrement(r.Context(), sys, hub, color, writeID); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		selected, err := eventStreamColors(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", 500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		changes, unsubscribe := hub.subscribe()
		defer unsubscribe()
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		var last []byte
		write := func() bool {
			b, _ := json.Marshal(residentSnapshot(sys, hub.snapshotColors(selected)))
			if bytes.Equal(b, last) {
				return true
			}
			_, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", b)
			f.Flush()
			last = b
			return err == nil
		}
		if !write() {
			return
		}
		for {
			select {
			case <-r.Context().Done():
				return
			case <-changes:
				if !write() {
					return
				}
			case <-tick.C:
				fmt.Fprint(w, ": keepalive\n\n")
				f.Flush()
			}
		}
	})
	return mux
}

func tellIncrement(ctx context.Context, sys *actors.System, hub *projectionHub, color string, writeID statecharts.Identifier) error {
	if err := sys.Tell(ctx, statecharts.Identifier(color), incrementEvent(writeID)); err != nil {
		return err
	}
	// The actor's projection already announces its value. This second,
	// coalesced notification ensures subscribers also observe any different
	// actor evicted while the target was paged in.
	hub.changed()
	return nil
}

func randomWriteID(prefix string) (statecharts.Identifier, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate write ID: %w", err)
	}
	return statecharts.Identifier(prefix + "-" + hex.EncodeToString(value[:])), nil
}

func eventStreamColors(r *http.Request) ([]string, error) {
	if raw := r.URL.Query().Get("colors"); raw != "" {
		return selectColors(strings.Split(raw, ","), len(colors))
	}
	n, err := strconv.Atoi(r.URL.Query().Get("n"))
	if err != nil {
		return nil, fmt.Errorf("provide colors or n=1..%d", len(colors))
	}
	return selectColors(nil, n)
}

func residentSnapshot(sys *actors.System, ps []projection) []projection {
	for i := range ps {
		ps[i].Resident = sys.IsResident(statecharts.Identifier(ps[i].Name))
	}
	return ps
}
