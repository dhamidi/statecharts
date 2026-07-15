package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/sqllog/sqlite3"
)

func openLog(path string) (*sqlite3.Storage, error) {
	return sqlite3.Open(path)
}

type streamTransport struct {
	mu      sync.RWMutex
	outputs map[statecharts.Identifier]chan []byte
}

func newStreamTransport() *streamTransport {
	return &streamTransport{outputs: map[statecharts.Identifier]chan []byte{}}
}
func (t *streamTransport) Attach(statecharts.Dispatcher) {}
func (t *streamTransport) register(id statecharts.Identifier) <-chan []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan []byte, 32)
	t.outputs[id] = ch
	return ch
}
func (t *streamTransport) unregister(id statecharts.Identifier) {
	t.mu.Lock()
	delete(t.outputs, id)
	t.mu.Unlock()
}
func (t *streamTransport) Send(ctx context.Context, req statecharts.SendRequest) error {
	if req.Type != streamIOProcessor {
		return fmt.Errorf("unsupported UI transport type %q", req.Type)
	}
	t.mu.RLock()
	ch := t.outputs[req.Target]
	t.mu.RUnlock()
	if ch == nil {
		return fmt.Errorf("closed UI transport target %q", req.Target)
	}
	text, ok := req.Data.AsString()
	if !ok {
		return fmt.Errorf("invalid stream frame Value kind %q", req.Data.Kind())
	}
	frame := []byte(text)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- frame:
		return nil
	default:
		return fmt.Errorf("UI transport buffer full")
	}
}

type counterRuntime struct {
	counters, ui *actors.System
	streams      *streamTransport
	storage      *sqlite3.Storage
	requests     *hubRequestRegistry
}

type hubRequest struct {
	colors []string
	reply  chan []projection
}
type hubRequestRegistry struct {
	mu      sync.Mutex
	next    uint64
	pending map[string]hubRequest
}

func newHubRequestRegistry() *hubRequestRegistry {
	return &hubRequestRegistry{pending: map[string]hubRequest{}}
}
func (r *hubRequestRegistry) add(q hubRequest) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	id := strconv.FormatUint(r.next, 10)
	r.pending[id] = q
	return id
}
func (r *hubRequestRegistry) take(id string) (hubRequest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q, ok := r.pending[id]
	delete(r.pending, id)
	return q, ok
}
func (r *hubRequestRegistry) remove(id string) { r.mu.Lock(); delete(r.pending, id); r.mu.Unlock() }

func setupCounters(ctx context.Context, store *sqlite3.Storage) (*counterRuntime, error) {
	requests := newHubRequestRegistry()
	hubChart, err := buildHubChart(requests)
	if err != nil {
		return nil, err
	}
	streamChart, err := buildStreamChart()
	if err != nil {
		return nil, err
	}
	transport := newStreamTransport()
	ui := actors.NewSystem(actors.WithNodeName("ui"), actors.WithIOProcessor("sse", func() statecharts.IOProcessor { return transport }))
	cleanup := func() { _ = ui.Stop(context.Background()) }
	if err = ui.Register(hubChart); err != nil {
		cleanup()
		return nil, err
	}
	if err = ui.Register(streamChart); err != nil {
		cleanup()
		return nil, err
	}
	if err = ui.Spawn(ctx, "hub", hubKind); err != nil {
		cleanup()
		return nil, err
	}
	bridge := actors.NewBridge("ui", nil, "counters")
	chart, err := buildCounterChart()
	if err != nil {
		cleanup()
		return nil, err
	}
	counters := actors.NewSystem(actors.WithNodeName("counters"), actors.WithStorage(store), actors.WithMaxResident(3), actors.WithIdleTimeout(time.Minute), actors.WithSCXMLPeer(bridge), actors.WithResidencyObserver(func(change actors.ResidencyChange) {
		_ = ui.Tell(context.Background(), "hub", statecharts.Event{Name: "residency", Type: statecharts.EventExternal, Data: taggedMap(residencyValueTag, map[string]statecharts.Value{"actor_id": stringValue(string(change.ActorID)), "state": stringValue(string(change.State))})})
	}))
	fail := func(e error) (*counterRuntime, error) {
		_ = counters.Stop(context.Background())
		cleanup()
		return nil, e
	}
	bridge.SetTarget(ui)
	if err = counters.Register(chart); err != nil {
		return fail(err)
	}
	for _, name := range colors {
		if err = counters.Spawn(ctx, statecharts.Identifier(name), counterKind, actors.Durable()); err != nil {
			return fail(err)
		}
	}
	for _, name := range colors {
		if err = counters.Tell(ctx, statecharts.Identifier(name), statecharts.Event{Name: "publish", Type: statecharts.EventExternal}); err != nil {
			return fail(err)
		}
	}
	rt := &counterRuntime{counters: counters, ui: ui, streams: transport, storage: store, requests: requests}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ps, e := rt.query(ctx, colors)
		if e == nil && len(ps) == len(colors) {
			break
		}
		if time.Now().After(deadline) {
			return fail(fmt.Errorf("timed out rebuilding projection"))
		}
		time.Sleep(time.Millisecond)
	}
	return rt, nil
}

func (rt *counterRuntime) query(ctx context.Context, selected []string) ([]projection, error) {
	reply := make(chan []projection, 1)
	id := rt.requests.add(hubRequest{colors: append([]string(nil), selected...), reply: reply})
	if err := rt.ui.Tell(ctx, "hub", statecharts.Event{Name: "query", Type: statecharts.EventExternal, Data: taggedMap(hubQueryValueTag, map[string]statecharts.Value{"request_id": stringValue(id)})}); err != nil {
		rt.requests.remove(id)
		return nil, err
	}
	select {
	case p := <-reply:
		return p, nil
	case <-ctx.Done():
		rt.requests.remove(id)
		return nil, ctx.Err()
	}
}
func (rt *counterRuntime) stop(ctx context.Context) error {
	countersErr := rt.counters.Stop(ctx)
	uiErr := rt.ui.Stop(ctx)
	closeErr := rt.storage.Close()
	return errors.Join(countersErr, uiErr, closeErr)
}

func counterHandler(rt *counterRuntime) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", pageHandler(func() []projection { p, _ := rt.query(context.Background(), colors); return p }))
	mux.HandleFunc("GET /datastar.js", datastarHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /ui/events", rt.streamHandler("browser", func(*http.Request) ([]string, error) { return append([]string(nil), colors...), nil }))
	mux.HandleFunc("GET /events", rt.streamHandler("terminal", eventStreamColors))
	mux.HandleFunc("POST /counters/{color}/writes/{writeID}", func(w http.ResponseWriter, r *http.Request) {
		color, id := r.PathValue("color"), r.PathValue("writeID")
		if _, ok := colorValues[color]; !ok {
			http.Error(w, "unknown color", 404)
			return
		}
		if _, err := statecharts.NewIdentifier(id); err != nil || strings.Contains(id, ".") {
			http.Error(w, "invalid write ID", 400)
			return
		}
		if err := rt.counters.Tell(r.Context(), statecharts.Identifier(color), incrementEvent(statecharts.Identifier(id))); err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /counters/{color}/increment", func(w http.ResponseWriter, r *http.Request) {
		color := r.PathValue("color")
		if _, ok := colorValues[color]; !ok {
			http.Error(w, "unknown color", 404)
			return
		}
		datastar := strings.EqualFold(r.Header.Get("Datastar-Request"), "true")
		before := -1
		if datastar {
			if p, queryErr := rt.query(r.Context(), []string{color}); queryErr == nil && len(p) == 1 {
				before = p[0].Value
			}
		}
		id, err := randomWriteID("ui")
		if err == nil {
			err = rt.counters.Tell(r.Context(), statecharts.Identifier(color), incrementEvent(id))
		}
		if err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		if datastar {
			var p []projection
			deadline := time.Now().Add(5 * time.Second)
			for {
				p, _ = rt.query(r.Context(), colors)
				for _, item := range p {
					if item.Name == color && item.Value > before {
						goto ready
					}
				}
				if time.Now().After(deadline) {
					http.Error(w, "timed out waiting for projection", http.StatusGatewayTimeout)
					return
				}
				time.Sleep(time.Millisecond)
			}
		ready:
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_ = renderDashboard("online", p).WriteHTML(w)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (rt *counterRuntime) streamHandler(mode string, choose func(*http.Request) ([]string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		selected, err := choose(r)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", 500)
			return
		}
		idraw, err := randomWriteID("out")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		actorID := statecharts.Identifier("sse-" + string(idraw))
		output := idraw
		frames := rt.streams.register(output)
		defer rt.streams.unregister(output)
		if err = rt.ui.Spawn(r.Context(), actorID, streamKind); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if err = rt.ui.Tell(r.Context(), actorID, statecharts.Event{Name: "start", Type: statecharts.EventExternal, Data: taggedMap(streamStartValueTag, map[string]statecharts.Value{"mode": stringValue(mode), "colors": encodeStrings(selected), "output": stringValue(string(output))})}); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rt.ui.Tell(context.Background(), actorID, statecharts.Event{Name: "close", Type: statecharts.EventExternal})
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		for {
			select {
			case <-r.Context().Done():
				return
			case frame := <-frames:
				if _, err = w.Write(frame); err != nil {
					return
				}
				f.Flush()
			}
		}
	}
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
