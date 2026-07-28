package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func handler(r *runtime, inspect http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200); w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /", func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/" {
			http.NotFound(w, q)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(indexHTML))
	})
	mux.HandleFunc("GET /app.js", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.Write([]byte(appJS))
	})
	mux.HandleFunc("GET /styles.css", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Write([]byte(stylesCSS))
	})
	mux.HandleFunc("GET /api/plans", func(w http.ResponseWriter, _ *http.Request) {
		names := []string{"starter", "growth", "scale"}
		catalog := make([]map[string]any, 0, len(names))
		for _, name := range names {
			catalog = append(catalog, map[string]any{"name": name, "unit_amount": plans[name], "currency": "USD"})
		}
		writeJSON(w, map[string]any{"plans": catalog})
	})
	mux.HandleFunc("GET /api/subscriptions", func(w http.ResponseWriter, q *http.Request) { writeJSON(w, map[string]any{"subscriptions": r.list()}) })
	mux.HandleFunc("POST /api/subscriptions", func(w http.ResponseWriter, q *http.Request) {
		var x struct {
			ID, Plan, Scenario string
			Quantity           int64
		}
		if json.NewDecoder(http.MaxBytesReader(w, q.Body, 4096)).Decode(&x) != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		if err := r.create(q.Context(), x.ID, x.Plan, x.Scenario, x.Quantity); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errConflict) {
				status = http.StatusConflict
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(201)
		p, _ := r.projection(q.Context(), x.ID)
		writeJSON(w, p)
	})
	mux.HandleFunc("GET /api/subscriptions/{id}", func(w http.ResponseWriter, q *http.Request) {
		p, e := r.projection(q.Context(), q.PathValue("id"))
		if e != nil {
			http.Error(w, e.Error(), 404)
			return
		}
		writeJSON(w, p)
	})
	command := func(event string, payload func(*http.Request) (any, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, q *http.Request) {
			v, e := payload(q)
			if e == nil {
				e = r.tell(q.Context(), q.PathValue("id"), event, v)
			}
			if e != nil {
				status := http.StatusBadRequest
				if errors.Is(e, errUnknown) {
					status = http.StatusNotFound
				}
				http.Error(w, e.Error(), status)
				return
			}
			writeJSON(w, map[string]any{"accepted": true})
		}
	}
	empty := func(*http.Request) (any, error) { return map[string]any{}, nil }
	mux.HandleFunc("POST /api/subscriptions/{id}/cancel", command("cancel", empty))
	mux.HandleFunc("POST /api/subscriptions/{id}/retry", command("retry", empty))
	mux.HandleFunc("POST /api/subscriptions/{id}/advance", command("period.advance", func(q *http.Request) (any, error) {
		p, e := r.projection(q.Context(), q.PathValue("id"))
		if e != nil {
			return nil, e
		}
		n, _ := strconv.ParseInt(fmt.Sprint(p["period"]), 10, 64)
		return map[string]any{"period": n + 1}, nil
	}))
	mux.HandleFunc("POST /api/subscriptions/{id}/scenario", command("scenario.set", func(q *http.Request) (any, error) {
		var x struct{ Scenario string }
		if json.NewDecoder(q.Body).Decode(&x) != nil || !scenarios[x.Scenario] {
			return nil, fmt.Errorf("invalid scenario")
		}
		return map[string]any{"scenario": x.Scenario}, nil
	}))
	mux.HandleFunc("GET /api/subscriptions/{id}/events", func(w http.ResponseWriter, q *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		c := make(chan struct{}, 1)
		id := q.PathValue("id")
		r.mu.Lock()
		if r.watchers[id] == nil {
			r.watchers[id] = map[chan struct{}]struct{}{}
		}
		r.watchers[id][c] = struct{}{}
		r.mu.Unlock()
		defer func() { r.mu.Lock(); delete(r.watchers[id], c); r.mu.Unlock() }()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			p, e := r.projection(q.Context(), id)
			if e != nil {
				return
			}
			b, _ := json.Marshal(p)
			fmt.Fprintf(w, "event: state\ndata: %s\n\n", b)
			f.Flush()
			select {
			case <-q.Context().Done():
				return
			case <-c:
			case <-tick.C:
			}
		}
	})
	if inspect != nil {
		mounted := http.StripPrefix("/inspect", inspect)
		mux.Handle("GET /inspect", mounted)
		mux.Handle("POST /inspect", mounted)
		mux.Handle("GET /inspect/", mounted)
		mux.Handle("POST /inspect/", mounted)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'self'; form-action 'self'")
		w.Header().Set("Referrer-Policy", "same-origin")
		if strings.Contains(q.URL.Path, "//") {
			http.NotFound(w, q)
			return
		}
		mux.ServeHTTP(w, q)
	})
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
