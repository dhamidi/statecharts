// Package inspectorhttp adapts an inspector.Service to a versioned HTTP API.
// It does not listen on a network address and leaves authentication to host
// middleware; the request context is passed unchanged to the service.
package inspectorhttp

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"strconv"
	"strings"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/inspector"
	definitionjson "github.com/dhamidi/statecharts/syntax/json"
)

// uiFiles contains the complete browser application. It deliberately has no
// runtime asset or module dependencies.
//
//go:embed ui/*
var uiFiles embed.FS

const (
	maxBodyBytes      = 64 << 10
	streamBuffer      = 64
	recentLimit       = 1000
	uiBasePlaceholder = "__STATECHARTS_INSPECTOR_BASE__"
)

type handler struct{ service *inspector.Service }

// NewHandler returns the transport adapter for service. The returned handler
// is safe to mount below any prefix with http.StripPrefix.
func NewHandler(service *inspector.Service) http.Handler { return &handler{service: service} }

type envelope struct {
	Data  any        `json:"data,omitempty"`
	Error *wireError `json:"error,omitempty"`
}

type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "", "/":
		h.asset(w, r, "ui/index.html", "text/html; charset=utf-8", "no-cache")
	case "/assets/v2/app.js":
		h.asset(w, r, "ui/app.js", "text/javascript; charset=utf-8", "no-cache")
	case "/assets/v2/app.css":
		h.asset(w, r, "ui/app.css", "text/css; charset=utf-8", "no-cache")
	case "/v1/systems":
		h.systems(w, r)
	case "/v1/actors":
		h.actors(w, r)
	case "/v1/actor":
		h.actor(w, r)
	case "/v1/definition":
		h.definition(w, r)
	case "/v1/history":
		h.history(w, r)
	case "/v1/recent":
		h.recent(w, r)
	case "/v1/stream":
		h.stream(w, r)
	case "/v1/events":
		h.send(w, r)
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

func (h *handler) asset(w http.ResponseWriter, r *http.Request, name, contentType, cache string) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	data, err := uiFiles.ReadFile(name)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "asset not found")
		return
	}
	if name == "ui/index.html" {
		data = []byte(strings.ReplaceAll(string(data), uiBasePlaceholder, html.EscapeString(requestBase(r))))
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", cache)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if name == "ui/index.html" {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'self'; form-action 'self'; object-src 'none'; frame-ancestors 'self'")
		w.Header().Set("Referrer-Policy", "no-referrer")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func requestBase(r *http.Request) string {
	base := r.RequestURI
	if query := strings.IndexByte(base, '?'); query >= 0 {
		base = base[:query]
	}
	if base == "" || !strings.HasPrefix(base, "/") || strings.HasPrefix(base, "//") {
		base = r.URL.EscapedPath()
	}
	if base == "" {
		base = "/"
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return base
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	return false
}

func (h *handler) systems(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if h.service == nil {
		writeServiceError(w, inspector.ErrServiceClosed)
		return
	}
	names, err := h.service.Systems(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Data: struct {
		Systems []string `json:"systems"`
	}{names}})
}

func (h *handler) actors(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	q := r.URL.Query()
	limit, err := integer(q.Get("limit"), 0)
	if err != nil {
		writeInvalid(w, "invalid limit")
		return
	}
	var durable *bool
	if raw := q.Get("durable"); raw != "" {
		value, e := strconv.ParseBool(raw)
		if e != nil {
			writeInvalid(w, "invalid durable filter")
			return
		}
		durable = &value
	}
	kind := q.Get("kind")
	lifecycle := statecharts.ActorLifecycle(q.Get("lifecycle"))
	residency := actors.ResidencyState(q.Get("residency"))
	if (kind != "" && invalidIdentifier(kind)) ||
		(lifecycle != "" && lifecycle != statecharts.ActorLifecycleActive && lifecycle != statecharts.ActorLifecycleTerminal) ||
		(residency != "" && residency != actors.ResidencyPagedOut && residency != actors.ResidencyHydrating && residency != actors.ResidencyResident) {
		writeInvalid(w, "invalid actor filter")
		return
	}
	page, err := h.callQueryActors(r, q.Get("system"), actors.ActorQuery{
		After: statecharts.ActorMetadataCursor(q.Get("after")), Limit: limit, IDPrefix: q.Get("prefix"),
		Kind: statecharts.Identifier(kind), Revision: statecharts.RevisionID(q.Get("revision")),
		Durable: durable, Lifecycle: lifecycle, Residency: residency,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Data: page})
}

func invalidIdentifier(value string) bool {
	_, err := statecharts.NewIdentifier(value)
	return err != nil
}

func (h *handler) callQueryActors(r *http.Request, system string, query actors.ActorQuery) (actors.ActorPage, error) {
	if h.service == nil {
		return actors.ActorPage{}, inspector.ErrServiceClosed
	}
	return h.service.QueryActors(r.Context(), system, query)
}

func target(r *http.Request) (string, actors.ActorID, error) {
	system, id := r.URL.Query().Get("system"), actors.ActorID(r.URL.Query().Get("id"))
	if system == "" || id == "" {
		return "", "", errors.New("system and id are required")
	}
	return system, id, nil
}

func (h *handler) actor(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	system, id, err := target(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	if h.service == nil {
		writeServiceError(w, inspector.ErrServiceClosed)
		return
	}
	info, live, err := h.service.InspectActor(r.Context(), system, id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Data: struct {
		Info actors.ActorInfo                `json:"info"`
		Live *statecharts.InstanceInspection `json:"live"`
	}{info, live}})
}

func (h *handler) definition(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	system, id, err := target(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	if h.service == nil {
		writeServiceError(w, inspector.ErrServiceClosed)
		return
	}
	result, err := h.service.InspectActorDefinition(r.Context(), system, id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	pinnedSource, err := definitionjson.MarshalIndent(result.Pinned, "", "  ")
	if err != nil {
		writeServiceError(w, fmt.Errorf("inspector HTTP: encode pinned definition: %w", err))
		return
	}
	response := struct {
		actors.ActorDefinition
		PinnedSource  json.RawMessage `json:"pinnedSource"`
		CurrentSource json.RawMessage `json:"currentSource,omitempty"`
	}{ActorDefinition: result, PinnedSource: pinnedSource}
	if result.CurrentAvailable {
		response.CurrentSource, err = definitionjson.MarshalIndent(result.Current, "", "  ")
		if err != nil {
			writeServiceError(w, fmt.Errorf("inspector HTTP: encode current definition: %w", err))
			return
		}
	}
	writeJSON(w, http.StatusOK, envelope{Data: response})
}

func (h *handler) history(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	system, id, err := target(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	after, err := unsigned(r.URL.Query().Get("after"), 0)
	if err != nil {
		writeInvalid(w, "invalid after cursor")
		return
	}
	limit, err := integer(r.URL.Query().Get("limit"), 0)
	if err != nil {
		writeInvalid(w, "invalid limit")
		return
	}
	tail, err := boolean(r.URL.Query().Get("tail"), false)
	if err != nil {
		writeInvalid(w, "invalid tail")
		return
	}
	if h.service == nil {
		writeServiceError(w, inspector.ErrServiceClosed)
		return
	}
	page, err := h.service.QueryActorHistory(r.Context(), system, id, actors.ActorHistoryQuery{After: after, Limit: limit, Tail: tail})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Data: page})
}

func (h *handler) recent(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	cursor, err := unsigned(r.URL.Query().Get("cursor"), 0)
	if err != nil {
		writeInvalid(w, "invalid cursor")
		return
	}
	limit, err := integer(r.URL.Query().Get("limit"), 0)
	if err != nil {
		writeInvalid(w, "invalid limit")
		return
	}
	if h.service == nil {
		writeServiceError(w, inspector.ErrServiceClosed)
		return
	}
	page, err := h.service.Recent(r.Context(), r.URL.Query().Get("system"), cursor, limit)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Data: page})
}

type command struct {
	Name statecharts.Identifier `json:"name"`
	Data statecharts.Value      `json:"data"`
}

func (h *handler) send(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	system, id, err := target(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}
	var cmd command
	if err := decodeStrict(w, r, &cmd); err != nil {
		writeInvalid(w, "invalid command body")
		return
	}
	if _, err := statecharts.NewIdentifier(string(id)); err != nil {
		writeInvalid(w, "invalid actor id")
		return
	}
	if _, err := statecharts.NewIdentifier(string(cmd.Name)); err != nil {
		writeInvalid(w, "invalid event name")
		return
	}
	if _, err := cmd.Data.Wire(); err != nil {
		writeInvalid(w, "invalid event data")
		return
	}
	if h.service == nil {
		writeServiceError(w, inspector.ErrServiceClosed)
		return
	}
	if err := h.service.SendEvent(r.Context(), system, id, cmd.Name, cmd.Data); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, envelope{Data: struct {
		Accepted bool `json:"accepted"`
	}{true}})
}

func decodeStrict(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func integer(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	return strconv.Atoi(raw)
}
func unsigned(raw string, fallback uint64) (uint64, error) {
	if raw == "" {
		return fallback, nil
	}
	return strconv.ParseUint(raw, 10, 64)
}
func boolean(raw string, fallback bool) (bool, error) {
	if raw == "" {
		return fallback, nil
	}
	return strconv.ParseBool(raw)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeInvalid(w http.ResponseWriter, message string) {
	writeError(w, http.StatusBadRequest, "invalid_request", message)
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, envelope{Error: &wireError{code, message}})
}

func writeServiceError(w http.ResponseWriter, err error) {
	status, code, message := http.StatusInternalServerError, "service_error", "inspector service failed"
	switch {
	case errors.Is(err, inspector.ErrUnauthorized):
		status, code, message = http.StatusForbidden, "unauthorized", "request is not authorized"
	case errors.Is(err, inspector.ErrUnknownSystem):
		status, code, message = http.StatusNotFound, "unknown_system", "system not found"
	case errors.Is(err, actors.ErrUnknownActor):
		status, code, message = http.StatusNotFound, "unknown_actor", "actor not found"
	case errors.Is(err, actors.ErrHistoryUnavailable):
		status, code, message = http.StatusConflict, "history_unavailable", "durable history is unavailable"
	case errors.Is(err, inspector.ErrRedaction):
		status, code, message = http.StatusInternalServerError, "redaction_failed", "response redaction failed"
	case errors.Is(err, inspector.ErrServiceClosed), errors.Is(err, inspector.ErrStreamClosed):
		status, code, message = http.StatusServiceUnavailable, "service_unavailable", "inspector service is unavailable"
	default:
		// Validation failures originate below the stable service boundary. Do not
		// expose their details, but classify known input errors as client errors.
		if strings.Contains(err.Error(), "limit must be") || errors.Is(err, actors.ErrInvalidHistoryQuery) || strings.Contains(err.Error(), "invalid identifier") || strings.Contains(err.Error(), "cursor") {
			status, code, message = http.StatusBadRequest, "invalid_request", "request input is invalid"
		}
	}
	writeError(w, status, code, message)
}

func (h *handler) stream(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if h.service == nil {
		writeServiceError(w, inspector.ErrServiceClosed)
		return
	}
	system := r.URL.Query().Get("system")
	cursorText := r.Header.Get("Last-Event-ID")
	if cursorText == "" {
		cursorText = r.URL.Query().Get("cursor")
	}
	cursor, err := unsigned(cursorText, 0)
	if err != nil {
		writeInvalid(w, "invalid cursor")
		return
	}
	sub, err := h.service.Subscribe(r.Context(), system, streamBuffer)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	defer sub.Close()
	initial, err := h.service.Recent(r.Context(), system, cursor, recentLimit)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	flush := func() bool { return controller.Flush() == nil }
	if !flush() {
		return
	}
	last := cursor
	emit := func(record inspector.StreamRecord) bool {
		if record.Sequence == 0 && record.Kind == inspector.StreamGap {
			payload, e := json.Marshal(record)
			if e != nil {
				return false
			}
			if _, e = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", record.Kind, payload); e != nil {
				return false
			}
			return flush()
		}
		if record.Sequence <= last {
			return true
		}
		payload, e := json.Marshal(record)
		if e != nil {
			return false
		}
		if _, e = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", record.Sequence, record.Kind, payload); e != nil {
			return false
		}
		last = record.Sequence
		return flush()
	}
	catchup := func(page *inspector.RingPage) bool {
		boundary := uint64(0)
		first := true
		for {
			if page == nil {
				fetched, e := h.service.Recent(r.Context(), system, last, recentLimit)
				if e != nil {
					return false
				}
				page = &fetched
			}
			if first {
				boundary = page.Latest
				first = false
				if page.Expired && last > page.Latest {
					last = 0
				}
			}
			before := last
			for _, record := range page.Records {
				if !emit(record) {
					return false
				}
			}
			if last >= boundary {
				return true
			}
			if last == before {
				return false
			}
			page = nil
		}
	}
	if !catchup(&initial) {
		return
	}
	if !flush() {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case record, ok := <-sub.C:
			if !ok {
				payload, _ := json.Marshal(envelope{Error: &wireError{"stream_closed", "inspector stream closed"}})
				_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", payload)
				_ = flush()
				return
			}
			if record.Sequence <= last {
				continue
			}
			if record.Dropped > 0 {
				if !catchup(nil) {
					return
				}
				continue
			}
			if !emit(record) {
				return
			}
		}
	}
}
