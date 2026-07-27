package inspectorhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/inspector"
)

type identityKey struct{}

func testHandler(t *testing.T, received chan<- statecharts.Event, options ...inspector.Option) (http.Handler, *actors.System, *inspector.Service) {
	t.Helper()
	b := statecharts.New("http-test", func() *struct{} { return &struct{}{} })
	record := b.Action("record", func(_ *struct{}, ec statecharts.ExecContext, _ []statecharts.Value) error {
		if received != nil {
			event, _ := ec.Event()
			received <- event
		}
		return nil
	})
	chart, err := b.Build(statecharts.Atomic("active", statecharts.On("message", statecharts.Then(record.Do()))))
	if err != nil {
		t.Fatal(err)
	}
	system := actors.NewSystem()
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	if err := system.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := system.Spawn(t.Context(), "actor", chart.ID()); err != nil {
		t.Fatal(err)
	}
	if len(options) == 0 {
		options = []inspector.Option{inspector.WithAuthorizer(inspector.AllowAll())}
	}
	service := inspector.New(options...)
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("test", system); err != nil {
		t.Fatal(err)
	}
	return NewHandler(service), system, service
}

func request(t *testing.T, h http.Handler, method, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestEmbeddedUIAssetsAndStripPrefix(t *testing.T) {
	h, _, _ := testHandler(t, nil)
	for _, tc := range []struct{ path, contentType, cache string }{
		{"/", "text/html", "no-cache"},
		{"/assets/v1/app.js", "text/javascript", "immutable"},
		{"/assets/v1/app.css", "text/css", "immutable"},
	} {
		w := request(t, h, http.MethodGet, tc.path, nil)
		if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), tc.contentType) || !strings.Contains(w.Header().Get("Cache-Control"), tc.cache) || w.Body.Len() == 0 {
			t.Errorf("GET %s = %d type=%q cache=%q bytes=%d", tc.path, w.Code, w.Header().Get("Content-Type"), w.Header().Get("Cache-Control"), w.Body.Len())
		}
	}
	shell := request(t, h, http.MethodGet, "/", nil).Body.String()
	shellHeaders := request(t, h, http.MethodGet, "/", nil).Header()
	if !strings.Contains(shellHeaders.Get("Content-Security-Policy"), "connect-src 'self'") || shellHeaders.Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("shell security headers: CSP=%q Referrer-Policy=%q", shellHeaders.Get("Content-Security-Policy"), shellHeaders.Get("Referrer-Policy"))
	}
	if strings.Contains(shell, "http://") || strings.Contains(shell, "https://") || !strings.Contains(shell, `src="assets/v1/app.js"`) {
		t.Fatalf("shell has an external or non-relative dependency: %s", shell)
	}
	js := request(t, h, http.MethodGet, "/assets/v1/app.js", nil).Body.String()
	if strings.Contains(js, "cdn.") || strings.Contains(js, "https://") || !strings.Contains(js, "customElements.define") {
		t.Fatal("application is not a dependency-free Web Components bundle")
	}
	for _, marker := range []string{
		`get(observation, "actor")`, `get(history, "entries")`, `get(this._data, "currentAvailable")`,
		`get(record, "reason")`, `get(record, "dropped")`, `TIMELINE_LIMIT = 200`,
		`RECENT_PAGE_LIMIT = 1000`, `id: "ID"`, `"paged out"`,
	} {
		if !strings.Contains(js, marker) {
			t.Errorf("application is missing wire/operational contract marker %q", marker)
		}
	}
	for _, obsolete := range []string{`ActorID`, `capital(history,'Events')`, `['','counter','match','bot']`} {
		if strings.Contains(js, obsolete) {
			t.Errorf("application retains obsolete wire or example-specific marker %q", obsolete)
		}
	}
	mounted := http.StripPrefix("/inspect", h)
	for _, path := range []string{"/inspect/", "/inspect/assets/v1/app.js", "/inspect/v1/systems"} {
		if w := request(t, mounted, http.MethodGet, path, nil); w.Code != http.StatusOK {
			t.Errorf("mounted GET %s = %d %s", path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/", "/assets/v1/app.js"} {
		w := request(t, h, http.MethodPost, path, nil)
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodGet {
			t.Errorf("POST %s = %d Allow=%q", path, w.Code, w.Header().Get("Allow"))
		}
	}
}

func TestJSONEndpointsAndPaginationValidation(t *testing.T) {
	h, _, _ := testHandler(t, nil)
	for _, target := range []string{
		"/v1/systems", "/v1/actors?system=test&limit=1", "/v1/actor?system=test&id=actor",
		"/v1/definition?system=test&id=actor", "/v1/recent?system=test&limit=1",
	} {
		w := request(t, h, http.MethodGet, target, nil)
		if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
			t.Fatalf("GET %s = %d %s", target, w.Code, w.Body.String())
		}
		var result envelope
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result.Data == nil || result.Error != nil {
			t.Fatalf("GET %s envelope = %#v, %v", target, result, err)
		}
	}
	for _, target := range []string{"/v1/actors?system=test&limit=no", "/v1/actors?system=test&durable=no", "/v1/recent?system=test&cursor=-1"} {
		if w := request(t, h, http.MethodGet, target, nil); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), `"code":"invalid_request"`) {
			t.Fatalf("GET %s = %d %s", target, w.Code, w.Body.String())
		}
	}
	for _, target := range []string{
		"/v1/actors?system=test&kind=bad%20kind",
		"/v1/actors?system=test&lifecycle=retired",
		"/v1/actors?system=test&residency=remote",
	} {
		w := request(t, h, http.MethodGet, target, nil)
		if w.Code != http.StatusBadRequest || w.Body.String() != "{\"error\":{\"code\":\"invalid_request\",\"message\":\"invalid actor filter\"}}\n" {
			t.Fatalf("GET %s = %d %s", target, w.Code, w.Body.String())
		}
	}
}

func TestHistoryUnavailableUsesStableConflict(t *testing.T) {
	h, _, _ := testHandler(t, nil)
	w := request(t, h, http.MethodGet, "/v1/history?system=test&id=actor", nil)
	if w.Code != http.StatusConflict || w.Body.String() != "{\"error\":{\"code\":\"history_unavailable\",\"message\":\"durable history is unavailable\"}}\n" {
		t.Fatalf("history = %d %s", w.Code, w.Body.String())
	}
}

func TestAuthorizationUsesRequestContextAndHidesHostError(t *testing.T) {
	authorizer := inspector.AuthorizerFunc(func(ctx context.Context, _ inspector.AuthorizationRequest) error {
		if ctx.Value(identityKey{}) != "operator" {
			return context.Canceled
		}
		return nil
	})
	h, _, _ := testHandler(t, nil, inspector.WithAuthorizer(authorizer))
	denied := request(t, h, http.MethodGet, "/v1/systems", nil)
	if denied.Code != http.StatusForbidden || strings.Contains(denied.Body.String(), "canceled") {
		t.Fatalf("denied = %d %s", denied.Code, denied.Body.String())
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/systems", nil).WithContext(context.WithValue(context.Background(), identityKey{}, "operator"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("authorized = %d %s", w.Code, w.Body.String())
	}
}

func TestCommandStrictCanonicalDecodeAndSingleSend(t *testing.T) {
	received := make(chan statecharts.Event, 2)
	h, _, _ := testHandler(t, received)
	valid := `{"name":"message","data":{"version":1,"kind":"string","string":"hello"}}`
	if w := request(t, h, http.MethodPost, "/v1/events?system=test&id=actor", []byte(valid)); w.Code != http.StatusAccepted {
		t.Fatalf("send = %d %s", w.Code, w.Body.String())
	}
	event := <-received
	if text, ok := event.Data.AsString(); event.Name != "message" || event.Type != statecharts.EventExternal || !ok || text != "hello" {
		t.Fatalf("event = %#v", event)
	}
	select {
	case duplicate := <-received:
		t.Fatalf("duplicate = %#v", duplicate)
	default:
	}
	for _, body := range []string{
		`{"name":"message","data":{"version":1,"kind":"null"},"origin":"forged"}`,
		`{"name":"message","data":{"version":1,"kind":"string"}}`,
		valid + `{}`,
	} {
		if w := request(t, h, http.MethodPost, "/v1/events?system=test&id=actor", []byte(body)); w.Code != http.StatusBadRequest {
			t.Fatalf("body %s = %d %s", body, w.Code, w.Body.String())
		}
	}
}

type flushRecorder struct {
	mu      sync.Mutex
	header  http.Header
	body    bytes.Buffer
	status  int
	flushed chan struct{}
}

func newFlushRecorder() *flushRecorder {
	return &flushRecorder{header: make(http.Header), flushed: make(chan struct{}, 8)}
}
func (w *flushRecorder) Header() http.Header    { return w.header }
func (w *flushRecorder) WriteHeader(status int) { w.status = status }
func (w *flushRecorder) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(p)
}
func (w *flushRecorder) Flush() {
	select {
	case w.flushed <- struct{}{}:
	default:
	}
}
func (w *flushRecorder) String() string { w.mu.Lock(); defer w.mu.Unlock(); return w.body.String() }

func waitForBody(t *testing.T, w *flushRecorder, contains string) {
	t.Helper()
	for !strings.Contains(w.String(), contains) {
		select {
		case <-w.flushed:
		case <-t.Context().Done():
			t.Fatalf("waiting for %q in SSE body", contains)
		}
	}
}

type unwrapResponseWriter struct{ writer *flushRecorder }

func (w *unwrapResponseWriter) Header() http.Header         { return w.writer.Header() }
func (w *unwrapResponseWriter) Write(p []byte) (int, error) { return w.writer.Write(p) }
func (w *unwrapResponseWriter) WriteHeader(status int)      { w.writer.WriteHeader(status) }
func (w *unwrapResponseWriter) Unwrap() http.ResponseWriter { return w.writer }

func TestSSECatchupPaginatesAndThenDeliversLive(t *testing.T) {
	h, system, service := testHandler(t, nil,
		inspector.WithAuthorizer(inspector.AllowAll()),
		inspector.WithRingSize(1024),
		inspector.WithSourceBuffer(4096),
	)
	observed, err := service.Subscribe(t.Context(), "test", 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer observed.Close()
	for range 1005 {
		if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal}); err != nil {
			t.Fatal(err)
		}
		<-observed.C
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/v1/stream?system=test", nil).WithContext(ctx)
	w := newFlushRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, r); close(done) }()
	waitForBody(t, w, "id: 1005\n")

	if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	<-observed.C
	waitForBody(t, w, "id: 1006\n")
	cancel()
	<-done

	body := w.String()
	for sequence := uint64(1); sequence <= 1006; sequence++ {
		if count := strings.Count(body, "id: "+strconv.FormatUint(sequence, 10)+"\n"); count != 1 {
			t.Fatalf("sequence %d appeared %d times", sequence, count)
		}
	}
}

func TestSSEAheadCursorOnEmptyStreamEmitsEpochGap(t *testing.T) {
	h, _, _ := testHandler(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/v1/stream?system=test&cursor=999", nil).WithContext(ctx)
	w := newFlushRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, r); close(done) }()
	waitForBody(t, w, "cursor ahead of stream")
	cancel()
	<-done
	if body := w.String(); !strings.Contains(body, "event: gap\n") || strings.Contains(body, "id: 0\n") {
		t.Fatalf("empty-stream epoch gap = %q", body)
	}
}

func TestSSELastEventIDOverridesInitialURLCursor(t *testing.T) {
	h, system, service := testHandler(t, nil)
	observed, err := service.Subscribe(t.Context(), "test", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer observed.Close()
	for range 3 {
		if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal}); err != nil {
			t.Fatal(err)
		}
		<-observed.C
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/v1/stream?system=test&cursor=0", nil).WithContext(ctx)
	r.Header.Set("Last-Event-ID", "3")
	w := newFlushRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, r); close(done) }()
	<-w.flushed
	if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	<-observed.C
	waitForBody(t, w, "id: 4\n")
	cancel()
	<-done
	body := w.String()
	if strings.Contains(body, "id: 1\n") || strings.Contains(body, "id: 2\n") || strings.Contains(body, "id: 3\n") {
		t.Fatalf("reconnect replayed acknowledged records: %q", body)
	}
}

func TestSSEExpiredCursorEmitsOneGapThenRetainedRecords(t *testing.T) {
	h, system, service := testHandler(t, nil, inspector.WithAuthorizer(inspector.AllowAll()), inspector.WithRingSize(2))
	observed, err := service.Subscribe(t.Context(), "test", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer observed.Close()
	for range 4 {
		if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal}); err != nil {
			t.Fatal(err)
		}
		<-observed.C
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/v1/stream?system=test", nil).WithContext(ctx)
	r.Header.Set("Last-Event-ID", "1")
	w := newFlushRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, r); close(done) }()
	waitForBody(t, w, "id: 4\n")
	cancel()
	<-done
	body := w.String()
	if strings.Count(body, "event: gap\n") != 1 || !strings.Contains(body, "cursor expired") || strings.Count(body, "id: 2\n") != 1 || strings.Count(body, "id: 3\n") != 1 || strings.Count(body, "id: 4\n") != 1 {
		t.Fatalf("expired cursor stream = %q", body)
	}
}

func TestSSEFlushesThroughUnwrappingMiddlewareAndEndsWhenServiceCloses(t *testing.T) {
	h, _, service := testHandler(t, nil)
	r := httptest.NewRequest(http.MethodGet, "/v1/stream?system=test", nil)
	inner := newFlushRecorder()
	w := &unwrapResponseWriter{writer: inner}
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, r); close(done) }()
	<-inner.flushed
	service.Close()
	waitForBody(t, inner, "stream_closed")
	<-done
	if !strings.Contains(inner.String(), "event: error\n") {
		t.Fatalf("closed stream = %q", inner.String())
	}
}

func TestSSERepeatedConnectDisconnectReturnsEveryHandler(t *testing.T) {
	h, _, service := testHandler(t, nil)
	for range 50 {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		request := httptest.NewRequest(http.MethodGet, "/v1/stream?system=test", nil).WithContext(ctx)
		response := newFlushRecorder()
		done := make(chan struct{})
		go func() {
			h.ServeHTTP(response, request)
			close(done)
		}()
		select {
		case <-response.flushed:
		case <-ctx.Done():
			t.Fatal("SSE handler did not establish its stream")
		}
		cancel()
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-done:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			t.Fatal("SSE handler did not return after disconnect")
		}
	}

	done := make(chan struct{})
	go func() {
		service.Close()
		close(done)
	}()
	timer := time.NewTimer(2 * time.Second)
	select {
	case <-done:
		if !timer.Stop() {
			<-timer.C
		}
	case <-timer.C:
		t.Fatal("service close did not join stream drainers after repeated disconnects")
	}
}

func TestSSELastEventIDCatchupGapDedupAndCancellation(t *testing.T) {
	h, system, service := testHandler(t, nil, inspector.WithAuthorizer(inspector.AllowAll()), inspector.WithRingSize(2))
	ready, err := service.Subscribe(t.Context(), "test", 4)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal}); err != nil {
			t.Fatal(err)
		}
		<-ready.C
	}
	ready.Close()
	page, err := service.Recent(t.Context(), "test", 0, 10)
	if err != nil || page.Latest < 3 {
		t.Fatalf("recent = %#v, %v", page, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/v1/stream?system=test", nil).WithContext(ctx)
	r.Header.Set("Last-Event-ID", "1")
	w := newFlushRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, r); close(done) }()
	for range 3 {
		<-w.flushed
	}
	cancel()
	<-done
	body := w.String()
	if !strings.Contains(body, "event: observation") || strings.Count(body, "id: 2\n") != 1 || strings.Count(body, "id: 3\n") != 1 {
		t.Fatalf("SSE catchup/dedup = %q", body)
	}

	ctx, cancel = context.WithCancel(context.Background())
	r = httptest.NewRequest(http.MethodGet, "/v1/stream?system=test&cursor=999", nil).WithContext(ctx)
	w = newFlushRecorder()
	done = make(chan struct{})
	go func() { h.ServeHTTP(w, r); close(done) }()
	<-w.flushed
	cancel()
	<-done
	if !strings.Contains(w.String(), "event: gap") || !strings.Contains(w.String(), "cursor ahead of stream") {
		t.Fatalf("ahead cursor SSE = %q", w.String())
	}
}

func TestClosedAndUnknownResourcesUseStableErrors(t *testing.T) {
	h, _, service := testHandler(t, nil)
	if w := request(t, h, http.MethodGet, "/v1/actor?system=missing&id=actor", nil); w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "unknown_system") {
		t.Fatalf("unknown system = %d %s", w.Code, w.Body.String())
	}
	if w := request(t, h, http.MethodGet, "/v1/actor?system=test&id=missing", nil); w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "unknown_actor") {
		t.Fatalf("unknown actor = %d %s", w.Code, w.Body.String())
	}
	service.Close()
	if w := request(t, h, http.MethodGet, "/v1/systems", nil); w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "service_unavailable") {
		t.Fatalf("closed = %d %s", w.Code, w.Body.String())
	}
}
