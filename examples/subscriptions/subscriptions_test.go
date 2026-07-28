package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/datamodel/ecmascript"
	"github.com/dhamidi/statecharts/sqllog/sqlite3"
)

func TestDefinitionRoundTripsAndInitialConfigurationIsParallel(t *testing.T) {
	c, err := subscriptionChart()
	if err != nil {
		t.Fatal(err)
	}
	if c.Definition().Datamodel != "ecmascript" {
		t.Fatalf("datamodel=%q", c.Definition().Datamodel)
	}
	b, _ := json.Marshal(c.Definition())
	var d statecharts.Definition
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	m, _ := ecmascript.New()
	round, err := statecharts.Compile(d, m)
	if err != nil || !reflect.DeepEqual(c.Definition(), round.Definition()) {
		t.Fatalf("round trip: %v", err)
	}
	r := newRuntime(t, t.TempDir()+"/parallel.db")
	defer closeRuntime(t, r)
	if err := r.create(context.Background(), "sub.parallel", "starter", "accepted_delayed_success", 1); err != nil {
		t.Fatal(err)
	}
	p := waitProjection(t, r, "sub.parallel", func(p map[string]any) bool {
		return hasState(p, "grace") && (hasState(p, "charging") || hasState(p, "awaiting"))
	})
	if !hasState(p, "grace") {
		t.Fatalf("configuration=%v", p["states"])
	}
}

func TestDeterministicScenarios(t *testing.T) {
	tests := []struct{ scenario, state string }{
		{"success", "enabled"}, {"hard_decline", "disabled"}, {"retryable_decline", "enabled"},
		{"communication_failure", "enabled"}, {"accepted_delayed_success", "enabled"},
		{"duplicate_result", "enabled"}, {"stale_out_of_order", "enabled"},
		{"lost_result", "enabled"}, {"idempotency_replay", "enabled"},
	}
	for _, tc := range tests {
		t.Run(tc.scenario, func(t *testing.T) {
			r := newRuntime(t, t.TempDir()+"/scenario.db")
			defer closeRuntime(t, r)
			if err := r.create(context.Background(), "sub.scenario", "starter", tc.scenario, 2); err != nil {
				t.Fatal(err)
			}
			p := waitProjection(t, r, "sub.scenario", func(p map[string]any) bool { return hasState(p, tc.state) })
			if tc.scenario == "duplicate_result" {
				p = waitProjection(t, r, "sub.scenario", func(p map[string]any) bool { return number(p["duplicate_count"]) >= 1 })
			}
			if tc.scenario == "stale_out_of_order" {
				p = waitProjection(t, r, "sub.scenario", func(p map[string]any) bool { return number(p["stale_count"]) >= 1 })
			}
			if tc.scenario == "retryable_decline" && number(p["attempt"]) != 2 {
				t.Fatalf("attempt=%v", p["attempt"])
			}
			if tc.scenario == "duplicate_result" && number(p["duplicate_count"]) < 1 {
				t.Fatalf("duplicates=%v", p["duplicate_count"])
			}
			if tc.scenario == "stale_out_of_order" && number(p["stale_count"]) < 1 {
				t.Fatalf("stale=%v", p["stale_count"])
			}
			if tc.scenario == "communication_failure" {
				if number(p["attempt"]) != 1 {
					t.Fatalf("unknown result caused another charge: attempt=%v", p["attempt"])
				}
				if !activityHas(p, "communication_error") || !activityHas(p, "lookup") {
					t.Fatalf("activity=%v", p["activity"])
				}
				activity := p["activity"].([]map[string]any)
				if activity[0]["kind"] != "lookup" || activity[1]["kind"] != "communication_error" || activity[2]["kind"] != "payment" {
					t.Fatalf("activity is not newest first: %v", activity)
				}
			}
			if tc.scenario == "lost_result" && !activityHas(p, "lookup") {
				t.Fatalf("activity=%v", p["activity"])
			}
		})
	}
}

func TestResultsCarryIdentityMoneyAndRejectMismatches(t *testing.T) {
	r := newRuntime(t, t.TempDir()+"/result.db")
	defer closeRuntime(t, r)
	if err := r.create(context.Background(), "sub.result", "starter", "accepted_delayed_success", 2); err != nil {
		t.Fatal(err)
	}
	p := waitProjection(t, r, "sub.result", func(p map[string]any) bool { return hasState(p, "awaiting") })
	bad := map[string]any{"operation": p["operation"], "attempt": p["attempt"], "correlation": p["correlation"], "idempotency_key": p["idempotency_key"], "result_id": "forged", "period": p["period"], "amount": 1, "currency": "USD", "status": "succeeded"}
	if err := r.tell(context.Background(), "sub.result", "payment.succeeded", bad); err != nil {
		t.Fatal(err)
	}
	p = waitProjection(t, r, "sub.result", func(p map[string]any) bool { return number(p["stale_count"]) > 0 })
	if hasState(p, "enabled") {
		t.Fatal("mismatched amount enabled entitlement")
	}
	p = waitProjection(t, r, "sub.result", func(p map[string]any) bool { return hasState(p, "enabled") })
	acts := p["activity"].([]map[string]any)
	if len(acts) == 0 {
		t.Fatal("no activity")
	}
	for _, k := range []string{"operation", "attempt", "correlation", "key", "result_id", "amount", "currency"} {
		if _, ok := acts[0][k]; !ok {
			t.Fatalf("activity lacks %s: %v", k, acts[0])
		}
	}
}

func TestProcessorIdempotencyReplayConflictAndSinglePayment(t *testing.T) {
	s, err := sqlite3.Open(t.TempDir() + "/p.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := initPayments(s.DB()); err != nil {
		t.Fatal(err)
	}
	p := newPayments(s.DB(), nil)
	defer p.Close()
	d := &captureDispatcher{events: make(chan statecharts.Event, 8)}
	b := p.factory().(*paymentBinding)
	b.Attach(d)
	request := func(fp string) statecharts.SendRequest {
		return statecharts.SendRequest{Type: "payments", Event: "charge", Data: value(map[string]any{"subscription": "sub.x", "operation": "sub.x:1", "attempt": 1, "correlation": "c1", "idempotency_key": "k", "fingerprint": fp, "period": 1, "amount": 900, "currency": "USD", "scenario": "idempotency_replay"})}
	}
	if err := b.Send(context.Background(), request("same")); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(context.Background(), request("same")); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(context.Background(), request("different")); err == nil {
		t.Fatal("conflicting fingerprint accepted")
	}
	var payments, deliveries int
	_ = s.DB().QueryRow(`SELECT count(*) FROM payment_ledger`).Scan(&payments)
	_ = s.DB().QueryRow(`SELECT count(*) FROM payment_jobs`).Scan(&deliveries)
	if payments != 1 {
		t.Fatalf("payments=%d", payments)
	}
	if deliveries < 2 {
		t.Fatalf("replay did not return original settlement: jobs=%d", deliveries)
	}
}

func TestDelayedJobAndProjectionSurviveRestart(t *testing.T) {
	path := t.TempDir() + "/restart.db"
	r := newRuntime(t, path)
	if err := r.create(context.Background(), "sub.restart", "starter", "accepted_delayed_success", 1); err != nil {
		t.Fatal(err)
	}
	waitProjection(t, r, "sub.restart", func(p map[string]any) bool { return hasState(p, "awaiting") })
	closeRuntime(t, r)
	r = newRuntime(t, path)
	defer closeRuntime(t, r)
	p := waitProjection(t, r, "sub.restart", func(p map[string]any) bool { return hasState(p, "enabled") })
	if p["id"] != "sub.restart" {
		t.Fatalf("projection=%v", p)
	}
}

func TestProjectionDoesNotAppendQueryEventsOrOutboundEffects(t *testing.T) {
	r := newRuntime(t, t.TempDir()+"/projection.db")
	defer closeRuntime(t, r)
	if err := r.create(context.Background(), "sub.projection", "starter", "success", 1); err != nil {
		t.Fatal(err)
	}
	waitProjection(t, r, "sub.projection", func(p map[string]any) bool { return hasState(p, "enabled") })
	var logBefore, outboundBefore int
	if err := r.store.DB().QueryRow(`SELECT count(*) FROM statechart_log WHERE session_id=?`, "sub.projection").Scan(&logBefore); err != nil {
		t.Fatal(err)
	}
	if err := r.store.DB().QueryRow(`SELECT count(*) FROM statechart_outbound WHERE session_id=?`, "sub.projection").Scan(&outboundBefore); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := r.projection(context.Background(), "sub.projection"); err != nil {
			t.Fatal(err)
		}
	}
	var logAfter, outboundAfter int
	if err := r.store.DB().QueryRow(`SELECT count(*) FROM statechart_log WHERE session_id=?`, "sub.projection").Scan(&logAfter); err != nil {
		t.Fatal(err)
	}
	if err := r.store.DB().QueryRow(`SELECT count(*) FROM statechart_outbound WHERE session_id=?`, "sub.projection").Scan(&outboundAfter); err != nil {
		t.Fatal(err)
	}
	if logAfter != logBefore || outboundAfter != outboundBefore {
		t.Fatalf("projection mutated durable history: log %d -> %d, outbound %d -> %d", logBefore, logAfter, outboundBefore, outboundAfter)
	}
}

func TestSettledChargeCancelsItsReconciliationTimer(t *testing.T) {
	r := newRuntime(t, t.TempDir()+"/timer.db")
	defer closeRuntime(t, r)
	if err := r.create(context.Background(), "sub.timer", "starter", "success", 1); err != nil {
		t.Fatal(err)
	}
	waitProjection(t, r, "sub.timer", func(p map[string]any) bool { return hasState(p, "current") })
	_, inspection, err := r.system.InspectActor(context.Background(), "sub.timer")
	if err != nil {
		t.Fatal(err)
	}
	if inspection == nil {
		t.Fatal("actor is not resident")
	}
	for _, pending := range inspection.PendingSends {
		if pending.Event.Name == "lookup.timer" {
			t.Fatalf("settled charge retained reconciliation timer: %+v", pending)
		}
	}
}

func TestProcessorCallbackNotifiesObservers(t *testing.T) {
	r := newRuntime(t, t.TempDir()+"notify.db")
	defer closeRuntime(t, r)
	if err := r.create(context.Background(), "sub.notify", "starter", "accepted_delayed_success", 1); err != nil {
		t.Fatal(err)
	}
	waitProjection(t, r, "sub.notify", func(p map[string]any) bool { return hasState(p, "awaiting") })
	c := make(chan struct{}, 1)
	r.mu.Lock()
	r.watchers["sub.notify"] = map[chan struct{}]struct{}{c: {}}
	r.mu.Unlock()
	select {
	case <-c:
	case <-time.After(3 * time.Second):
		t.Fatal("processor callback did not notify SSE observers")
	}
	r.mu.Lock()
	delete(r.watchers, "sub.notify")
	r.mu.Unlock()
}

func TestCreateConflictCancellationProjectionAndHTTPAuthority(t *testing.T) {
	r := newRuntime(t, t.TempDir()+"http.db")
	defer closeRuntime(t, r)
	if err := r.create(context.Background(), "sub.http", "starter", "success", 1); err != nil {
		t.Fatal(err)
	}
	if err := r.create(context.Background(), "sub.http", "scale", "hard_decline", 9); err == nil {
		t.Fatal("existing subscription reconfigured")
	}
	waitProjection(t, r, "sub.http", func(p map[string]any) bool { return hasState(p, "enabled") })
	if err := r.tell(context.Background(), "sub.http", "cancel", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	p := waitProjection(t, r, "sub.http", func(p map[string]any) bool { return hasState(p, "cancelled") })
	if p["plan"] != "starter" {
		t.Fatalf("projection=%v", p)
	}
	h := handler(r, nil)
	post := func(path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		q := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		q.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, q)
		return w
	}
	if w := post("/api/subscriptions", `{`); w.Code != 400 {
		t.Fatalf("malformed=%d", w.Code)
	}
	if w := post("/api/subscriptions", `{"ID":"sub.http","Plan":"scale","Scenario":"success","Quantity":1}`); w.Code != 409 {
		t.Fatalf("conflict=%d", w.Code)
	}
	if w := post("/api/subscriptions/missing/retry", `{}`); w.Code != 404 {
		t.Fatalf("unknown=%d", w.Code)
	}
	if w := post("/api/subscriptions/sub.http/advance", `{"period":999,"unit_amount":1}`); w.Code != 200 {
		t.Fatalf("advance=%d", w.Code)
	}
}

func TestConsoleAssetsAndRoutes(t *testing.T) {
	r := newRuntime(t, t.TempDir()+"/assets.db")
	defer closeRuntime(t, r)
	h := handler(r, nil)
	tests := []struct {
		path, contentType, marker string
	}{
		{"/", "text/html; charset=utf-8", "<subscription-console>"},
		{"/app.js", "text/javascript; charset=utf-8", "customElements.define('processor-activity'"},
		{"/styles.css", "text/css; charset=utf-8", "@media(max-width:700px)"},
		{"/api/plans", "application/json", `"unit_amount":900`},
	}
	for _, tc := range tests {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if w.Code != http.StatusOK || w.Header().Get("Content-Type") != tc.contentType || !strings.Contains(w.Body.String(), tc.marker) {
			t.Fatalf("GET %s: code=%d type=%q body marker=%v", tc.path, w.Code, w.Header().Get("Content-Type"), strings.Contains(w.Body.String(), tc.marker))
		}
		if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(w.Header().Get("Content-Security-Policy"), "connect-src 'self'") {
			t.Fatalf("GET %s missing security/cache headers: %v", tc.path, w.Header())
		}
	}
	for _, path := range []string{"/index.html", "/app.js/typo", "/api/subscriptions/typo/unknown"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("GET %s unexpectedly returned %d", path, w.Code)
		}
	}
	if strings.Contains(appJS, "innerHTML") || strings.Contains(indexHTML, "http://") || strings.Contains(indexHTML, "https://") {
		t.Fatal("console assets permit unsafe rendering or external assets")
	}
	for _, marker := range []string{"EventSource", "generation!==this.generation", "Intl.NumberFormat", "communication_failure", "idempotency_replay"} {
		if !strings.Contains(appJS, marker) {
			t.Fatalf("app.js lacks UI contract marker %q", marker)
		}
	}
	for _, marker := range []string{"ids.length===1?'actor':'actors'", "activity-list", "identityFact('Operation'", "copyButton(label,id)", "scenario-control", "scenario-authority", "const signature=JSON.stringify", "el('details',{class:'create-panel'})"} {
		if !strings.Contains(appJS, marker) {
			t.Fatalf("app.js lacks reviewed UI marker %q", marker)
		}
	}
	if strings.Contains(appJS, "activity-table") || strings.Contains(appJS, "el('table'") {
		t.Fatal("processor activity must use a rail-appropriate list, not a table")
	}
}

func newRuntime(t *testing.T, path string) *runtime {
	t.Helper()
	r, e := openRuntime(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func closeRuntime(t *testing.T, r *runtime) {
	t.Helper()
	if e := r.close(context.Background()); e != nil {
		t.Error(e)
	}
	if e := r.store.Close(); e != nil {
		t.Error(e)
	}
}
func waitProjection(t *testing.T, r *runtime, id string, ok func(map[string]any) bool) map[string]any {
	t.Helper()
	d := time.Now().Add(8 * time.Second)
	var p map[string]any
	var e error
	for time.Now().Before(d) {
		p, e = r.projection(context.Background(), id)
		if e == nil && ok(p) {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout projection=%v error=%v", p, e)
	return nil
}
func hasState(p map[string]any, w string) bool {
	if p == nil {
		return false
	}
	for _, s := range p["states"].([]string) {
		if s == w {
			return true
		}
	}
	return false
}
func number(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case json.Number:
		x, _ := n.Int64()
		return x
	}
	return 0
}
func activityHas(p map[string]any, kind string) bool {
	for _, a := range p["activity"].([]map[string]any) {
		if a["kind"] == kind {
			return true
		}
	}
	return false
}

type captureDispatcher struct{ events chan statecharts.Event }

func (d *captureDispatcher) Deliver(_ context.Context, e statecharts.Event) error {
	d.events <- e
	return nil
}
