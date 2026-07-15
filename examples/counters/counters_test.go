package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

func mustQuery(t *testing.T, rt *counterRuntime, selected []string) []projection {
	t.Helper()
	ps, err := rt.query(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

func waitValue(t *testing.T, rt *counterRuntime, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range mustQuery(t, rt, colors) {
			if p.Name == name && p.Value == want {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s never reached %d: %#v", name, want, mustQuery(t, rt, colors))
}

func TestDurableIdempotenceAndRecovery(t *testing.T) {
	path := t.TempDir() + "/counters.db"
	ctx1, cancel1 := context.WithCancel(context.Background())
	store1, err := openLog(path)
	if err != nil {
		t.Fatal(err)
	}
	rt1, err := setupCounters(ctx1, store1)
	sys1 := rt1.counters
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := sys1.Tell(ctx1, "red", incrementEvent("same-write")); err != nil {
			t.Fatal(err)
		}
	}
	waitValue(t, rt1, "red", 1)
	if err := sys1.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	cp, ok, err := store1.Load(context.Background(), "red")
	if err != nil || !ok {
		t.Fatalf("load red checkpoint: ok=%v err=%v", ok, err)
	}
	if cp.Seq == 0 {
		t.Fatal("red checkpoint sequence = 0, want real log boundary")
	}
	// Datamodel snapshot bytes are an opaque, model-owned cache. The restart
	// below proves that they restore the count and idempotency state without
	// coupling the example to the GoModel snapshot envelope.
	cancel1()
	store1.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	store2, err := openLog(path)
	if err != nil {
		t.Fatal(err)
	}
	rt2, err := setupCounters(ctx2, store2)
	sys2 := rt2.counters
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.stop(context.Background())
	waitValue(t, rt2, "red", 1)
	if err := sys2.Tell(ctx2, "red", incrementEvent("same-write")); err != nil {
		t.Fatal(err)
	}
	waitValue(t, rt2, "red", 1)
}

func TestFourthActivationUpdatesResidencyProjection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := openLog(t.TempDir() + "/counters.db")
	if err != nil {
		t.Fatal(err)
	}
	rt, err := setupCounters(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.stop(context.Background())

	before := mustQuery(t, rt, colors)
	if got := residentCount(before); got != 3 {
		t.Fatalf("initial resident count = %d, want 3: %#v", got, before)
	}
	if err := rt.counters.Tell(ctx, "red", incrementEvent("activate-red")); err != nil {
		t.Fatal(err)
	}
	after := mustQuery(t, rt, colors)
	if got := residentCount(after); got != 3 {
		t.Fatalf("resident count after fourth activation = %d, want 3: %#v", got, after)
	}
	if !projectionFor(t, after, "red").Resident {
		t.Fatal("newly activated red counter is not resident")
	}
	evicted := false
	for _, p := range before {
		if p.Resident && !projectionFor(t, after, p.Name).Resident {
			evicted = true
			break
		}
	}
	if !evicted {
		t.Fatalf("activating red did not evict one of the prior residents: before=%#v after=%#v", before, after)
	}
}

func residentCount(ps []projection) int {
	n := 0
	for _, p := range ps {
		if p.Resident {
			n++
		}
	}
	return n
}

func projectionFor(t *testing.T, ps []projection, name string) projection {
	t.Helper()
	for _, p := range ps {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("projection %q not found in %#v", name, ps)
	return projection{}
}

func TestIncrementEventCarriesIdentifierInCanonicalTaggedPayload(t *testing.T) {
	ev := incrementEvent("write-42")
	if ev.Name != "increment" {
		t.Fatalf("event name = %q, want stable increment name", ev.Name)
	}
	encoded, err := statecharts.EncodeEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := statecharts.DecodeEvent(encoded)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := taggedFields(decoded.Data, incrementValueTag)
	if err != nil {
		t.Fatal(err)
	}
	writeID, ok := fields["write_id"].AsString()
	if !ok || writeID != "write-42" {
		t.Fatalf("decoded payload = %#v, want Identifier write-42", decoded.Data)
	}
}

func TestProjectionDecodesCanonicalExponentInteger(t *testing.T) {
	want := projection{Name: "red", Color: "red", Value: 10, Resident: true, ActorState: actorStateResident}
	got, err := decodeProjection(encodeProjection(want))
	if err != nil {
		t.Fatalf("decodeProjection value 10: %v", err)
	}
	if got != want {
		t.Fatalf("decodeProjection value 10 = %#v, want %#v", got, want)
	}
}

func TestSnapshotParserAndRendering(t *testing.T) {
	var got []projection
	err := consumeSSE(strings.NewReader(": hi\nevent: snapshot\ndata: [{\"name\":\"blue\",\"color\":\"blue\",\"value\":12}]\n\n"), func(p []projection) { got = p })
	if err != nil || len(got) != 1 || got[0].Value != 12 {
		t.Fatalf("got %#v, err %v", got, err)
	}
	html := renderString(renderCounterBox(got[0]))
	if !strings.Contains(html, "#2563eb") || !strings.Contains(html, ">12<") {
		t.Fatalf("unexpected box: %s", html)
	}
}

func TestServerPageStartsDatastarEventStreamAndCountersAreClickable(t *testing.T) {
	recorder := httptest.NewRecorder()
	pageHandler(func() []projection { return nil })(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	page := recorder.Body.String()

	if recorder.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q", recorder.Header().Get("Content-Type"))
	}
	if !strings.Contains(page, `src="/datastar.js"`) || strings.Contains(page, "cdn.jsdelivr") || len(datastarJS) == 0 {
		t.Fatalf("page does not load the Datastar v1.0.2 browser bundle: %s", page)
	}
	if !strings.Contains(page, `data-init="@get(&#39;/ui/events&#39;,`) {
		t.Fatalf("page does not start its Datastar event stream: %s", page)
	}
	if !strings.Contains(page, "retryMaxCount") {
		t.Fatalf("page event stream does not keep retrying after an outage: %s", page)
	}
	if strings.Contains(page, `\"`) {
		t.Fatalf("page contains backslash-escaped HTML attributes: %s", page)
	}
	card := renderString(renderCounterBox(projection{Name: "red", Color: "red", Value: 12, Resident: true}))
	if !strings.Contains(card, `<button`) || !strings.Contains(card, `data-on:click="@post(&#39;/counters/red/increment&#39;)"`) {
		t.Fatalf("counter is not an increment control: %s", card)
	}
	pagedOut := renderString(renderCounterBox(projection{Name: "blue", Color: "blue", Value: 9}))
	if !strings.Contains(pagedOut, "is-hydrating") || strings.Contains(pagedOut, "querySelector(&#39;data&#39;)") {
		t.Fatalf("paged-out tap must show hydration without changing the projected count: %s", pagedOut)
	}
	hydrating := renderString(renderCounterBox(projection{Name: "blue", Color: "blue", Value: 9, ActorState: actorStateHydrating}))
	if !strings.Contains(hydrating, `class="counter is-hydrating"`) || !strings.Contains(hydrating, `>hydrating</span>`) || !strings.Contains(hydrating, `>9</data>`) {
		t.Fatalf("hydrating actor state is not rendered independently of its durable count: %s", hydrating)
	}
}

func TestServerUIIncrementEndpointUpdatesCounter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := openLog(t.TempDir() + "/counters.db")
	if err != nil {
		t.Fatal(err)
	}
	rt, err := setupCounters(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.stop(context.Background())
	before := projectionFor(t, mustQuery(t, rt, colors), "red").Value
	server := counterHandler(rt)
	definitionResponse := httptest.NewRecorder()
	server.ServeHTTP(definitionResponse, httptest.NewRequest(http.MethodGet, "/definitions/counter", nil))
	if definitionResponse.Code != http.StatusOK || definitionResponse.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("definition response = %d %q", definitionResponse.Code, definitionResponse.Header().Get("Content-Type"))
	}
	var definition statecharts.Definition
	if err := json.Unmarshal(definitionResponse.Body.Bytes(), &definition); err != nil {
		t.Fatalf("decode canonical definition: %v", err)
	}
	if definition.ID != counterKind || !strings.Contains(definitionResponse.Body.String(), "counters.counter.apply-idempotent-increment") || strings.Contains(definitionResponse.Body.String(), "func") {
		t.Fatalf("counter definition is not stable and inspectable: %s", definitionResponse.Body.String())
	}

	page := httptest.NewRecorder()
	server.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := strings.Count(page.Body.String(), "<button"); got != len(colors) {
		t.Fatalf("server-rendered counter cards = %d, want %d: %s", got, len(colors), page.Body.String())
	}

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/counters/red/increment", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("increment status = %d, body %q", recorder.Code, recorder.Body.String())
	}
	waitValue(t, rt, "red", before+1)

	datastarRequest := httptest.NewRequest(http.MethodPost, "/counters/red/increment", nil)
	datastarRequest.Header.Set("Datastar-Request", "true")
	datastarResponse := httptest.NewRecorder()
	server.ServeHTTP(datastarResponse, datastarRequest)
	if datastarResponse.Code != http.StatusOK || datastarResponse.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("Datastar increment response = %d %q, want 200 text/html", datastarResponse.Code, datastarResponse.Header().Get("Content-Type"))
	}
	if body := datastarResponse.Body.String(); !strings.Contains(body, `id="dashboard"`) || !strings.Contains(body, fmt.Sprintf(">%d</data>", before+2)) {
		t.Fatalf("Datastar increment response does not contain the updated dashboard: %s", body)
	}
	waitValue(t, rt, "red", before+2)
}

func TestEventStreamColorSelection(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/events?colors=violet%2Cred", nil)
	got, err := eventStreamColors(request)
	if err != nil || strings.Join(got, ",") != "violet,red" {
		t.Fatalf("eventStreamColors = %v, %v", got, err)
	}
	if _, err := eventStreamColors(httptest.NewRequest(http.MethodGet, "/events?colors=unknown", nil)); err == nil {
		t.Fatal("unknown event-stream color was accepted")
	}
}

func TestEventStreamDoesNotRepeatUnchangedSelectedCounters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := openLog(t.TempDir() + "/counters.db")
	if err != nil {
		t.Fatal(err)
	}
	rt, err := setupCounters(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.stop(context.Background())
	server := httptest.NewServer(counterHandler(rt))
	defer server.Close()

	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	request, err := http.NewRequestWithContext(streamCtx, http.MethodGet, server.URL+"/events?colors=blue", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	stream := bufio.NewReader(response.Body)
	for range 3 {
		line, err := stream.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data:") && !strings.Contains(line, `"name":"blue"`) {
			t.Fatalf("initial snapshot = %q, want blue", line)
		}
	}

	// This projection can only reach hub@ui through the counter System's
	// Bridge; the HTTP/service side never writes hub projection state.
	// Indigo is among the initially resident actors, so this changes no blue
	// value or residency state.
	if err := rt.counters.Tell(ctx, "indigo", incrementEvent("unrelated-indigo")); err != nil {
		t.Fatal(err)
	}
	read := make(chan string, 1)
	go func() {
		line, _ := stream.ReadString('\n')
		read <- line
	}()
	select {
	case line := <-read:
		t.Fatalf("unrelated red update produced blue stream output %q", line)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestColorSelectionUsesPositionalNames(t *testing.T) {
	got, err := selectColors([]string{"blue", "red"}, 7)
	if err != nil || len(got) != 2 || got[0] != "blue" || got[1] != "red" {
		t.Fatalf("selectColors positional = %v, %v", got, err)
	}
	got, err = selectColors(nil, 3)
	if err != nil || strings.Join(got, ",") != "red,orange,yellow" {
		t.Fatalf("selectColors default = %v, %v", got, err)
	}
	if _, err := selectColors([]string{"chartreuse"}, 7); err == nil {
		t.Fatal("unknown color was accepted")
	}
	if _, err := selectColors([]string{"red", "red"}, 7); err == nil {
		t.Fatal("duplicate color was accepted")
	}
}

func TestTerminalViewsShowCountsAndPerColorSparklines(t *testing.T) {
	reader := readerTerminalFrame("connected", []string{"red", "blue"}, []projection{
		{Name: "red", Value: 42, Resident: true},
		{Name: "blue", Value: 9, Resident: false},
	})
	for _, want := range []string{"red", "42", "resident", "blue", "9", "paged out"} {
		if !strings.Contains(reader, want) {
			t.Fatalf("reader terminal frame lacks %q: %s", want, reader)
		}
	}
	line := sparkline([]uint64{0, 1, 2, 4, 8}, 8)
	if len([]rune(line)) != 8 || !strings.ContainsAny(line, "▁▂▃▄▅▆▇█") {
		t.Fatalf("invalid sparkline %q", line)
	}

	writer := writerTerminalFrame("connected", 25, []colorWriteStats{{
		Color: "red", Generated: 12, Attempts: 20, Succeeded: 10, Retries: 8, InFlight: 2,
	}}, make(map[string]uint64), make(map[string][]uint64))
	for _, want := range []string{"target: 25.0 writes/s", "offered", "writes", "attempts", "24.0/s", "12", "20"} {
		if !strings.Contains(writer, want) {
			t.Fatalf("writer terminal frame lacks %q: %s", want, writer)
		}
	}
}

func TestFollowSnapshotsReconnects(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := requests.Add(1)
		if attempt == 1 {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: snapshot\ndata: [{\"name\":\"red\",\"color\":\"red\",\"value\":9}]\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	state := newReaderState()
	connection, err := newConnectionActor(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.stop(context.Background())
	done := make(chan struct{})
	go func() {
		followSnapshots(ctx, server.URL, []string{"red"}, state, connection)
		close(done)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, counters := connection.status(), state.snapshot()
		if status == "connected" && len(counters) == 1 && counters[0].Value == 9 {
			cancel()
			select {
			case <-done:
				return
			case <-time.After(time.Second):
				t.Fatal("snapshot follower did not stop after cancellation")
			}
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	status, counters := connection.status(), state.snapshot()
	t.Fatalf("reader did not reconnect: requests=%d status=%q counters=%#v", requests.Load(), status, counters)
}

func TestConnectionActorUsesExplicitChartStates(t *testing.T) {
	updates := make(chan string, 3)
	a, err := newConnectionActor(context.Background(), func(status string) { updates <- status })
	if err != nil {
		t.Fatal(err)
	}
	defer a.stop(context.Background())
	if got := a.status(); got != "connecting" {
		t.Fatalf("initial = %q", got)
	}
	a.outcome(context.Background(), false)
	deadline := time.Now().Add(time.Second)
	for a.status() != "reconnecting" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := a.status(); got != "reconnecting" {
		t.Fatalf("failure = %q", got)
	}
	a.outcome(context.Background(), true)
	deadline = time.Now().Add(time.Second)
	for a.status() != "connected" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := a.status(); got != "connected" {
		t.Fatalf("success = %q", got)
	}
	for _, want := range []string{"connecting", "reconnecting", "connected"} {
		select {
		case got := <-updates:
			if got != want {
				t.Fatalf("connection transition = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("connection actor did not publish %q transition", want)
		}
	}
}

func TestDatastarPatchFramesEveryHTMLLine(t *testing.T) {
	got := datastarPatch("<main id=\"dashboard\">\ncontent\n</main>")
	want := "event: datastar-patch-elements\n" +
		"data: elements <main id=\"dashboard\">\n" +
		"data: elements content\n" +
		"data: elements </main>\n\n"
	if got != want {
		t.Fatalf("patch mismatch:\n got: %q\nwant: %q", got, want)
	}

	var rendered bytes.Buffer
	if err := renderDashboard("connected", []projection{{Name: "red", Color: "red", Value: 1}}).WriteHTML(&rendered); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered.String(), `id="dashboard"`) {
		t.Fatalf("dashboard has no morph target: %s", rendered.String())
	}
}
