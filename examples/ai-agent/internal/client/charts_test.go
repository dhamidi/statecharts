package client

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

func TestClientChartsRoundTripDefinitionsAndSnapshotWithoutCapabilities(t *testing.T) {
	runtime := &uiRuntime{Subscribers: []chan string{make(chan string)}}
	requests := newUIRequests()
	requests.snapshots["held"] = make(chan uiSnapshot)
	requests.subscriptions["held"] = make(chan chan string)
	requests.unsubscribes["held"] = make(chan string)

	tests := []struct {
		name  string
		build func() (*statecharts.Chart, error)
	}{
		{"link", func() (*statecharts.Chart, error) {
			return BuildLinkChart("http://example.test", []protocol.ToolName{"shell_command"})
		}},
		{"directorylink", func() (*statecharts.Chart, error) { return BuildDirectoryLinkChart("http://example.test") }},
		{"tool", func() (*statecharts.Chart, error) { return BuildToolChart("http://example.test") }},
		{"ui", func() (*statecharts.Chart, error) { return BuildUIChart(runtime, requests) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chart, err := test.build()
			if err != nil {
				t.Fatalf("build canonical chart: %v", err)
			}
			assertClientDefinitionRoundTrip(t, chart)
			assertClientSnapshotPortable(t, chart)
		})
	}
}

func assertClientDefinitionRoundTrip(t *testing.T, chart *statecharts.Chart) {
	t.Helper()
	wire, err := json.Marshal(chart.Definition())
	if err != nil {
		t.Fatalf("marshal definition: %v", err)
	}
	if !strings.Contains(string(wire), "ai-agent.client.") {
		t.Fatalf("definition does not contain stable client operation names: %s", wire)
	}
	var decoded statecharts.Definition
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("unmarshal definition: %v", err)
	}
	again, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("remarshal definition: %v", err)
	}
	if !bytes.Equal(again, wire) {
		t.Fatalf("definition JSON changed across round trip\nfirst:  %s\nsecond: %s", wire, again)
	}
}

func assertClientSnapshotPortable(t *testing.T, chart *statecharts.Chart) {
	t.Helper()
	block := func(ctx context.Context, _ statecharts.InvokeRequest, _ statecharts.InvokeIO) (statecharts.Value, error) {
		<-ctx.Done()
		return statecharts.NullValue(), ctx.Err()
	}
	factory := func() statecharts.InvokeHandler { return statecharts.InvokeHandlerFunc(block) }
	opts := []statecharts.Option{
		statecharts.WithClock(statecharts.NewManualClock(time.Unix(0, 0))),
		statecharts.WithInvokeHandler(linkInvokeType, factory),
		statecharts.WithInvokeHandler(directoryLinkInvokeType, factory),
		statecharts.WithInvokeHandler(toolInvokeType, factory),
		statecharts.WithInvokeHandler(uiInvokeType, factory),
	}
	instance, err := chart.NewInstance(opts...)
	if err != nil {
		t.Fatalf("new instance: %v", err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := instance.Stop(ctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()
	snapshot, err := instance.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot with populated runtime capability registries: %v", err)
	}
	if _, err := json.Marshal(snapshot); err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
}
