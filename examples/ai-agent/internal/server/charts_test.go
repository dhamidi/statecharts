package server

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

func TestServerChartsRoundTripDefinitionsAndSnapshotWithoutCapabilities(t *testing.T) {
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	requests := NewRequestRegistry()
	requests.lists["held"] = make(chan []protocol.ConversationSummary)
	requests.watches["held"] = make(chan chan protocol.ConversationSummary)
	requests.unwatches["held"] = make(chan protocol.ConversationSummary)
	requests.connections["held"] = make(chan chan sseFrame)
	requests.directory["held"] = []chan protocol.ConversationSummary{make(chan protocol.ConversationSummary)}
	requests.frames["held"] = make(chan sseFrame)

	tests := []struct {
		name  string
		build func() (*statecharts.Chart, error)
	}{
		{"conversation", BuildConversationChart},
		{"fanout", BuildFanoutChart},
		{"toolregistry", func() (*statecharts.Chart, error) { return BuildToolRegistryChart(clock) }},
		{"user", BuildUserChart},
		{"directory", func() (*statecharts.Chart, error) { return BuildDirectoryChart(requests) }},
		{"llmrequest", BuildLLMRequestChart},
		{"connection", func() (*statecharts.Chart, error) { return BuildConnectionChart(requests) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chart, err := test.build()
			if err != nil {
				t.Fatalf("build canonical chart: %v", err)
			}
			assertServerDefinitionRoundTrip(t, chart)
			assertServerSnapshotPortable(t, chart, clock)
		})
	}
}

func assertServerDefinitionRoundTrip(t *testing.T, chart *statecharts.Chart) {
	t.Helper()
	wire, err := json.Marshal(chart.Definition())
	if err != nil {
		t.Fatalf("marshal definition: %v", err)
	}
	if !strings.Contains(string(wire), "ai-agent.server.") {
		t.Fatalf("definition does not contain stable server operation names: %s", wire)
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

func assertServerSnapshotPortable(t *testing.T, chart *statecharts.Chart, clock statecharts.Clock) {
	t.Helper()
	instance, err := chart.NewInstance(statecharts.WithClock(clock))
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
		t.Fatalf("snapshot with populated runtime capability registry: %v", err)
	}
	if _, err := json.Marshal(snapshot); err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
}
