package statecharts

import "testing"

type Door struct {
	OpenCount int
	Locked    bool
}

func TestBuildSimpleChart(t *testing.T) {
	notLocked := Cond(func(d *Door, ec ExecContext) bool {
		return !d.Locked
	})
	recordOpen := Action(func(d *Door, ec ExecContext) error {
		d.OpenCount++
		return nil
	})

	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed",
					On("open.request",
						Target("open"),
						If(notLocked),
						Then(recordOpen),
					),
				),
				Atomic("open",
					On("close.request", Target("closed")),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	got := chart.States()
	want := []Identifier{"door", "closed", "open"}
	if len(got) != len(want) {
		t.Fatalf("States() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("States()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// document order must reflect declaration order, not lexical order.
	if chart.byID["closed"].docOrder >= chart.byID["open"].docOrder {
		t.Fatalf("expected 'closed' to precede 'open' in document order")
	}
}

func TestBuildParallelChart(t *testing.T) {
	chart, err := Build(
		Parallel("machine",
			Children(
				Compound("motor", "off",
					Children(
						Atomic("off", On("motor.start", Target("on"))),
						Atomic("on", On("motor.stop", Target("off"))),
					),
				),
				Compound("light", "dark",
					Children(
						Atomic("dark", On("light.on", Target("lit"))),
						Atomic("lit", On("light.off", Target("dark"))),
					),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if chart.root.kind != KindParallel {
		t.Fatalf("root kind = %v, want KindParallel", chart.root.kind)
	}
	if len(chart.root.children) != 2 {
		t.Fatalf("root has %d children, want 2", len(chart.root.children))
	}
}

func TestBuildHistoryChart(t *testing.T) {
	chart, err := Build(
		Compound("app", "running",
			Children(
				Compound("running", "step1",
					Children(
						Atomic("step1", On("next", Target("step2"))),
						Atomic("step2", On("next", Target("step3"))),
						Atomic("step3"),
						History("running.hist", Shallow, "step1"),
					),
				),
				Atomic("paused",
					On("resume", Target("running.hist")),
				),
			),
			On("pause", Target("paused")),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	hist, ok := chart.byID["running.hist"]
	if !ok {
		t.Fatalf("history state not found")
	}
	if hist.kind != KindHistory || hist.historyKind != Shallow {
		t.Fatalf("history state kind/historyKind mismatch: %v/%v", hist.kind, hist.historyKind)
	}
	if hist.initial != "step1" {
		t.Fatalf("history default target = %q, want step1", hist.initial)
	}
}

func TestBuildValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		spec StateSpec
	}{
		{
			name: "duplicate ids",
			spec: Compound("root", "a", Children(
				Atomic("a"),
				Atomic("a"),
			)),
		},
		{
			name: "compound with no children",
			spec: Compound("root", "a"),
		},
		{
			name: "unresolved initial",
			spec: Compound("root", "missing", Children(Atomic("a"))),
		},
		{
			name: "unresolved transition target",
			spec: Compound("root", "a", Children(
				Atomic("a", On("go", Target("nowhere"))),
			)),
		},
		{
			name: "atomic with children (impossible via constructor, but final with children is)",
			spec: StateSpec{
				ID:   "root",
				Kind: KindFinal,
				Children: []StateSpec{
					Atomic("a"),
				},
			},
		},
		{
			name: "history with unresolved default target",
			spec: Compound("root", "a", Children(
				Atomic("a"),
				History("h", Shallow, "missing"),
			)),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Build(c.spec); err == nil {
				t.Fatalf("Build(%s): expected error, got nil", c.name)
			}
		})
	}
}
