package statecharts

import (
	"context"
	"testing"
)

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

// SCXML 3.11 requires only that a state's 'initial' target be a descendant
// of that state, not a direct child -- entry fills in every intervening
// ancestor. A grandchild target (or deeper) must Build successfully and
// enter correctly.
func TestBuildCompoundInitialCanTargetDeepDescendant(t *testing.T) {
	chart, err := Build(
		Compound("root", "grandchild",
			Children(
				Compound("child", "grandchild",
					Children(
						Atomic("grandchild"),
						Atomic("sibling"),
					),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ip := newInterpretation(chart, nil)
	ip.start()
	got := ip.activeStates()
	if !hasState(got, "child") || !hasState(got, "grandchild") {
		t.Fatalf("initial configuration = %v, want to contain 'child' and 'grandchild'", got)
	}
	if hasState(got, "sibling") {
		t.Fatalf("initial configuration = %v, must not contain 'sibling'", got)
	}
}

func TestBuildValidationErrors(t *testing.T) {
	invoke := Invoke(func(context.Context, any, InvokeIO) (any, error) { return nil, nil })
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
			name: "initial targets a state outside its own subtree",
			spec: Compound("root", "a", Children(
				Compound("a", "b.child", Children(Atomic("a.child"))),
				Compound("b", "b.child", Children(Atomic("b.child"))),
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
		{
			name: "transition targets siblings in one compound state",
			spec: Compound("root", "a", Children(
				Atomic("a", On("go", Target("a", "b"))),
				Atomic("b"),
			)),
		},
		{
			name: "transition targets an ancestor and its descendant",
			spec: Compound("root", "parent", Children(
				Compound("parent", "child", Children(
					Atomic("child", On("go", Target("parent", "child"))),
				)),
			)),
		},
		{
			name: "shallow history default is not an immediate child",
			spec: Compound("root", "parent", Children(
				Compound("parent", "nested", Children(
					Compound("nested", "leaf", Children(Atomic("leaf"))),
					History("history", Shallow, "leaf"),
				)),
			)),
		},
		{
			name: "history default is outside parent subtree",
			spec: Compound("root", "parent", Children(
				Compound("parent", "inside", Children(
					Atomic("inside"),
					History("history", Deep, "outside"),
				)),
				Atomic("outside"),
			)),
		},
		{
			name: "history default cycle",
			spec: Compound("root", "active", Children(
				Atomic("active"),
				History("h1", Shallow, "h2"),
				History("h2", Shallow, "h1"),
			)),
		},
		{
			name: "compound has only history pseudo-state children",
			spec: Compound("root", "history", Children(
				History("history", Shallow, "history"),
			)),
		},
		{
			name: "parallel has final child",
			spec: Compound("root", "parallel", Children(
				Parallel("parallel", Children(Final("done"))),
			)),
		},
		{
			name: "final has transition",
			spec: Compound("root", "done", Children(
				Final("done", On("again", Target("other"))),
				Atomic("other"),
			)),
		},
		{
			name: "final has invoke",
			spec: Compound("root", "done", Children(
				Final("done", invoke),
			)),
		},
		{
			name: "done data is attached to non-final state",
			spec: Atomic("root", WithDone(func(ExecContext) any { return "unused" })),
		},
		{
			name: "transition has no event condition or target",
			spec: Atomic("root", Eventless()),
		},
		{
			name: "invalid state identifier",
			spec: Atomic("bad id"),
		},
		{
			name: "invalid event descriptor",
			spec: Atomic("root", On("bad..event", Target("root"))),
		},
		{
			name: "invalid state kind",
			spec: StateSpec{ID: "root", Kind: StateKind(99)},
		},
		{
			name: "invalid history kind",
			spec: Compound("root", "active", Children(
				Atomic("active"),
				History("history", HistoryKind(99), "active"),
			)),
		},
		{
			name: "duplicate explicit invoke ids",
			spec: Compound("root", "parallel", Children(
				Parallel("parallel", Children(
					Atomic("a", Invoke(func(context.Context, any, InvokeIO) (any, error) { return nil, nil }, WithInvokeID("service"))),
					Atomic("b", Invoke(func(context.Context, any, InvokeIO) (any, error) { return nil, nil }, WithInvokeID("service"))),
				)),
			)),
		},
		{
			name: "compound document root has onentry that would be ignored",
			spec: Compound("root", "active", OnEntry(func(ExecContext) error { return nil }), Children(Atomic("active"))),
		},
		{
			name: "compound document root has onexit that would be ignored",
			spec: Compound("root", "active", OnExit(func(ExecContext) error { return nil }), Children(Atomic("active"))),
		},
		{
			name: "compound document root has invoke that would be ignored",
			spec: Compound("root", "active", invoke, Children(Atomic("active"))),
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

func TestBuildAcceptsLegalTargetsInSeparateParallelRegions(t *testing.T) {
	_, err := Build(
		Compound("root", "parallel",
			Children(
				Parallel("parallel",
					Children(
						Compound("left", "left.a", Children(
							Atomic("left.a", On("go", Target("left.b", "right.b"))),
							Atomic("left.b"),
						)),
						Compound("right", "right.a", Children(
							Atomic("right.a"),
							Atomic("right.b"),
						)),
					),
				),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
}

func TestBuildPreservesDirectActionsAddedAfterBuilderOptions(t *testing.T) {
	noop := func(ExecContext) error { return nil }
	spec := Atomic("root",
		OnEntry(noop),
		On("go", Then(noop)),
		Invoke(func(context.Context, any, InvokeIO) (any, error) { return nil, nil }, WithFinalize(noop)),
	)
	spec.OnEntry = append(spec.OnEntry, noop)
	spec.Transitions[0].Actions = append(spec.Transitions[0].Actions, noop)
	spec.Invokes[0].Finalize = append(spec.Invokes[0].Finalize, noop)

	chart, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	countActions := func(blocks []actionBlock) int {
		count := 0
		for _, block := range blocks {
			count += len(block)
		}
		return count
	}
	if got := countActions(chart.root.onEntry); got != 2 {
		t.Fatalf("compiled onentry action count = %d, want 2", got)
	}
	if got := countActions(chart.root.transitions[0].actions); got != 2 {
		t.Fatalf("compiled transition action count = %d, want 2", got)
	}
	if got := countActions(chart.root.invokes[0].finalize); got != 2 {
		t.Fatalf("compiled finalize action count = %d, want 2", got)
	}
}

func TestChartIDReturnsRootStateID(t *testing.T) {
	chart, err := Build(
		Compound("door", "closed",
			Children(
				Atomic("closed", On("open.request", Target("open"))),
				Atomic("open"),
			),
		),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if chart.ID() != "door" {
		t.Fatalf("ID() = %q, want %q", chart.ID(), "door")
	}
}

func TestChartNewDatamodelRoundTrips(t *testing.T) {
	chart, err := Build(
		Atomic("solo"),
		WithNewDatamodel(func() any { return &Door{OpenCount: 42} }),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	v, ok := chart.NewDatamodel()
	if !ok {
		t.Fatalf("NewDatamodel() ok = false, want true")
	}
	d, ok := v.(*Door)
	if !ok {
		t.Fatalf("NewDatamodel() = %T, want *Door", v)
	}
	if d.OpenCount != 42 {
		t.Fatalf("NewDatamodel().OpenCount = %d, want 42", d.OpenCount)
	}

	// Each call produces a fresh value, not a shared one.
	v2, _ := chart.NewDatamodel()
	d2 := v2.(*Door)
	d2.OpenCount = 99
	if d.OpenCount != 42 {
		t.Fatalf("NewDatamodel() values are not independent: mutating one changed the other")
	}
}

func TestChartWithoutNewDatamodelReportsNotOK(t *testing.T) {
	chart, err := Build(Atomic("solo"))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, ok := chart.NewDatamodel(); ok {
		t.Fatalf("NewDatamodel() ok = true, want false (no WithNewDatamodel given)")
	}
}
