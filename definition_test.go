package statecharts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func definitionString(t *testing.T, value string) Value {
	t.Helper()
	result, err := StringValue(value)
	if err != nil {
		t.Fatalf("StringValue(%q): %v", value, err)
	}
	return result
}

func definitionMap(t *testing.T, values map[string]Value) Value {
	t.Helper()
	result, err := MapValue(values)
	if err != nil {
		t.Fatalf("MapValue: %v", err)
	}
	return result
}

func testExpression(t *testing.T, kind, value string) Expression {
	t.Helper()
	return Expression{Kind: Identifier(kind), Data: definitionString(t, value)}
}

func explicitStateID(id Identifier) StateDefinitionID {
	return StateDefinitionID{Value: id}
}

func completeDefinition(t *testing.T) Definition {
	t.Helper()
	condition := testExpression(t, "go.condition", "ready")
	value := testExpression(t, "go.value", "payload")
	location := testExpression(t, "go.location", "result")
	array := testExpression(t, "go.array", "items")
	staticActions := ExecutableBlock{
		NewRaiseExecutable(RaiseDefinition{Event: "raised", Data: &value}),
		NewSendExecutable(SendDefinition{
			Event: "sent", Target: "peer@node", Type: "custom", ID: "timer",
			Delay: "1s", Content: &value,
		}),
		NewCancelExecutable(CancelDefinition{SendID: "timer"}),
		NewLogExecutable(LogDefinition{Label: "observed", Expr: &value}),
		NewAssignExecutable(AssignDefinition{Location: location, Expr: value}),
		NewChooseExecutable(ChooseDefinition{
			Branches: []ChooseBranchDefinition{{
				Condition: condition,
				Actions:   []ExecutableBlock{{NewScriptExecutable(ScriptDefinition{Expr: value})}},
			}},
			Else: []ExecutableBlock{{NewExtensionExecutable(ExtensionDefinition{
				Namespace: "urn:example", Name: "fallback", Data: definitionString(t, "extension"),
			})}},
		}),
		NewForEachExecutable(ForEachDefinition{
			Array: array, Item: "item", Index: "index",
			Actions: []ExecutableBlock{{NewCallExecutable(CallDefinition{Function: FunctionRef{
				Name: "visit", Version: "v1", Args: []Expression{value},
			}})}},
		}),
		NewScriptExecutable(ScriptDefinition{Expr: value}),
		NewCallExecutable(CallDefinition{Function: FunctionRef{Name: "record", Version: "v1", Args: []Expression{value}}}),
		NewExtensionExecutable(ExtensionDefinition{Namespace: "urn:example", Name: "audit", Data: definitionString(t, "extension")}),
	}
	dynamicEvent := testExpression(t, "go.event", "dynamic")
	dynamicType := testExpression(t, "go.type", "worker")
	dynamicSource := testExpression(t, "go.source", "queue")
	return Definition{
		ID: "demo", Name: "Demo", Datamodel: "go", RevisionSalt: "test-salt",
		Root: StateDefinition{
			ID: explicitStateID("root"), Kind: KindCompound,
			Initial: &TransitionDefinition{Targets: []Identifier{"idle"}},
			Children: []StateDefinition{
				{
					ID: explicitStateID("idle"), Kind: KindAtomic,
					OnEntry: []ExecutableBlock{staticActions, {NewRaiseExecutable(RaiseDefinition{EventExpr: &dynamicEvent})}},
					OnExit:  []ExecutableBlock{{NewLogExecutable(LogDefinition{Label: "exit", Expr: &value})}},
					Transitions: []TransitionDefinition{
						{Events: []Identifier{"start.*"}, Targets: []Identifier{"work"}, Condition: &condition, Actions: []ExecutableBlock{{NewRaiseExecutable(RaiseDefinition{Event: "transitioned"})}}},
						{Targets: []Identifier{"done"}, Condition: &condition, Type: TransitionInternal},
					},
					Invokes: []InvokeDefinition{
						{
							ID: "service", Type: "worker", Src: "queue://jobs", AutoForward: true,
							Params:   []ParamDefinition{{Name: "job", Expr: &value}},
							Finalize: []ExecutableBlock{{NewAssignExecutable(AssignDefinition{Location: location, Expr: value})}},
						},
						{
							ID: "dynamic-service", TypeExpr: &dynamicType, SrcExpr: &dynamicSource,
							Content: &value,
						},
					},
				},
				{
					ID: explicitStateID("work"), Kind: KindParallel,
					Children: []StateDefinition{
						{ID: explicitStateID("region-a"), Kind: KindCompound, Initial: &TransitionDefinition{Targets: []Identifier{"a"}, Actions: []ExecutableBlock{{NewLogExecutable(LogDefinition{Label: "initial", Expr: &value})}}}, Children: []StateDefinition{
							{ID: explicitStateID("a"), Kind: KindAtomic},
							{ID: explicitStateID("a-history"), Kind: KindHistory, History: Shallow, Initial: &TransitionDefinition{Targets: []Identifier{"a"}}},
						}},
						{ID: explicitStateID("region-b"), Kind: KindCompound, Initial: &TransitionDefinition{Targets: []Identifier{"b"}}, Children: []StateDefinition{
							{ID: explicitStateID("b"), Kind: KindAtomic},
						}},
					},
				},
				{ID: explicitStateID("history"), Kind: KindHistory, History: Deep, Initial: &TransitionDefinition{Targets: []Identifier{"idle"}, Actions: []ExecutableBlock{{NewLogExecutable(LogDefinition{Label: "history", Expr: &value})}}}},
				{ID: explicitStateID("done"), Kind: KindFinal, DoneData: &DoneDataDefinition{Content: &value}},
			},
		},
	}
}

func TestDefinitionCloneCoversEveryVariantAndIsIsolated(t *testing.T) {
	original := completeDefinition(t)
	want, err := original.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes original: %v", err)
	}
	clone := original.Clone()
	got, err := clone.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes clone: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("clone canonical bytes differ\noriginal: %q\nclone:    %q", want, got)
	}

	clone.Name = "Changed"
	clone.Root.Children[0].Transitions[0].Targets[0] = "done"
	clone.Root.Children[0].OnEntry[0][0].Raise.Data.Data = definitionString(t, "changed")
	clone.Root.Children[0].Invokes[0].Params[0].Expr.Data = definitionString(t, "changed")
	clone.Root.Children[3].DoneData.Content.Data = definitionString(t, "changed")
	after, err := original.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes after clone mutation: %v", err)
	}
	if !bytes.Equal(after, want) {
		t.Fatal("mutating clone changed source definition")
	}
}

func TestDefinitionPreservesOrderAndExecutableBlockBoundaries(t *testing.T) {
	definition := completeDefinition(t)
	left, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}
	definition.Root.Children[0].OnEntry[0], definition.Root.Children[0].OnEntry[1] = definition.Root.Children[0].OnEntry[1], definition.Root.Children[0].OnEntry[0]
	right, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes reordered: %v", err)
	}
	if bytes.Equal(left, right) {
		t.Fatal("canonical bytes ignored executable block order/boundaries")
	}
}

func TestDefinitionGeneratedIDsAreDeterministicWithoutMutatingInput(t *testing.T) {
	definition := Definition{
		ID: "generated-demo", Datamodel: "go",
		Root: StateDefinition{
			ID: explicitStateID("state.1"), Kind: KindCompound,
			Initial: &TransitionDefinition{Targets: []Identifier{"state.3"}},
			Children: []StateDefinition{
				{ID: StateDefinitionID{Generated: true}, Kind: KindAtomic},
				{ID: explicitStateID("state.2"), Kind: KindAtomic},
			},
		},
	}
	first, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}
	second, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes second: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("generated IDs are not deterministic")
	}
	if definition.Root.Children[0].ID.Value != "" {
		t.Fatalf("canonicalization mutated generated ID to %q", definition.Root.Children[0].ID.Value)
	}

	resolved := definition.Clone()
	resolved.Root.Children[0].ID.Value = "state.3"
	resolvedBytes, err := resolved.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes resolved: %v", err)
	}
	if !bytes.Equal(first, resolvedBytes) {
		t.Fatal("omitted and already-resolved generated IDs canonicalized differently")
	}

	explicit := resolved.Clone()
	explicit.Root.Children[0].ID.Generated = false
	explicitBytes, err := explicit.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes explicit: %v", err)
	}
	if bytes.Equal(first, explicitBytes) {
		t.Fatal("generated and explicit ID intent canonicalized identically")
	}
}

func TestDefinitionImplicitInitialCanonicalizesToFirstChild(t *testing.T) {
	implicit := Definition{
		ID: "implicit-initial", Datamodel: "go",
		Root: StateDefinition{
			ID: explicitStateID("root"), Kind: KindCompound,
			Children: []StateDefinition{
				{ID: explicitStateID("first-history"), Kind: KindHistory, Initial: &TransitionDefinition{Targets: []Identifier{"first"}}},
				{ID: explicitStateID("first"), Kind: KindAtomic},
				{ID: explicitStateID("second"), Kind: KindAtomic},
			},
		},
	}
	if err := implicit.Validate(); err != nil {
		t.Fatalf("implicit first-child initial did not validate: %v", err)
	}
	explicit := implicit.Clone()
	explicit.Root.Initial = &TransitionDefinition{Targets: []Identifier{"first"}}
	implicitBytes, err := implicit.CanonicalBytes()
	if err != nil {
		t.Fatalf("implicit CanonicalBytes: %v", err)
	}
	explicitBytes, err := explicit.CanonicalBytes()
	if err != nil {
		t.Fatalf("explicit CanonicalBytes: %v", err)
	}
	if !bytes.Equal(implicitBytes, explicitBytes) {
		t.Fatal("implicit and explicit first-child initial transitions canonicalized differently")
	}
	if implicit.Root.Initial != nil {
		t.Fatal("validation/canonicalization mutated caller-owned implicit initial")
	}
}

func TestDefinitionPreservesExistingAtomicAndFinalRootSemantics(t *testing.T) {
	atomic := Definition{
		ID: "atomic-root", Datamodel: "go",
		Root: StateDefinition{
			ID: explicitStateID("ready"), Kind: KindAtomic,
			OnEntry: []ExecutableBlock{{NewRaiseExecutable(RaiseDefinition{Event: "started"})}},
			Invokes: []InvokeDefinition{{ID: "worker", Type: "worker", Src: "queue:work"}},
		},
	}
	if err := atomic.Validate(); err != nil {
		t.Fatalf("atomic root content rejected: %v", err)
	}
	result := testExpression(t, "literal", "done")
	final := Definition{
		ID: "final-root", Datamodel: "go",
		Root: StateDefinition{
			ID: explicitStateID("done"), Kind: KindFinal,
			DoneData: &DoneDataDefinition{Content: &result},
		},
	}
	if err := final.Validate(); err != nil {
		t.Fatalf("final root rejected: %v", err)
	}
}

func TestDefinitionCanonicalBytesAreDeterministicForValueMaps(t *testing.T) {
	left := completeDefinition(t)
	right := completeDefinition(t)
	leftValue := definitionMap(t, map[string]Value{"z": Int64Value(1), "a": definitionString(t, "first")})
	rightValue := definitionMap(t, map[string]Value{"a": definitionString(t, "first"), "z": Int64Value(1)})
	left.Root.Children[0].OnEntry[0][0].Raise.Data.Data = leftValue
	right.Root.Children[0].OnEntry[0][0].Raise.Data.Data = rightValue
	leftBytes, err := left.CanonicalBytes()
	if err != nil {
		t.Fatalf("left CanonicalBytes: %v", err)
	}
	rightBytes, err := right.CanonicalBytes()
	if err != nil {
		t.Fatalf("right CanonicalBytes: %v", err)
	}
	if !bytes.Equal(leftBytes, rightBytes) {
		t.Fatal("canonical definition bytes depend on Value map insertion order")
	}
	if !bytes.HasPrefix(leftBytes, []byte(DefinitionCanonicalMagic)) {
		t.Fatalf("canonical bytes lack versioned magic prefix: %q", leftBytes)
	}
}

func TestDefinitionCanonicalFormatVersionIsPinned(t *testing.T) {
	definition := completeDefinition(t)
	initializer := testExpression(t, "go.value", "initial")
	definition.DataBinding = DataBindingLate
	definition.Data = []DataDefinition{{ID: "global", Expr: &initializer}}
	definition.Root.Children[0].Data = []DataDefinition{{ID: "local", Source: "embed:local.json"}}
	canonical, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}
	digest := sha256.Sum256(canonical)
	got := hex.EncodeToString(digest[:])
	const want = "338d917d5668ddc4c18c89986d51357710cdbf41835cc6fb5ba52710f81b5deb"
	if got != want {
		t.Fatalf("canonical format changed: sha256 = %s, want %s; bump DefinitionCanonicalVersion before intentionally changing the format", got, want)
	}
}

func TestDefinitionValidationRejectsMalformedStructureWithPaths(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Definition)
		path string
	}{
		{"duplicate state ID", func(d *Definition) { d.Root.Children[1].ID = explicitStateID("idle") }, "root.children[1].id"},
		{"unresolved target", func(d *Definition) { d.Root.Children[0].Transitions[0].Targets[0] = "missing" }, "root.children[0].transitions[0].targets[0]"},
		{"invalid root content", func(d *Definition) {
			d.Root.OnEntry = []ExecutableBlock{{NewRaiseExecutable(RaiseDefinition{Event: "bad"})}}
		}, "root.onEntry"},
		{"done data on non-final", func(d *Definition) { d.Root.Children[0].DoneData = &DoneDataDefinition{} }, "root.children[0].doneData"},
		{"duplicate invoke ID", func(d *Definition) { d.Root.Children[0].Invokes[1].ID = "service" }, "root.children[0].invokes[1].id"},
		{"invoke params and content", func(d *Definition) {
			expr := *d.Root.Children[0].Invokes[0].Params[0].Expr
			d.Root.Children[0].Invokes[0].Content = &expr
		}, "root.children[0].invokes[0].content"},
		{"invoke src and srcexpr", func(d *Definition) {
			expr := testExpression(t, "go.source", "dynamic")
			d.Root.Children[0].Invokes[0].SrcExpr = &expr
		}, "root.children[0].invokes[0].srcExpr"},
		{"malformed executable union", func(d *Definition) { d.Root.Children[0].OnEntry[0][0].Send = &SendDefinition{Event: "also-set"} }, "root.children[0].onEntry[0][0]"},
		{"empty executable block", func(d *Definition) { d.Root.Children[0].OnEntry = []ExecutableBlock{nil} }, "root.children[0].onEntry[0]"},
		{"choose without branch", func(d *Definition) {
			d.Root.Children[0].OnEntry = []ExecutableBlock{{NewChooseExecutable(ChooseDefinition{})}}
		}, "root.children[0].onEntry[0][0].choose.branches"},
		{"forbidden finalize send", func(d *Definition) {
			condition := testExpression(t, "go.condition", "false")
			d.Root.Children[0].Invokes[0].Finalize[0] = ExecutableBlock{NewChooseExecutable(ChooseDefinition{
				Branches: []ChooseBranchDefinition{{Condition: condition}},
				Else:     []ExecutableBlock{{NewSendExecutable(SendDefinition{Event: "bad"})}},
			})}
		}, "root.children[0].invokes[0].finalize[0][0].choose.else[0][0]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := completeDefinition(t)
			test.edit(&definition)
			err := definition.Validate()
			if err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("Validate error = %v, want path containing %q", err, test.path)
			}
		})
	}
}

func TestDefinitionValidationKeepsInvokeContentAndSourceModesDistinct(t *testing.T) {
	definition := completeDefinition(t)
	static := definition.Root.Children[0].Invokes[0]
	dynamic := definition.Root.Children[0].Invokes[1]
	if static.Content != nil || len(static.Params) != 1 {
		t.Fatalf("static invoke lost params/content distinction: %+v", static)
	}
	if dynamic.Src != "" || dynamic.SrcExpr == nil || dynamic.Content == nil {
		t.Fatalf("dynamic invoke lost static/dynamic source or whole content: %+v", dynamic)
	}
	if err := definition.Validate(); err != nil {
		t.Fatalf("Validate complete definition: %v", err)
	}
}

func TestDefinitionValidationRejectsInvalidUTF8AndNonPlainStaticIdentifiers(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Definition)
		path string
	}{
		{"invalid UTF-8 name", func(d *Definition) { d.Name = string([]byte{0xff}) }, "definition.name"},
		{"invalid UTF-8 source", func(d *Definition) { d.Root.Children[0].Invokes[0].Src = string([]byte{0xfe}) }, "root.children[0].invokes[0].src"},
		{"raise descriptor", func(d *Definition) { d.Root.Children[0].OnEntry[0][0].Raise.Event = "error.*" }, "raise.event"},
		{"send ID descriptor", func(d *Definition) { d.Root.Children[0].OnEntry[0][1].Send.ID = "timer.*" }, "send.id"},
		{"cancel ID descriptor", func(d *Definition) { d.Root.Children[0].OnEntry[0][2].Cancel.SendID = "#timer" }, "cancel.sendID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := completeDefinition(t)
			test.edit(&definition)
			if _, err := definition.CanonicalBytes(); err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("CanonicalBytes error = %v, want path containing %q", err, test.path)
			}
		})
	}

	definition := completeDefinition(t)
	definition.Root.Children[0].Transitions[0].Events = []Identifier{"*", "error.*", "error."}
	if err := definition.Validate(); err != nil {
		t.Fatalf("transition event descriptors should remain valid: %v", err)
	}
}

func TestDefinitionRepresentsOrderedDataDeclarations(t *testing.T) {
	initial := testExpression(t, "literal", "initial")
	inline := testExpression(t, "literal", "inline")
	local := testExpression(t, "literal", "local")
	definition := completeDefinition(t)
	definition.DataBinding = DataBindingLate
	definition.Data = []DataDefinition{
		{ID: "from-expression", Expr: &initial},
		{ID: "from-source", Source: "embed:defaults.json"},
		{ID: "from-content", Content: &inline},
		{ID: "empty"},
	}
	definition.Root.Children[0].Data = []DataDefinition{{ID: "local", Expr: &local}}
	if err := definition.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	clone := definition.Clone()
	clone.Data[0].Expr.Data = testStringValue("changed")
	clone.Root.Children[0].Data[0].Expr.Data = testStringValue("changed-local")
	if got, _ := definition.Data[0].Expr.Data.AsString(); got != "initial" {
		t.Fatalf("source top-level data expression mutated through clone: %q", got)
	}
	if got, _ := definition.Root.Children[0].Data[0].Expr.Data.AsString(); got != "local" {
		t.Fatalf("source state data expression mutated through clone: %q", got)
	}
	first, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}
	second, err := definition.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes repeat: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("data declaration canonical bytes are not deterministic")
	}
}

func TestDefinitionValidationRejectsMalformedDataDeclarations(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Definition)
		path string
	}{
		{
			name: "invalid binding",
			edit: func(definition *Definition) { definition.DataBinding = "sometimes" },
			path: "definition.dataBinding",
		},
		{
			name: "duplicate ID across scopes",
			edit: func(definition *Definition) {
				definition.Data = []DataDefinition{{ID: "item"}}
				definition.Root.Children[0].Data = []DataDefinition{{ID: "item"}}
			},
			path: "root.children[0].data[0].id",
		},
		{
			name: "multiple initializers",
			edit: func(definition *Definition) {
				expr := testExpression(t, "literal", "value")
				definition.Data = []DataDefinition{{ID: "item", Source: "embed:item", Expr: &expr}}
			},
			path: "definition.data[0]",
		},
		{
			name: "data on final state",
			edit: func(definition *Definition) {
				definition.Root.Children[3].Data = []DataDefinition{{ID: "item"}}
			},
			path: "root.children[3].data",
		},
		{
			name: "inactive history kind",
			edit: func(definition *Definition) { definition.Root.Children[0].History = Deep },
			path: "root.children[0].history",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := completeDefinition(t)
			test.edit(&definition)
			err := definition.Validate()
			if err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("Validate error = %v, want path containing %q", err, test.path)
			}
		})
	}
}
