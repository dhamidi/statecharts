package statecharts

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

const (
	goLiteralKind   Identifier = "go.literal"
	goDataKind      Identifier = "go.data"
	goConditionKind Identifier = "go.condition"
	goScriptKind    Identifier = "go.script"
	goValueKind     Identifier = "go.value"
	goLocationKind  Identifier = "go.location"
)

// GoCondition is a typed host condition. Args are the evaluated expressions
// attached to the reference at its use site.
type GoCondition[D any] func(*D, ExecContext, []Value) (bool, error)

// GoAction is a typed host action used by both script expressions and named
// call nodes.
type GoAction[D any] func(*D, ExecContext, []Value) error

// GoValue is a typed host value producer or location reader.
type GoValue[D any] func(*D, ExecContext, []Value) (Value, error)

// GoLocation writes a canonical value to a typed host location.
type GoLocation[D any] func(*D, ExecContext, Value, []Value) error

// GoSnapshotCodec controls the D portion of a session snapshot. Decode must
// construct a replacement value. The default is encoding/json. Applications
// whose D has a non-deterministic MarshalJSON must provide a deterministic
// codec because snapshot and revision reproducibility otherwise cannot be
// guaranteed.
type GoSnapshotCodec[D any] struct {
	Encode func(*D) ([]byte, error)
	Decode func([]byte) (*D, error)
}

// GoModelOption configures a GoModel.
type GoModelOption[D any] func(*GoModel[D])

// WithGoSnapshotCodec replaces the default encoding/json codec for D.
func WithGoSnapshotCodec[D any](codec GoSnapshotCodec[D]) GoModelOption[D] {
	return func(model *GoModel[D]) { model.codec = codec }
}

type goRegistration[D any] struct {
	kind Identifier
	cond GoCondition[D]
	act  GoAction[D]
	get  GoValue[D]
	set  GoLocation[D]
}

// GoModel is the default datamodel. It combines a typed D factory with a
// model-local registry of stable, named host behavior.
type GoModel[D any] struct {
	mu            sync.RWMutex
	factory       func() *D
	codec         GoSnapshotCodec[D]
	registrations map[string]goRegistration[D]
}

// NewGoModel creates an independently scoped Go datamodel registry.
func NewGoModel[D any](factory func() *D, options ...GoModelOption[D]) *GoModel[D] {
	m := &GoModel[D]{factory: factory, registrations: map[string]goRegistration[D]{}}
	m.codec = GoSnapshotCodec[D]{
		Encode: func(d *D) ([]byte, error) { return json.Marshal(d) },
		Decode: func(b []byte) (*D, error) {
			var d D
			if err := json.Unmarshal(b, &d); err != nil {
				return nil, err
			}
			return &d, nil
		},
	}
	for _, option := range options {
		if option != nil {
			option(m)
		}
	}
	return m
}

func (*GoModel[D]) Name() Identifier                { return "go" }
func registrationKey(n Identifier, v string) string { return string(n) + "\x00" + v }

// GoConditionRef creates serializable condition expressions without repeating
// the registered behavior's name or version.
type GoConditionRef struct {
	name    Identifier
	version string
}

// GoActionRef creates serializable script expressions and call references.
type GoActionRef struct {
	name    Identifier
	version string
}

// GoValueRef creates serializable value expressions.
type GoValueRef struct {
	name    Identifier
	version string
}

// GoLocationRef creates serializable readable and writable location expressions.
type GoLocationRef struct {
	name    Identifier
	version string
}

func refExpression(kind, name Identifier, version string, args []Expression) Expression {
	v, _ := encodeGoRef(FunctionRef{Name: name, Version: version, Args: cloneExpressions(args)})
	return Expression{Kind: kind, Data: v}
}
func (r GoConditionRef) Expression(args ...Expression) Expression {
	return refExpression(goConditionKind, r.name, r.version, args)
}
func (r GoActionRef) Expression(args ...Expression) Expression {
	return refExpression(goScriptKind, r.name, r.version, args)
}
func (r GoActionRef) Function(args ...Expression) FunctionRef {
	return FunctionRef{Name: r.name, Version: r.version, Args: cloneExpressions(args)}
}
func (r GoValueRef) Expression(args ...Expression) Expression {
	return refExpression(goValueKind, r.name, r.version, args)
}
func (r GoLocationRef) Expression(args ...Expression) Expression {
	return refExpression(goLocationKind, r.name, r.version, args)
}

// If returns this registered condition as a serializable expression.
func (r GoConditionRef) If(args ...Expression) Expression { return r.Expression(args...) }

// Do returns this registered action as a serializable executable node.
func (r GoActionRef) Do(args ...Expression) Executable {
	return NewScriptExecutable(ScriptDefinition{Expr: r.Expression(args...)})
}

// Get returns this registered value computation as a serializable expression.
func (r GoValueRef) Get(args ...Expression) Expression { return r.Expression(args...) }

// At returns this registered readable/writable location as a serializable expression.
func (r GoLocationRef) At(args ...Expression) Expression { return r.Expression(args...) }

func (m *GoModel[D]) register(n Identifier, v string, r goRegistration[D]) error {
	if err := validatePlainIdentifier(n); err != nil {
		return fmt.Errorf("statecharts: invalid Go function name %q: %w", n, err)
	}
	if v == "" {
		return fmt.Errorf("statecharts: Go function %q requires a non-empty version", n)
	}
	if err := validateUTF8(v); err != nil {
		return fmt.Errorf("statecharts: invalid version for Go function %q: %w", n, err)
	}
	if (r.kind == goConditionKind && r.cond == nil) || (r.kind == goScriptKind && r.act == nil) || (r.kind == goValueKind && r.get == nil) || (r.kind == goLocationKind && (r.get == nil || r.set == nil)) {
		return fmt.Errorf("statecharts: nil Go callback for %q version %q", n, v)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := registrationKey(n, v)
	if _, ok := m.registrations[k]; ok {
		return fmt.Errorf("statecharts: duplicate Go function %q version %q", n, v)
	}
	m.registrations[k] = r
	return nil
}

// Condition registers one condition implementation and returns its reusable reference.
func (m *GoModel[D]) Condition(n Identifier, v string, fn GoCondition[D]) (GoConditionRef, error) {
	r := GoConditionRef{n, v}
	return r, m.register(n, v, goRegistration[D]{kind: goConditionKind, cond: fn})
}

// Script registers one action implementation and returns its reusable reference.
func (m *GoModel[D]) Script(n Identifier, v string, fn GoAction[D]) (GoActionRef, error) {
	r := GoActionRef{n, v}
	return r, m.register(n, v, goRegistration[D]{kind: goScriptKind, act: fn})
}

// Action is an alias for Script; its handle supports both Script expressions and Call references.
func (m *GoModel[D]) Action(n Identifier, v string, fn GoAction[D]) (GoActionRef, error) {
	return m.Script(n, v, fn)
}

// Value registers one value producer and returns its reusable reference.
func (m *GoModel[D]) Value(n Identifier, v string, fn GoValue[D]) (GoValueRef, error) {
	r := GoValueRef{n, v}
	return r, m.register(n, v, goRegistration[D]{kind: goValueKind, get: fn})
}

// Location registers a reusable readable and writable host location.
func (m *GoModel[D]) Location(n Identifier, v string, get GoValue[D], set GoLocation[D]) (GoLocationRef, error) {
	r := GoLocationRef{n, v}
	return r, m.register(n, v, goRegistration[D]{kind: goLocationKind, get: get, set: set})
}

// GoLiteral returns a canonical literal expression.
func GoLiteral(v Value) Expression { return Expression{Kind: goLiteralKind, Data: v.Clone()} }

// GoData returns a readable and writable expression for a declared data ID.
func GoData(id Identifier) Expression {
	s, _ := StringValue(string(id))
	return Expression{Kind: goDataKind, Data: s}
}

type goCompiled[D any] struct {
	owner   *goProgramOwner
	kind    Identifier
	reg     goRegistration[D]
	args    []*goCompiled[D]
	literal Value
	dataID  Identifier
}

type goProgramOwner struct{ marker byte }

type goProgram[D any] struct {
	owner         *goProgramOwner
	factory       func() *D
	codec         GoSnapshotCodec[D]
	fingerprint   []byte
	compiled      map[string]*goCompiled[D]
	functions     map[string]*goCompiled[D]
	declarations  []Identifier
	dataLocations map[Identifier]*goCompiled[D]
}

type goCompile[D any] struct {
	registry     map[string]goRegistration[D]
	declarations map[Identifier]bool
	used         map[string]Identifier
	program      *goProgram[D]
}
type goCategory byte

const (
	catValue goCategory = iota
	catBoolean
	catLocation
	catReadableLocation
	catScript
)

func (m *GoModel[D]) Compile(def *Definition) (DatamodelProgram, error) {
	if def == nil {
		return nil, fmt.Errorf("statecharts: nil definition")
	}
	if err := def.Validate(); err != nil {
		return nil, err
	}
	if def.Datamodel != m.Name() {
		return nil, fmt.Errorf("statecharts: definition datamodel %q is not %q", def.Datamodel, m.Name())
	}
	m.mu.RLock()
	registry := make(map[string]goRegistration[D], len(m.registrations))
	for k, v := range m.registrations {
		registry[k] = v
	}
	factory, codec := m.factory, m.codec
	m.mu.RUnlock()
	if factory == nil || codec.Encode == nil || codec.Decode == nil {
		return nil, fmt.Errorf("statecharts: Go model requires non-nil factory and snapshot codec")
	}
	p := &goProgram[D]{
		owner:   &goProgramOwner{},
		factory: factory, codec: codec,
		compiled:      make(map[string]*goCompiled[D]),
		functions:     make(map[string]*goCompiled[D]),
		dataLocations: make(map[Identifier]*goCompiled[D]),
	}
	c := goCompile[D]{registry: registry, declarations: map[Identifier]bool{}, used: map[string]Identifier{}, program: p}
	collectDataIDs(def.Data, &c)
	collectStateDataIDs(&def.Root, &c)
	if err := c.definition(def); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(c.used))
	for k, kind := range c.used {
		keys = append(keys, k+"\x00"+string(kind))
	}
	sort.Strings(keys)
	h := sha256.New()
	h.Write([]byte("statecharts/go-model/v2\x00"))
	for _, k := range keys {
		fmt.Fprintf(h, "%d:", len(k))
		h.Write([]byte(k))
	}
	p.fingerprint = h.Sum(nil)
	return p, nil
}
func collectDataIDs(ds []DataDefinition, c interface{ add(Identifier) }) {
	for _, d := range ds {
		c.add(d.ID)
	}
}
func (c *goCompile[D]) add(id Identifier) {
	if !c.declarations[id] {
		c.declarations[id] = true
		c.program.declarations = append(c.program.declarations, id)
		c.program.dataLocations[id] = &goCompiled[D]{owner: c.program.owner, kind: goDataKind, dataID: id}
	}
}
func collectStateDataIDs[D any](s *StateDefinition, c *goCompile[D]) {
	for _, d := range s.Data {
		c.add(d.ID)
	}
	for i := range s.Children {
		collectStateDataIDs(&s.Children[i], c)
	}
}

func (c *goCompile[D]) expression(e Expression, cat goCategory) (*goCompiled[D], error) {
	x, err := c.raw(e)
	if err != nil {
		return nil, err
	}
	valid := false
	switch cat {
	case catValue:
		valid = x.kind == goLiteralKind || x.kind == goDataKind || x.kind == goValueKind || x.kind == goLocationKind
	case catBoolean:
		valid = x.kind == goConditionKind
	case catLocation:
		valid = x.kind == goDataKind || x.kind == goLocationKind
	case catReadableLocation:
		valid = x.kind == goDataKind || x.kind == goLocationKind
	case catScript:
		valid = x.kind == goScriptKind
	}
	if !valid {
		return nil, fmt.Errorf("statecharts: Go expression kind %q is invalid in requested context", x.kind)
	}
	c.program.compiled[valueKeyInternal(e)] = x
	return x, nil
}
func (c *goCompile[D]) raw(e Expression) (*goCompiled[D], error) {
	if e.Kind == goLiteralKind {
		return &goCompiled[D]{owner: c.program.owner, kind: e.Kind, literal: e.Data.Clone()}, nil
	}
	if e.Kind == goDataKind {
		id, ok := e.Data.AsString()
		if !ok || !c.declarations[Identifier(id)] {
			return nil, fmt.Errorf("statecharts: unknown Go data ID %q", id)
		}
		return &goCompiled[D]{owner: c.program.owner, kind: e.Kind, dataID: Identifier(id)}, nil
	}
	ref, err := decodeGoRef(e.Data)
	if err != nil {
		return nil, fmt.Errorf("statecharts: compile %q: %w", e.Kind, err)
	}
	r, ok := c.registry[registrationKey(ref.Name, ref.Version)]
	if !ok {
		return nil, fmt.Errorf("statecharts: unknown Go function %q version %q", ref.Name, ref.Version)
	}
	if r.kind != e.Kind {
		return nil, fmt.Errorf("statecharts: Go function %q version %q has wrong kind %q, want %q", ref.Name, ref.Version, r.kind, e.Kind)
	}
	if (r.kind == goConditionKind && r.cond == nil) || (r.kind == goScriptKind && r.act == nil) || (r.kind == goValueKind && r.get == nil) || (r.kind == goLocationKind && (r.get == nil || r.set == nil)) {
		return nil, fmt.Errorf("statecharts: Go function %q has nil callback", ref.Name)
	}
	x := &goCompiled[D]{owner: c.program.owner, kind: e.Kind, reg: r, args: make([]*goCompiled[D], len(ref.Args))}
	c.used[registrationKey(ref.Name, ref.Version)] = r.kind
	for i, a := range ref.Args {
		x.args[i], err = c.expression(a, catValue)
		if err != nil {
			return nil, fmt.Errorf("statecharts: argument %d: %w", i, err)
		}
	}
	return x, nil
}

func (c *goCompile[D]) data(ds []DataDefinition) error {
	for _, d := range ds {
		if d.Source != "" {
			return fmt.Errorf("statecharts: Go data source %q is unsupported", d.Source)
		}
		expression := d.Expr
		if expression == nil {
			expression = d.Content
		}
		if expression != nil {
			if _, err := c.expression(*expression, catValue); err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *goCompile[D]) definition(d *Definition) error {
	if err := c.data(d.Data); err != nil {
		return err
	}
	return c.state(&d.Root)
}
func (c *goCompile[D]) state(s *StateDefinition) error {
	if err := c.data(s.Data); err != nil {
		return err
	}
	if s.Initial != nil {
		if err := c.transition(s.Initial); err != nil {
			return err
		}
	}
	for _, b := range append(append([]ExecutableBlock{}, s.OnEntry...), s.OnExit...) {
		if err := c.blocks([]ExecutableBlock{b}); err != nil {
			return err
		}
	}
	for i := range s.Transitions {
		if err := c.transition(&s.Transitions[i]); err != nil {
			return err
		}
	}
	for i := range s.Invokes {
		v := &s.Invokes[i]
		if err := c.valueExpressions(v.TypeExpr, v.SrcExpr, v.Content); err != nil {
			return err
		}
		if err := c.locationExpression(v.IDLocation); err != nil {
			return err
		}
		if err := c.params(v.Params); err != nil {
			return err
		}
		if err := c.blocks(v.Finalize); err != nil {
			return err
		}
	}
	if s.DoneData != nil {
		if err := c.params(s.DoneData.Params); err != nil {
			return err
		}
		if err := c.valueExpressions(s.DoneData.Content); err != nil {
			return err
		}
	}
	for i := range s.Children {
		if err := c.state(&s.Children[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *goCompile[D]) valueExpressions(expressions ...*Expression) error {
	for _, expression := range expressions {
		if expression != nil {
			if _, err := c.expression(*expression, catValue); err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *goCompile[D]) locationExpression(expression *Expression) error {
	if expression != nil {
		_, err := c.expression(*expression, catLocation)
		return err
	}
	return nil
}
func (c *goCompile[D]) params(params []ParamDefinition) error {
	for i := range params {
		if err := c.valueExpressions(params[i].Expr); err != nil {
			return err
		}
		if params[i].Location != nil {
			if _, err := c.expression(*params[i].Location, catReadableLocation); err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *goCompile[D]) transition(t *TransitionDefinition) error {
	if t.Condition != nil {
		if _, err := c.expression(*t.Condition, catBoolean); err != nil {
			return err
		}
	}
	return c.blocks(t.Actions)
}
func (c *goCompile[D]) blocks(bs []ExecutableBlock) error {
	for _, b := range bs {
		for i := range b {
			e := &b[i]
			switch e.Kind {
			case ExecutableRaise:
				if err := c.valueExpressions(e.Raise.EventExpr, e.Raise.Data); err != nil {
					return err
				}
			case ExecutableSend:
				s := e.Send
				if err := c.valueExpressions(s.EventExpr, s.TargetExpr, s.TypeExpr, s.DelayExpr, s.Content); err != nil {
					return err
				}
				if err := c.locationExpression(s.IDLocation); err != nil {
					return err
				}
				if err := c.params(s.Params); err != nil {
					return err
				}
			case ExecutableCancel:
				if err := c.valueExpressions(e.Cancel.SendIDExpr); err != nil {
					return err
				}
			case ExecutableLog:
				if err := c.valueExpressions(e.Log.LabelExpr, e.Log.Expr); err != nil {
					return err
				}
			case ExecutableAssign:
				if _, err := c.expression(e.Assign.Location, catLocation); err != nil {
					return err
				}
				if _, err := c.expression(e.Assign.Expr, catValue); err != nil {
					return err
				}
			case ExecutableScript:
				if _, err := c.expression(e.Script.Expr, catScript); err != nil {
					return err
				}
			case ExecutableCall:
				v, _ := encodeGoRef(e.Call.Function)
				compiled, err := c.expression(Expression{Kind: goScriptKind, Data: v}, catScript)
				if err != nil {
					return err
				}
				c.program.functions[functionKeyInternal(e.Call.Function)] = compiled
			case ExecutableForEach:
				if _, err := c.expression(e.ForEach.Array, catValue); err != nil {
					return err
				}
				if !c.declarations[e.ForEach.Item] {
					return fmt.Errorf("statecharts: unknown foreach item data ID %q", e.ForEach.Item)
				}
				if _, err := c.expression(GoData(e.ForEach.Item), catLocation); err != nil {
					return err
				}
				if e.ForEach.Index != "" && !c.declarations[e.ForEach.Index] {
					return fmt.Errorf("statecharts: unknown foreach index data ID %q", e.ForEach.Index)
				}
				if e.ForEach.Index != "" {
					if _, err := c.expression(GoData(e.ForEach.Index), catLocation); err != nil {
						return err
					}
				}
				if err := c.blocks(e.ForEach.Actions); err != nil {
					return err
				}
			case ExecutableChoose:
				for _, b := range e.Choose.Branches {
					if _, err := c.expression(b.Condition, catBoolean); err != nil {
						return err
					}
					if err := c.blocks(b.Actions); err != nil {
						return err
					}
				}
				if err := c.blocks(e.Choose.Else); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (p *goProgram[D]) Fingerprint() []byte { return append([]byte(nil), p.fingerprint...) }

func (p *goProgram[D]) ResolveExpression(expression Expression) (CompiledExpression, error) {
	compiled := p.compiled[valueKeyInternal(expression)]
	if compiled == nil {
		return nil, fmt.Errorf("statecharts: expression was not compiled by this Go program")
	}
	return compiled, nil
}

func (p *goProgram[D]) ResolveFunction(function FunctionRef) (CompiledExpression, error) {
	compiled := p.functions[functionKeyInternal(function)]
	if compiled == nil {
		return nil, fmt.Errorf("statecharts: function reference was not compiled by this Go program")
	}
	return compiled, nil
}

func (p *goProgram[D]) ResolveDataLocation(id Identifier) (CompiledExpression, error) {
	compiled := p.dataLocations[id]
	if compiled == nil {
		return nil, fmt.Errorf("statecharts: data ID %q was not declared in this Go program", id)
	}
	return compiled, nil
}

func (p *goProgram[D]) NewSession(SessionOptions) (_ DatamodelSession, err error) {
	defer recoverGo(&err)
	d := p.factory()
	if d == nil {
		return nil, fmt.Errorf("statecharts: Go model factory returned nil")
	}
	s := &goSession[D]{owner: p.owner, data: d, codec: p.codec, values: map[Identifier]Value{}}
	for _, id := range p.declarations {
		s.values[id] = NullValue()
	}
	return s, nil
}

type goSession[D any] struct {
	owner  *goProgramOwner
	data   *D
	codec  GoSnapshotCodec[D]
	values map[Identifier]Value
}

func recoverGo(errp *error) {
	if v := recover(); v != nil {
		*errp = fmt.Errorf("statecharts: Go callback panicked: %v", v)
	}
}

func recoverGoSnapshotCodec(errp *error) {
	if value := recover(); value != nil {
		*errp = fmt.Errorf("statecharts: Go snapshot codec panicked: %v", value)
	}
}
func asCompiled[D any](e CompiledExpression, owner *goProgramOwner) (*goCompiled[D], error) {
	x, ok := e.(*goCompiled[D])
	if !ok || x == nil {
		return nil, fmt.Errorf("statecharts: compiled Go expression type mismatch")
	}
	if x.owner != owner {
		return nil, fmt.Errorf("statecharts: compiled Go expression belongs to another program")
	}
	return x, nil
}
func (s *goSession[D]) arguments(ec ExecContext, x *goCompiled[D]) ([]Value, error) {
	a := make([]Value, len(x.args))
	for i := range x.args {
		v, e := s.eval(ec, x.args[i])
		if e != nil {
			return nil, e
		}
		a[i] = v
	}
	return a, nil
}
func (s *goSession[D]) eval(ec ExecContext, x *goCompiled[D]) (Value, error) {
	switch x.kind {
	case goLiteralKind:
		return x.literal.Clone(), nil
	case goDataKind:
		return s.values[x.dataID].Clone(), nil
	case goValueKind, goLocationKind:
		a, e := s.arguments(ec, x)
		if e != nil {
			return Value{}, e
		}
		return x.reg.get(s.data, ec, a)
	default:
		return Value{}, fmt.Errorf("statecharts: expression kind %q is not readable", x.kind)
	}
}
func (s *goSession[D]) EvaluateBoolean(ec ExecContext, e CompiledExpression) (result bool, err error) {
	defer recoverGo(&err)
	x, err := asCompiled[D](e, s.owner)
	if err != nil {
		return false, err
	}
	if x.kind != goConditionKind {
		return false, fmt.Errorf("statecharts: boolean expression has kind %q", x.kind)
	}
	a, err := s.arguments(ec, x)
	if err != nil {
		return false, err
	}
	return x.reg.cond(s.data, ec, a)
}
func (s *goSession[D]) EvaluateValue(ec ExecContext, e CompiledExpression) (result Value, err error) {
	defer recoverGo(&err)
	x, err := asCompiled[D](e, s.owner)
	if err != nil {
		return Value{}, err
	}
	v, err := s.eval(ec, x)
	return v.Clone(), err
}
func (s *goSession[D]) Assign(ec ExecContext, e CompiledExpression, v Value) (err error) {
	defer recoverGo(&err)
	x, err := asCompiled[D](e, s.owner)
	if err != nil {
		return err
	}
	if x.kind == goDataKind {
		s.values[x.dataID] = v.Clone()
		return nil
	}
	if x.kind != goLocationKind {
		return fmt.Errorf("statecharts: location expression type mismatch")
	}
	a, err := s.arguments(ec, x)
	if err != nil {
		return err
	}
	return x.reg.set(s.data, ec, v.Clone(), a)
}
func (s *goSession[D]) Execute(ec ExecContext, e CompiledExpression) (err error) {
	defer recoverGo(&err)
	x, err := asCompiled[D](e, s.owner)
	if err != nil {
		return err
	}
	if x.kind != goScriptKind {
		return fmt.Errorf("statecharts: script expression has kind %q", x.kind)
	}
	a, err := s.arguments(ec, x)
	if err != nil {
		return err
	}
	return x.reg.act(s.data, ec, a)
}
func (s *goSession[D]) ForEach(ec ExecContext, e CompiledExpression, b IterationBindings, body func() error) (err error) {
	defer recoverGo(&err)
	if body == nil {
		return fmt.Errorf("statecharts: nil foreach body")
	}
	x, err := asCompiled[D](e, s.owner)
	if err != nil {
		return err
	}
	v, err := s.eval(ec, x)
	if err != nil {
		return err
	}
	list, ok := v.AsList()
	if !ok {
		return fmt.Errorf("statecharts: foreach array is %s, want list", v.Kind())
	}
	for i, item := range list {
		if err := s.Assign(ec, b.Item, item); err != nil {
			return err
		}
		if b.Index != nil {
			if err := s.Assign(ec, b.Index, Int64Value(int64(i))); err != nil {
				return err
			}
		}
		if err := body(); err != nil {
			return err
		}
	}
	return nil
}

type goSnapshot struct {
	Version int                  `json:"version"`
	Data    []byte               `json:"data"`
	Values  map[Identifier]Value `json:"values"`
}

func (s *goSession[D]) EncodeSnapshot() (_ []byte, err error) {
	defer recoverGoSnapshotCodec(&err)
	d, err := s.codec.Encode(s.data)
	if err != nil {
		return nil, err
	}
	vals := make(map[Identifier]Value, len(s.values))
	for k, v := range s.values {
		vals[k] = v.Clone()
	}
	return json.Marshal(goSnapshot{Version: 1, Data: append([]byte(nil), d...), Values: vals})
}
func (s *goSession[D]) DecodeSnapshot(b []byte) (err error) {
	defer recoverGoSnapshotCodec(&err)
	var env goSnapshot
	if err := json.Unmarshal(append([]byte(nil), b...), &env); err != nil {
		return err
	}
	if env.Version != 1 {
		return fmt.Errorf("statecharts: unsupported Go snapshot version %d", env.Version)
	}
	if env.Values == nil {
		return fmt.Errorf("statecharts: incomplete Go snapshot")
	}
	d, err := s.codec.Decode(append([]byte(nil), env.Data...))
	if err != nil {
		return err
	}
	if d == nil {
		return fmt.Errorf("statecharts: snapshot codec returned nil")
	}
	if len(env.Values) != len(s.values) {
		return fmt.Errorf("statecharts: snapshot data declarations mismatch")
	}
	fresh := make(map[Identifier]Value, len(env.Values))
	for id, v := range env.Values {
		if _, ok := s.values[id]; !ok {
			return fmt.Errorf("statecharts: snapshot contains unknown data ID %q", id)
		}
		fresh[id] = v.Clone()
	}
	s.data = d
	s.values = fresh
	return nil
}
func (*goSession[D]) Close() error { return nil }

func encodeGoRef(ref FunctionRef) (Value, error) {
	args := make([]Value, len(ref.Args))
	for i, a := range ref.Args {
		v, err := encodeExpression(a)
		if err != nil {
			return Value{}, err
		}
		args[i] = v
	}
	n, _ := StringValue(string(ref.Name))
	v, _ := StringValue(ref.Version)
	return MapValue(map[string]Value{"name": n, "version": v, "args": ListValue(args)})
}
func decodeGoRef(v Value) (FunctionRef, error) {
	m, ok := v.AsMap()
	if !ok {
		return FunctionRef{}, fmt.Errorf("function reference must be a map")
	}
	n, nok := m["name"].AsString()
	ver, vok := m["version"].AsString()
	args, aok := m["args"].AsList()
	if !nok || !vok || !aok || n == "" || ver == "" {
		return FunctionRef{}, fmt.Errorf("invalid function reference")
	}
	r := FunctionRef{Name: Identifier(n), Version: ver, Args: make([]Expression, len(args))}
	var err error
	for i := range args {
		r.Args[i], err = decodeExpression(args[i])
		if err != nil {
			return FunctionRef{}, err
		}
	}
	return r, nil
}
func encodeExpression(e Expression) (Value, error) {
	k, _ := StringValue(string(e.Kind))
	return MapValue(map[string]Value{"kind": k, "data": e.Data})
}
func decodeExpression(v Value) (Expression, error) {
	m, ok := v.AsMap()
	if !ok {
		return Expression{}, fmt.Errorf("expression must be a map")
	}
	k, ok := m["kind"].AsString()
	if !ok || k == "" {
		return Expression{}, fmt.Errorf("expression kind required")
	}
	return Expression{Kind: Identifier(k), Data: m["data"].Clone()}, nil
}
func valueKeyInternal(e Expression) string {
	b, _ := e.Data.MarshalBinary()
	return fmt.Sprintf("%d:%s%s", len(e.Kind), e.Kind, b)
}

func functionKeyInternal(function FunctionRef) string {
	value, _ := encodeGoRef(function)
	data, _ := value.MarshalBinary()
	return string(data)
}
