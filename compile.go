package statecharts

import (
	"fmt"
	"time"
)

// Compile validates and normalizes definition, compiles all model-owned
// expressions, and lowers it to an immutable Chart.
func Compile(definition Definition, model Datamodel) (*Chart, error) {
	if model == nil {
		return nil, fmt.Errorf("statecharts: nil datamodel")
	}
	d, err := normalizeDefinition(definition)
	if err != nil {
		return nil, err
	}
	if d.Datamodel != model.Name() {
		return nil, definitionError("definition.datamodel", "datamodel %q does not match compiler %q", d.Datamodel, model.Name())
	}
	program, err := model.Compile(&d)
	if err != nil {
		return nil, err
	}
	if program == nil {
		return nil, fmt.Errorf("statecharts: datamodel %q returned a nil program", model.Name())
	}
	fingerprint := program.Fingerprint()
	if len(fingerprint) == 0 {
		return nil, fmt.Errorf("statecharts: datamodel %q returned an empty program fingerprint", model.Name())
	}
	canonical, err := d.CanonicalBytes()
	if err != nil {
		return nil, fmt.Errorf("statecharts: canonicalize chart definition: %w", err)
	}
	revision := deriveRevisionFromCanonical(canonical, model.Name(), fingerprint)
	globalData, err := compileData(d.Data, program, "definition.data")
	if err != nil {
		return nil, err
	}
	c := &Chart{
		byID: map[Identifier]*compiledState{}, name: d.Name, datamodel: model, program: program,
		programFingerprint: append([]byte(nil), fingerprint...), canonicalDefinition: append([]byte(nil), canonical...),
		revision: revision, definition: d, data: globalData, dataBinding: d.DataBinding,
		invokesByDefinitionID: map[Identifier]*compiledInvoke{},
	}
	order := 0
	root, err := compileDefinitionState(c, &d.Root, nil, &order, program, "root")
	if err != nil {
		return nil, err
	}
	c.root = root
	return c, nil
}

func compileDefinitionState(c *Chart, s *StateDefinition, parent *compiledState, order *int, p DatamodelProgram, path string) (*compiledState, error) {
	cs := &compiledState{id: s.ID.Value, kind: s.Kind, historyKind: s.History, parent: parent, docOrder: *order}
	*order++
	c.byID[cs.id] = cs
	c.order = append(c.order, cs)
	var err error
	if cs.onEntry, err = compileBlocks(s.OnEntry, p, path+".onEntry"); err != nil {
		return nil, err
	}
	if cs.onExit, err = compileBlocks(s.OnExit, p, path+".onExit"); err != nil {
		return nil, err
	}
	if cs.data, err = compileData(s.Data, p, path+".data"); err != nil {
		return nil, err
	}
	if s.DoneData != nil {
		if cs.modelPayload, err = compilePayload(s.DoneData.Params, s.DoneData.Content, p, path+".doneData"); err != nil {
			return nil, err
		}
	}
	if s.Initial != nil {
		cs.initial, err = compileDefinitionTransition(s.Initial, cs, p, path+".initial")
		if err != nil {
			return nil, err
		}
	}
	for i := range s.Transitions {
		t, e := compileDefinitionTransition(&s.Transitions[i], cs, p, fmt.Sprintf("%s.transitions[%d]", path, i))
		if e != nil {
			return nil, e
		}
		cs.transitions = append(cs.transitions, t)
	}
	for i := range s.Invokes {
		invoke, e := compileDefinitionInvoke(&s.Invokes[i], cs, p, fmt.Sprintf("%s.invokes[%d]", path, i))
		if e != nil {
			return nil, e
		}
		cs.invokes = append(cs.invokes, invoke)
		c.invokesByDefinitionID[invoke.definitionID] = invoke
	}
	for i := range s.Children {
		child, e := compileDefinitionState(c, &s.Children[i], cs, order, p, fmt.Sprintf("%s.children[%d]", path, i))
		if e != nil {
			return nil, e
		}
		cs.children = append(cs.children, child)
	}
	return cs, nil
}

func compileDefinitionInvoke(invoke *InvokeDefinition, owner *compiledState, p DatamodelProgram, path string) (*compiledInvoke, error) {
	compiled := &compiledInvoke{
		definitionID: invoke.DefinitionID,
		owner:        owner,
		id:           invoke.ID,
		staticType:   canonicalInvokeType(Identifier(invoke.Type)),
		staticSource: invoke.Src,
		autoForward:  invoke.AutoForward,
	}
	var err error
	if invoke.IDLocation != nil {
		compiled.modelIDLocation, err = p.ResolveExpression(*invoke.IDLocation)
		if err != nil {
			return nil, definitionError(path+".idLocation", "%v", err)
		}
		compiled.hasModelIDLocation = true
	}
	if invoke.TypeExpr != nil {
		compiled.typeExpr, err = p.ResolveExpression(*invoke.TypeExpr)
		if err != nil {
			return nil, definitionError(path+".typeExpr", "%v", err)
		}
		compiled.hasTypeExpr = true
	}
	if invoke.SrcExpr != nil {
		compiled.sourceExpr, err = p.ResolveExpression(*invoke.SrcExpr)
		if err != nil {
			return nil, definitionError(path+".srcExpr", "%v", err)
		}
		compiled.hasSourceExpr = true
	}
	compiled.payload, err = compilePayload(invoke.Params, invoke.Content, p, path)
	if err != nil {
		return nil, err
	}
	compiled.finalize, err = compileBlocks(invoke.Finalize, p, path+".finalize")
	if err != nil {
		return nil, err
	}
	return compiled, nil
}
func compileDefinitionTransition(t *TransitionDefinition, s *compiledState, p DatamodelProgram, path string) (*compiledTransition, error) {
	x := &compiledTransition{events: append([]Identifier(nil), t.Events...), target: append([]Identifier(nil), t.Targets...), internal: t.Type == TransitionInternal, source: s}
	var err error
	if t.Condition != nil {
		x.modelCondition, err = p.ResolveExpression(*t.Condition)
		if err != nil {
			return nil, definitionError(path+".condition", "%v", err)
		}
		x.hasModelCondition = true
	}
	x.actions, err = compileBlocks(t.Actions, p, path+".actions")
	return x, err
}
func compileData(ds []DataDefinition, p DatamodelProgram, path string) ([]compiledData, error) {
	r := make([]compiledData, len(ds))
	for i, d := range ds {
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		if d.Source != "" {
			return nil, definitionError(itemPath+".source", "data sources are not supported by the compiled datamodel contract")
		}
		var err error
		r[i].location, err = p.ResolveDataLocation(d.ID)
		if err != nil {
			return nil, definitionError(itemPath+".id", "%v", err)
		}
		e := d.Expr
		if e == nil {
			e = d.Content
		}
		if e != nil {
			r[i].initializer, err = p.ResolveExpression(*e)
			if err != nil {
				field := ".expr"
				if d.Expr == nil {
					field = ".content"
				}
				return nil, definitionError(itemPath+field, "%v", err)
			}
			r[i].hasInitializer = true
		}
	}
	return r, nil
}
func compilePayload(ps []ParamDefinition, content *Expression, p DatamodelProgram, path string) (*compiledPayload, error) {
	r := &compiledPayload{}
	var err error
	if content != nil {
		r.content, err = p.ResolveExpression(*content)
		r.hasContent = true
		if err != nil {
			return nil, definitionError(path+".content", "%v", err)
		}
		return r, nil
	}
	for i, v := range ps {
		e := v.Expr
		if e == nil {
			e = v.Location
		}
		h, x := p.ResolveExpression(*e)
		if x != nil {
			field := ".expr"
			if v.Expr == nil {
				field = ".location"
			}
			return nil, definitionError(fmt.Sprintf("%s.params[%d]%s", path, i, field), "%v", x)
		}
		r.params = append(r.params, compiledParam{v.Name, h})
	}
	return r, nil
}
func compileBlocks(bs []ExecutableBlock, p DatamodelProgram, path string) ([]actionBlock, error) {
	r := make([]actionBlock, len(bs))
	for i, b := range bs {
		r[i] = make(actionBlock, len(b))
		for j, e := range b {
			itemPath := fmt.Sprintf("%s[%d][%d]", path, i, j)
			o, err := compileOperation(e, p, itemPath)
			if err != nil {
				return nil, err
			}
			r[i][j] = compiledAction{op: o}
		}
	}
	return r, nil
}
func resolve(p DatamodelProgram, e *Expression) (CompiledExpression, error) {
	if e == nil {
		return nil, nil
	}
	return p.ResolveExpression(*e)
}
func compileOperation(e Executable, p DatamodelProgram, path string) (*compiledOperation, error) {
	o := &compiledOperation{kind: e.Kind}
	add := func(x *Expression, field string) error {
		h, err := resolve(p, x)
		o.expressions = append(o.expressions, h)
		if err != nil {
			return definitionError(path+field, "%v", err)
		}
		return nil
	}
	switch e.Kind {
	case ExecutableScript:
		h, err := p.ResolveExpression(e.Script.Expr)
		o.expressions = []CompiledExpression{h}
		if err != nil {
			return nil, definitionError(path+".script.expr", "%v", err)
		}
		return o, nil
	case ExecutableCall:
		h, err := p.ResolveFunction(e.Call.Function)
		o.expressions = []CompiledExpression{h}
		if err != nil {
			return nil, definitionError(path+".call.function", "%v", err)
		}
		return o, nil
	case ExecutableRaise:
		o.static = []string{string(e.Raise.Event)}
		if err := add(e.Raise.EventExpr, ".raise.eventExpr"); err != nil {
			return nil, err
		}
		if err := add(e.Raise.Data, ".raise.data"); err != nil {
			return nil, err
		}
	case ExecutableCancel:
		o.static = []string{string(e.Cancel.SendID)}
		if err := add(e.Cancel.SendIDExpr, ".cancel.sendIDExpr"); err != nil {
			return nil, err
		}
	case ExecutableLog:
		o.static = []string{e.Log.Label}
		if err := add(e.Log.LabelExpr, ".log.labelExpr"); err != nil {
			return nil, err
		}
		if err := add(e.Log.Expr, ".log.expr"); err != nil {
			return nil, err
		}
	case ExecutableAssign:
		a, err := p.ResolveExpression(e.Assign.Location)
		if err != nil {
			return nil, definitionError(path+".assign.location", "%v", err)
		}
		b, err := p.ResolveExpression(e.Assign.Expr)
		if err != nil {
			return nil, definitionError(path+".assign.expr", "%v", err)
		}
		o.expressions = []CompiledExpression{a, b}
	case ExecutableChoose:
		for i, b := range e.Choose.Branches {
			branchPath := fmt.Sprintf("%s.choose.branches[%d]", path, i)
			h, err := p.ResolveExpression(b.Condition)
			if err != nil {
				return nil, definitionError(branchPath+".condition", "%v", err)
			}
			o.expressions = append(o.expressions, h)
			x, err := compileBlocks(b.Actions, p, branchPath+".actions")
			if err != nil {
				return nil, err
			}
			o.blocks = append(o.blocks, x)
		}
		x, err := compileBlocks(e.Choose.Else, p, path+".choose.else")
		if err != nil {
			return nil, err
		}
		o.blocks = append(o.blocks, x)
	case ExecutableForEach:
		h, err := p.ResolveExpression(e.ForEach.Array)
		if err != nil {
			return nil, definitionError(path+".foreach.array", "%v", err)
		}
		o.expressions = []CompiledExpression{h}
		o.bindings.Item, err = p.ResolveDataLocation(e.ForEach.Item)
		if err != nil {
			return nil, definitionError(path+".foreach.item", "%v", err)
		}
		if e.ForEach.Index != "" {
			o.bindings.Index, err = p.ResolveDataLocation(e.ForEach.Index)
			if err != nil {
				return nil, definitionError(path+".foreach.index", "%v", err)
			}
		}
		x, err := compileBlocks(e.ForEach.Actions, p, path+".foreach.actions")
		if err != nil {
			return nil, err
		}
		o.blocks = [][]actionBlock{x}
	case ExecutableSend:
		s := e.Send
		o.static = []string{string(s.Event), s.Target, s.Type, string(s.ID)}
		for i, expression := range []*Expression{s.EventExpr, s.TargetExpr, s.TypeExpr, s.IDLocation, s.DelayExpr} {
			fields := []string{".send.eventExpr", ".send.targetExpr", ".send.typeExpr", ".send.idLocation", ".send.delayExpr"}
			if err := add(expression, fields[i]); err != nil {
				return nil, err
			}
		}
		if s.Delay != "" {
			d, err := time.ParseDuration(s.Delay)
			if err != nil {
				return nil, definitionError(path+".send.delay", "invalid delay %q: %v", s.Delay, err)
			}
			o.delay = d
		}
		var err error
		o.payload, err = compilePayload(s.Params, s.Content, p, path+".send")
		if err != nil {
			return nil, err
		}
	case ExecutableExtension:
		return nil, definitionError(path+".extension", "unhandled extension %q:%q", e.Extension.Namespace, e.Extension.Name)
	default:
		return nil, definitionError(path+".kind", "unsupported executable kind %q", e.Kind)
	}
	return o, nil
}
