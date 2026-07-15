package ecmascript

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/dhamidi/statecharts"
	"modernc.org/quickjs"
)

type expressionCategory uint8

const (
	categoryValue expressionCategory = iota
	categoryBoolean
	categoryLocation
	categoryScript
)

type compiledKind uint8

const (
	compiledSource compiledKind = iota
	compiledData
	compiledFunction
)

type programOwner struct{}

type compiledExpression struct {
	owner  *programOwner
	kind   compiledKind
	source string
	dataID statecharts.Identifier
}

type program struct {
	owner        *programOwner
	config       config
	expressions  map[string]*compiledExpression
	functions    map[string]*compiledExpression
	data         map[statecharts.Identifier]*compiledExpression
	declarations []statecharts.Identifier
}

var directDataName = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)

var reservedDataNames = map[string]bool{
	"In": true, "$data": true, "_event": true, "_sessionid": true,
	"_name": true, "_ioprocessors": true, "_x": true,
	"await": true, "break": true, "case": true, "catch": true, "class": true,
	"const": true, "continue": true, "debugger": true, "default": true,
	"delete": true, "do": true, "else": true, "enum": true, "export": true,
	"extends": true, "false": true, "finally": true, "for": true,
	"function": true, "if": true, "import": true, "in": true,
	"instanceof": true, "let": true, "new": true, "null": true,
	"return": true, "static": true, "super": true, "switch": true,
	"this": true, "throw": true, "true": true, "try": true,
	"typeof": true, "var": true, "void": true, "while": true,
	"with": true, "yield": true,
}

func compileDefinition(definition *statecharts.Definition, configuration config) (_ statecharts.DatamodelProgram, resultErr error) {
	if definition == nil {
		return nil, fmt.Errorf("ecmascript: nil definition")
	}
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	if definition.Datamodel != "ecmascript" {
		return nil, fmt.Errorf("ecmascript: definition datamodel %q is not %q", definition.Datamodel, "ecmascript")
	}
	vm, err := quickjs.NewVM()
	if err != nil {
		return nil, fmt.Errorf("ecmascript: create compiler VM: %w", err)
	}
	defer func() {
		if err := vm.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("ecmascript: close compiler VM: %w", err)
		}
	}()

	p := &program{
		owner:       &programOwner{},
		config:      configuration,
		expressions: make(map[string]*compiledExpression),
		functions:   make(map[string]*compiledExpression),
		data:        make(map[statecharts.Identifier]*compiledExpression),
	}
	c := compiler{program: p, vm: vm}
	if err := c.collectData(definition.Data); err != nil {
		return nil, err
	}
	if err := c.collectStateData(&definition.Root); err != nil {
		return nil, err
	}
	if err := c.dataInitializers(definition.Data, "definition.data"); err != nil {
		return nil, err
	}
	if err := c.state(&definition.Root, "definition.root"); err != nil {
		return nil, err
	}
	return p, nil
}

type compiler struct {
	program *program
	vm      *quickjs.VM
}

func (c *compiler) collectData(definitions []statecharts.DataDefinition) error {
	for _, definition := range definitions {
		id := definition.ID
		if reservedDataNames[string(id)] || strings.HasPrefix(string(id), "__sc_") || strings.HasPrefix(string(id), "__statecharts_") {
			return fmt.Errorf("ecmascript: data ID %q is reserved", id)
		}
		if directDataName.MatchString(string(id)) {
			result, err := c.vm.Eval(`Object.prototype.hasOwnProperty.call(globalThis,`+jsString(string(id))+`)`, quickjs.EvalGlobal)
			if err != nil {
				return fmt.Errorf("ecmascript: inspect data ID %q: %w", id, err)
			}
			if conflict, _ := result.(bool); conflict {
				return fmt.Errorf("ecmascript: data ID %q conflicts with an ECMAScript global", id)
			}
		}
		if _, exists := c.program.data[id]; exists {
			continue
		}
		compiled := &compiledExpression{owner: c.program.owner, kind: compiledData, dataID: id}
		c.program.data[id] = compiled
		c.program.declarations = append(c.program.declarations, id)
	}
	return nil
}

func (c *compiler) collectStateData(state *statecharts.StateDefinition) error {
	if err := c.collectData(state.Data); err != nil {
		return err
	}
	for i := range state.Children {
		if err := c.collectStateData(&state.Children[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *compiler) expression(expression statecharts.Expression, category expressionCategory, path string) error {
	source, err := expressionSource(expression)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	wrapper := expressionWrapper(source, category)
	if _, err := c.vm.Compile(wrapper, quickjs.EvalGlobal); err != nil {
		return fmt.Errorf("%s: ecmascript: invalid source: %w", path, err)
	}
	key, err := expressionKey(expression)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if _, exists := c.program.expressions[key]; !exists {
		c.program.expressions[key] = &compiledExpression{owner: c.program.owner, kind: compiledSource, source: source}
	}
	return nil
}

func expressionWrapper(source string, category expressionCategory) string {
	switch category {
	case categoryLocation:
		return `(() => { "use strict"; (` + source + `) = null; })`
	case categoryScript:
		return "(() => { \"use strict\";\n" + source + "\n})"
	default:
		return `(() => { "use strict"; return (` + source + `); })`
	}
}

func (c *compiler) dataInitializers(definitions []statecharts.DataDefinition, path string) error {
	for i := range definitions {
		definition := definitions[i]
		if definition.Source != "" {
			return fmt.Errorf("%s[%d].source: ecmascript: external data sources are unsupported", path, i)
		}
		expression := definition.Expr
		field := "expr"
		if expression == nil {
			expression = definition.Content
			field = "content"
		}
		if expression != nil {
			if err := c.expression(*expression, categoryValue, fmt.Sprintf("%s[%d].%s", path, i, field)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *compiler) state(state *statecharts.StateDefinition, path string) error {
	if err := c.dataInitializers(state.Data, path+".data"); err != nil {
		return err
	}
	if state.Initial != nil {
		if err := c.transition(state.Initial, path+".initial"); err != nil {
			return err
		}
	}
	for i := range state.OnEntry {
		if err := c.blocks([]statecharts.ExecutableBlock{state.OnEntry[i]}, fmt.Sprintf("%s.onEntry[%d]", path, i)); err != nil {
			return err
		}
	}
	for i := range state.OnExit {
		if err := c.blocks([]statecharts.ExecutableBlock{state.OnExit[i]}, fmt.Sprintf("%s.onExit[%d]", path, i)); err != nil {
			return err
		}
	}
	for i := range state.Transitions {
		if err := c.transition(&state.Transitions[i], fmt.Sprintf("%s.transitions[%d]", path, i)); err != nil {
			return err
		}
	}
	for i := range state.Invokes {
		invoke := &state.Invokes[i]
		invokePath := fmt.Sprintf("%s.invokes[%d]", path, i)
		if err := c.optional(invoke.IDLocation, categoryLocation, invokePath+".idLocation"); err != nil {
			return err
		}
		for _, field := range []struct {
			name       string
			expression *statecharts.Expression
		}{
			{"typeExpr", invoke.TypeExpr},
			{"srcExpr", invoke.SrcExpr},
			{"content", invoke.Content},
		} {
			if err := c.optional(field.expression, categoryValue, invokePath+"."+field.name); err != nil {
				return err
			}
		}
		if err := c.params(invoke.Params, invokePath+".params"); err != nil {
			return err
		}
		if err := c.blocks(invoke.Finalize, invokePath+".finalize"); err != nil {
			return err
		}
	}
	if state.DoneData != nil {
		if err := c.params(state.DoneData.Params, path+".doneData.params"); err != nil {
			return err
		}
		if err := c.optional(state.DoneData.Content, categoryValue, path+".doneData.content"); err != nil {
			return err
		}
	}
	for i := range state.Children {
		if err := c.state(&state.Children[i], fmt.Sprintf("%s.children[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func (c *compiler) transition(transition *statecharts.TransitionDefinition, path string) error {
	if err := c.optional(transition.Condition, categoryBoolean, path+".condition"); err != nil {
		return err
	}
	return c.blocks(transition.Actions, path+".actions")
}

func (c *compiler) optional(expression *statecharts.Expression, category expressionCategory, path string) error {
	if expression == nil {
		return nil
	}
	return c.expression(*expression, category, path)
}

func (c *compiler) params(params []statecharts.ParamDefinition, path string) error {
	for i := range params {
		if err := c.optional(params[i].Expr, categoryValue, fmt.Sprintf("%s[%d].expr", path, i)); err != nil {
			return err
		}
		if err := c.optional(params[i].Location, categoryValue, fmt.Sprintf("%s[%d].location", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func (c *compiler) blocks(blocks []statecharts.ExecutableBlock, path string) error {
	for i := range blocks {
		for j := range blocks[i] {
			executable := &blocks[i][j]
			itemPath := fmt.Sprintf("%s[%d][%d]", path, i, j)
			switch executable.Kind {
			case statecharts.ExecutableRaise:
				if err := c.optional(executable.Raise.EventExpr, categoryValue, itemPath+".eventExpr"); err != nil {
					return err
				}
				if err := c.optional(executable.Raise.Data, categoryValue, itemPath+".data"); err != nil {
					return err
				}
			case statecharts.ExecutableSend:
				send := executable.Send
				for _, field := range []struct {
					name       string
					expression *statecharts.Expression
				}{
					{"eventExpr", send.EventExpr},
					{"targetExpr", send.TargetExpr},
					{"typeExpr", send.TypeExpr},
					{"delayExpr", send.DelayExpr},
					{"content", send.Content},
				} {
					if err := c.optional(field.expression, categoryValue, itemPath+"."+field.name); err != nil {
						return err
					}
				}
				if err := c.optional(send.IDLocation, categoryLocation, itemPath+".idLocation"); err != nil {
					return err
				}
				if err := c.params(send.Params, itemPath+".params"); err != nil {
					return err
				}
			case statecharts.ExecutableCancel:
				if err := c.optional(executable.Cancel.SendIDExpr, categoryValue, itemPath+".sendIDExpr"); err != nil {
					return err
				}
			case statecharts.ExecutableLog:
				if err := c.optional(executable.Log.LabelExpr, categoryValue, itemPath+".labelExpr"); err != nil {
					return err
				}
				if err := c.optional(executable.Log.Expr, categoryValue, itemPath+".expr"); err != nil {
					return err
				}
			case statecharts.ExecutableAssign:
				if err := c.expression(executable.Assign.Location, categoryLocation, itemPath+".location"); err != nil {
					return err
				}
				if err := c.expression(executable.Assign.Expr, categoryValue, itemPath+".expr"); err != nil {
					return err
				}
			case statecharts.ExecutableScript:
				if err := c.expression(executable.Script.Expr, categoryScript, itemPath+".expr"); err != nil {
					return err
				}
			case statecharts.ExecutableCall:
				if err := c.function(executable.Call.Function, itemPath+".function"); err != nil {
					return err
				}
			case statecharts.ExecutableForEach:
				if err := c.expression(executable.ForEach.Array, categoryValue, itemPath+".array"); err != nil {
					return err
				}
				if c.program.data[executable.ForEach.Item] == nil {
					return fmt.Errorf("%s.item: ecmascript: unknown data ID %q", itemPath, executable.ForEach.Item)
				}
				if executable.ForEach.Index != "" && c.program.data[executable.ForEach.Index] == nil {
					return fmt.Errorf("%s.index: ecmascript: unknown data ID %q", itemPath, executable.ForEach.Index)
				}
				if err := c.blocks(executable.ForEach.Actions, itemPath+".actions"); err != nil {
					return err
				}
			case statecharts.ExecutableChoose:
				for branchIndex := range executable.Choose.Branches {
					branch := &executable.Choose.Branches[branchIndex]
					branchPath := fmt.Sprintf("%s.branches[%d]", itemPath, branchIndex)
					if err := c.expression(branch.Condition, categoryBoolean, branchPath+".condition"); err != nil {
						return err
					}
					if err := c.blocks(branch.Actions, branchPath+".actions"); err != nil {
						return err
					}
				}
				if err := c.blocks(executable.Choose.Else, itemPath+".else"); err != nil {
					return err
				}
			case statecharts.ExecutableExtension:
				return fmt.Errorf("%s: ecmascript: extension executable content is unsupported", itemPath)
			}
		}
	}
	return nil
}

func (c *compiler) function(function statecharts.FunctionRef, path string) error {
	args := make([]string, len(function.Args))
	for i := range function.Args {
		if err := c.expression(function.Args[i], categoryValue, fmt.Sprintf("%s.args[%d]", path, i)); err != nil {
			return err
		}
		var err error
		args[i], err = expressionSource(function.Args[i])
		if err != nil {
			return err
		}
		args[i] = "(" + args[i] + ")"
	}
	name, _ := json.Marshal(string(function.Name))
	source := `globalThis[` + string(name) + `](` + strings.Join(args, ",") + `)`
	if _, err := c.vm.Compile(expressionWrapper(source, categoryScript), quickjs.EvalGlobal); err != nil {
		return fmt.Errorf("%s: ecmascript: invalid function call: %w", path, err)
	}
	key, err := functionKey(function)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	c.program.functions[key] = &compiledExpression{owner: c.program.owner, kind: compiledFunction, source: source}
	return nil
}

func expressionKey(expression statecharts.Expression) (string, error) {
	data, err := expression.Data.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("ecmascript: encode expression: %w", err)
	}
	return strconv.Itoa(len(expression.Kind)) + ":" + string(expression.Kind) + string(data), nil
}

func functionKey(function statecharts.FunctionRef) (string, error) {
	wire, err := json.Marshal(function)
	if err != nil {
		return "", fmt.Errorf("ecmascript: encode function reference: %w", err)
	}
	return string(wire), nil
}

func (*program) Fingerprint() []byte {
	return []byte("statecharts/ecmascript/v1;value-wire/v1;pending-jobs=drain")
}

func (p *program) ResolveExpression(expression statecharts.Expression) (statecharts.CompiledExpression, error) {
	key, err := expressionKey(expression)
	if err != nil {
		return nil, err
	}
	compiled := p.expressions[key]
	if compiled == nil {
		return nil, fmt.Errorf("ecmascript: expression was not compiled by this program")
	}
	return compiled, nil
}

func (p *program) ResolveFunction(function statecharts.FunctionRef) (statecharts.CompiledExpression, error) {
	key, err := functionKey(function)
	if err != nil {
		return nil, err
	}
	compiled := p.functions[key]
	if compiled == nil {
		return nil, fmt.Errorf("ecmascript: function reference was not compiled by this program")
	}
	return compiled, nil
}

func (p *program) ResolveDataLocation(id statecharts.Identifier) (statecharts.CompiledExpression, error) {
	compiled := p.data[id]
	if compiled == nil {
		return nil, fmt.Errorf("ecmascript: data ID %q was not declared in this program", id)
	}
	return compiled, nil
}

func (p *program) NewSession(statecharts.SessionOptions) (statecharts.DatamodelSession, error) {
	return newSession(p)
}

func sortedDeclarations(declarations []statecharts.Identifier) []statecharts.Identifier {
	result := append([]statecharts.Identifier(nil), declarations...)
	sort.Slice(result, func(i, j int) bool { return result[i].Compare(result[j]) < 0 })
	return result
}
